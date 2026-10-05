package emulator

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"time"
)

// A secret's versions and their staging labels (#1376).
//
// Until #1376 a secret held one pointer, CurrentVersionID, and ListSecretVersionIds answered exactly
// that version as AWSCURRENT, whatever the secret's history. AWS's model is a set of versions, each
// carrying zero or more staging labels, and three pages state how the labels move:
//
//   - CreateSecret: "If you include SecretString or SecretBinary then Secrets Manager creates an
//     initial secret version and automatically attaches the staging label AWSCURRENT to it."
//   - PutSecretValue: "If you don't include VersionStages, then Secrets Manager automatically moves the
//     staging label AWSCURRENT to this version … If this operation moves the staging label AWSCURRENT
//     from another version to this version, then Secrets Manager also automatically moves the staging
//     label AWSPREVIOUS to the version that AWSCURRENT was removed from." And of VersionStages: "If you
//     specify a staging label that's already associated with a different version of the same secret,
//     then Secrets Manager removes the label from the other version and attaches it to this version."
//   - UpdateSecret: "If you include SecretString or SecretBinary to create a new secret version,
//     Secrets Manager automatically moves the staging label AWSCURRENT to the new version. Then it
//     attaches the label AWSPREVIOUS to the version that AWSCURRENT was removed from."
//
// A version left with no label is deprecated — ListSecretVersionIds: "Versions without staging labels
// are considered deprecated and are subject to deletion by Secrets Manager. By default, versions without
// staging labels aren't included." So a third value makes the first version deprecated: it loses
// AWSPREVIOUS to the second. Substrate keeps a deprecated version (its payload stays readable by ID) and
// never removes one; AWS's removal of versions beyond 100 is unmodelled, as is its 24-hour retention.
//
// The list lives on the secret record ([SecretState.Versions]), so one write moves every label at once.
// The payload stays where it always was, at [smSecretVersionStateKey], and a version records whether it
// holds a SecretString or a SecretBinary: GetSecretValue answers whichever the version was given as, and
// a resubmitted token is compared by kind as well as by bytes, so a SecretString and a SecretBinary with
// identical text are different values, as the pages' "the version SecretString and SecretBinary values
// are the same" requires.

const (
	// smStageCurrent is the staging label of a secret's current version.
	smStageCurrent = "AWSCURRENT"

	// smStagePrevious is the label AWSCURRENT's last holder receives when the label moves on.
	smStagePrevious = "AWSPREVIOUS"
)

// SMSecretVersion is one version of a secret: its identifier, the staging labels attached to it, when
// it was created, and whether its payload was given as SecretBinary.
type SMSecretVersion struct {
	// VersionID is the version's identifier, the request's ClientRequestToken or a minted one.
	VersionID string `json:"VersionId"`

	// Stages are the staging labels attached to the version. An empty list marks it deprecated.
	Stages []string `json:"VersionStages,omitempty"`

	// CreatedDate is when the version was created, on the simulated clock.
	CreatedDate time.Time `json:"CreatedDate"`

	// Binary records that the payload was given as SecretBinary rather than SecretString.
	Binary bool `json:"Binary,omitempty"`
}

// smVersions returns a secret's versions. A secret written before #1376 has no list; its one version is
// the CurrentVersionID whose payload exists, labeled AWSCURRENT and dated by LastChangedDate — the best
// reading of what it held, since earlier versions were never recorded. A secret created with no value
// has no payload and so no version, as CreateSecret's page states.
func (p *SecretsManagerPlugin) smVersions(ctx context.Context, secret *SecretState) ([]SMSecretVersion, error) {
	if len(secret.Versions) > 0 {
		return slices.Clone(secret.Versions), nil
	}
	if secret.CurrentVersionID == "" {
		return nil, nil
	}
	data, err := p.state.Get(ctx, secretsManagerNamespace, smSecretVersionStateKey(secret.AccountID, secret.Region, secret.Name, secret.CurrentVersionID))
	if err != nil {
		return nil, fmt.Errorf("sm load legacy version %s: %w", secret.CurrentVersionID, err)
	}
	if data == nil {
		return nil, nil
	}
	return []SMSecretVersion{{VersionID: secret.CurrentVersionID, Stages: []string{smStageCurrent}, CreatedDate: secret.LastChangedDate}}, nil
}

// smFindVersion returns the version named id, or nil.
func smFindVersion(versions []SMSecretVersion, id string) *SMSecretVersion {
	for i := range versions {
		if versions[i].VersionID == id {
			return &versions[i]
		}
	}
	return nil
}

// smVersionWithStage returns the version carrying stage, or nil.
func smVersionWithStage(versions []SMSecretVersion, stage string) *SMSecretVersion {
	for i := range versions {
		if slices.Contains(versions[i].Stages, stage) {
			return &versions[i]
		}
	}
	return nil
}

// smAttachVersion adds v to the secret's versions with the labels v carries, moving each label off the
// version that held it, and AWSPREVIOUS onto AWSCURRENT's last holder when AWSCURRENT moves, as the
// three pages above state. It writes the result to secret.Versions and points CurrentVersionID at the
// version carrying AWSCURRENT.
func smAttachVersion(secret *SecretState, versions []SMSecretVersion, v SMSecretVersion) {
	var previousHolder string
	for _, stage := range v.Stages {
		for i := range versions {
			if !slices.Contains(versions[i].Stages, stage) {
				continue
			}
			if stage == smStageCurrent {
				previousHolder = versions[i].VersionID
			}
			versions[i].Stages = slices.DeleteFunc(versions[i].Stages, func(s string) bool { return s == stage })
		}
	}
	if previousHolder != "" && !slices.Contains(v.Stages, smStagePrevious) {
		for i := range versions {
			versions[i].Stages = slices.DeleteFunc(versions[i].Stages, func(s string) bool { return s == smStagePrevious })
		}
		if prev := smFindVersion(versions, previousHolder); prev != nil {
			prev.Stages = append(prev.Stages, smStagePrevious)
		}
	}
	for i := range versions {
		if len(versions[i].Stages) == 0 {
			versions[i].Stages = nil
		}
	}
	versions = append(versions, v)
	secret.Versions = versions
	if cur := smVersionWithStage(versions, smStageCurrent); cur != nil {
		secret.CurrentVersionID = cur.VersionID
	}
}

// smRequestValue resolves a write's secret value from its SecretString and SecretBinary members, which
// every write page constrains the same way: "Either SecretString or SecretBinary must have a value, but
// not both." Both is InvalidParameterException ("The parameter name or value is invalid", 400). Whether
// neither is allowed is the operation's decision, so an empty value is returned rather than refused.
func smRequestValue(str, bin string) (value string, binary bool, err *AWSError) {
	if str != "" && bin != "" {
		return "", false, &AWSError{
			Code:       "InvalidParameterException",
			Message:    "Either SecretString or SecretBinary must have a value, but not both",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if bin != "" {
		return bin, true, nil
	}
	return str, false, nil
}

// smMissingValue refuses a write that must carry a value and carries neither SecretString nor
// SecretBinary. PutSecretValue's page states the rule: "You must include SecretBinary or SecretString,
// but not both".
func smMissingValue() *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    "You must include SecretBinary or SecretString",
		HTTPStatus: http.StatusBadRequest,
	}
}

// smValidateVersionStages checks PutSecretValue's VersionStages against its published constraints:
// "Array Members: Minimum number of 1 item. Maximum number of 20 items. Length Constraints: Minimum
// length of 1. Maximum length of 256." A nil list is absent and valid; an empty one is a list with no
// items, below the minimum.
func smValidateVersionStages(stages []string) *AWSError {
	if stages == nil {
		return nil
	}
	invalid := func(msg string) *AWSError {
		return &AWSError{Code: "InvalidParameterException", Message: msg, HTTPStatus: http.StatusBadRequest}
	}
	if len(stages) < 1 || len(stages) > 20 {
		return invalid(fmt.Sprintf("VersionStages must hold 1 to 20 labels, not %d", len(stages)))
	}
	for _, s := range stages {
		if len(s) < 1 || len(s) > 256 {
			return invalid(fmt.Sprintf("a staging label must be 1 to 256 characters, not %d", len(s)))
		}
	}
	return nil
}
