package emulator

// Secrets Manager ListSecrets: the SecretListEntry it renders, the Filters it applies, the order it
// lists in, and the secrets it leaves out (#1622).
//
// # What an entry carries
//
// Until #1622 every entry was `{ARN, Name}`. API_SecretListEntry publishes twenty members, and a
// consumer that reads ownership tags through ListSecrets — so that it need not call DescribeSecret,
// whose tag-conditioned IAM policy cannot be evaluated for a secret that does not exist yet — found
// none of them. An entry is now [smDescribeSecretBody]'s metadata with the two differences the list
// shape publishes:
//
//   - SecretVersionsToStages, the list shape's name for the version-to-labels map, carrying "the
//     currently assigned SecretVersionStage staging labels and the SecretVersionId attached to each
//     one". "A version that does not have any SecretVersionStage is considered deprecated … Such
//     versions are not included in this list." A secret with no version (created with no value) has
//     no map.
//   - RotationEnabled is emitted only when true. DescribeSecret's page documents a null for a secret
//     never configured for rotation, which is why [smDescribeSecretBody] emits one; SecretListEntry
//     carries no such statement, and the ListSecrets page's own sample entries carry no
//     RotationEnabled member at all, so the list shape omits it.
//
// # Filters
//
// API_Filter publishes seven keys and the match rule of each: name, tag-key, tag-value,
// primary-region and owning-service are "Prefix match, case-sensitive", description is "Prefix match,
// not case-sensitive", and all "Breaks the filter value string into words and then searches all
// attributes for matches. Not case-sensitive." A value "prefix[ed] … with an exclamation mark (!)"
// is a negation filter. How those pieces combine is substrate's reading, because no page states it:
//
//   - The values of one filter are alternatives (any may match) and separate filters are all
//     required, the convention every other AWS list filter follows.
//   - A negated value matches a secret none of whose attribute strings matches the value — "!prod"
//     on tag-key leaves out a secret carrying any key that begins "prod", rather than admitting one
//     that also carries some other key.
//   - description is matched word by word: every word of the value must begin some word of the
//     description. That is a prefix match whenever the value is one word, and it is also what the
//     user guide's "Find secrets" page shows, where "My Description" matches "Description of my
//     secret".
//   - all splits both the value and each attribute into words at the boundaries that page names —
//     "any change from uppercase to lowercase, from letter to number, or from number/letter to
//     punctuation", whose own example ("credsDatabase#892" is creds, Database and 892) places the
//     case boundary before an uppercase letter that follows a lowercase one — and admits a secret having any attribute word that begins with any value word,
//     the page's "searching for My_Secret matches secrets with the word my or secret".
//
// Substrate models neither replication nor service-managed secrets, so no secret has a PrimaryRegion
// or an OwningService: a positive primary-region or owning-service filter matches nothing, and a
// negated one matches every secret.
//
// # Order
//
// "If not specified, secrets are listed by CreatedDate", and SortBy names the other three keys.
// SortOrder's page publishes asc and desc and no default; ascending is substrate's reading. Ties —
// two secrets created at one simulated instant, which a pinned clock makes common — are broken by
// name, so the order is total. The list is filtered and sorted before it is cut, so a page is never
// under-filled by a filter applied after the cut.
//
// # NextToken is a cursor, not an offset
//
// The token names the last entry of the page it ended — its sort key and its name — and the next
// page is the entries that sort strictly after that pair. An array offset would skip entries when
// the listing changes between pages: a cleanup loop that deletes each page's secrets as it goes
// moves every later secret up, and the next offset lands past the ones that moved. A cursor is a
// value in the sort order rather than a count of what preceded it, so a deletion or a creation
// elsewhere in the listing cannot cost an entry its place — the rule #887 set for the RDS and
// ElastiCache describes (see query_marker_pagination.go). A token is refused unless it is exactly
// what [smEncodeListCursor] emits, so an unissued one is InvalidNextTokenException and never page
// one (#1086).
//
// # Secrets pending deletion
//
// "By default, secrets scheduled for deletion aren't included", and IncludePlannedDeletion
// "specifies whether to include" them. Substrate listed them unconditionally until #1622. An
// included one carries DeletedDate, as DescribeSecret's does.
//
// # Refusals
//
// Every refusal here is InvalidParameterException/400, the page's code for "The parameter name or
// value is invalid": more than ten filters, an unknown key, a value list outside one to ten items, a
// value over 512 characters or outside the published pattern, and a SortBy or SortOrder outside its
// enumeration. The messages are substrate's own; the page publishes codes, not messages.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"unicode"
)

// Filter keys API_Filter publishes.
const (
	smFilterDescription   = "description"
	smFilterName          = "name"
	smFilterTagKey        = "tag-key"
	smFilterTagValue      = "tag-value"
	smFilterPrimaryRegion = "primary-region"
	smFilterOwningService = "owning-service"
	smFilterAll           = "all"
)

// smFilterKeys is the Key enumeration API_Filter publishes.
var smFilterKeys = []string{
	smFilterDescription, smFilterName, smFilterTagKey, smFilterTagValue,
	smFilterPrimaryRegion, smFilterOwningService, smFilterAll,
}

// smFilterValuePattern is Filter.Values' published pattern, `^\!?[a-zA-Z0-9 :_@\/\+\=\.\-\!]*$`.
var smFilterValuePattern = regexp.MustCompile(`^!?[a-zA-Z0-9 :_@/+=.\-!]*$`)

// Constraints API_ListSecrets and API_Filter publish.
const (
	smMaxFilters         = 10
	smMaxFilterValues    = 10
	smMaxFilterValueSize = 512
)

// SortBy values API_ListSecrets publishes.
const (
	smSortCreatedDate      = "created-date"
	smSortLastAccessedDate = "last-accessed-date"
	smSortLastChangedDate  = "last-changed-date"
	smSortName             = "name"
)

// smListFilter is one member of ListSecrets' Filters.
type smListFilter struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

// smInvalidListParameter reports a ListSecrets parameter outside its published constraint.
func smInvalidListParameter(format string, args ...any) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    fmt.Sprintf(format, args...),
		HTTPStatus: http.StatusBadRequest,
	}
}

// smValidateListFilters checks Filters against the constraints API_ListSecrets and API_Filter
// publish.
func smValidateListFilters(filters []smListFilter) *AWSError {
	if len(filters) > smMaxFilters {
		return smInvalidListParameter("Filters may hold at most %d filters, not %d", smMaxFilters, len(filters))
	}
	for i, f := range filters {
		if !slices.Contains(smFilterKeys, f.Key) {
			return smInvalidListParameter("Filters[%d].Key %q is not one of %s", i, f.Key, strings.Join(smFilterKeys, ", "))
		}
		if len(f.Values) < 1 || len(f.Values) > smMaxFilterValues {
			return smInvalidListParameter("Filters[%d].Values must hold from 1 to %d values, not %d", i, smMaxFilterValues, len(f.Values))
		}
		for _, v := range f.Values {
			if len(v) > smMaxFilterValueSize {
				return smInvalidListParameter("Filters[%d].Values has a value longer than %d characters", i, smMaxFilterValueSize)
			}
			if !smFilterValuePattern.MatchString(v) {
				return smInvalidListParameter("Filters[%d].Values has the value %q, which is outside the published pattern", i, v)
			}
		}
	}
	return nil
}

// smFilterMatches reports whether a secret passes every filter.
func smFilterMatches(secret *SecretState, filters []smListFilter) bool {
	for _, f := range filters {
		if !smOneFilterMatches(secret, f) {
			return false
		}
	}
	return true
}

// smOneFilterMatches reports whether any of one filter's values admits the secret.
func smOneFilterMatches(secret *SecretState, f smListFilter) bool {
	for _, raw := range f.Values {
		value, negated := strings.CutPrefix(raw, "!")
		hit := smFilterValueHits(secret, f.Key, value)
		if hit != negated {
			return true
		}
	}
	return false
}

// smFilterValueHits reports whether one (un-negated) filter value matches any of the secret's
// attribute strings for key.
func smFilterValueHits(secret *SecretState, key, value string) bool {
	switch key {
	case smFilterName:
		return strings.HasPrefix(secret.Name, value)
	case smFilterDescription:
		return smDescriptionMatches(secret.Description, value)
	case smFilterTagKey:
		return slices.ContainsFunc(secret.Tags, func(t SMTag) bool { return strings.HasPrefix(t.Key, value) })
	case smFilterTagValue:
		return slices.ContainsFunc(secret.Tags, func(t SMTag) bool { return strings.HasPrefix(t.Value, value) })
	case smFilterAll:
		return smAllMatches(secret, value)
	default:
		// primary-region and owning-service: no secret here carries either member; see the preamble.
		return false
	}
}

// smDescriptionMatches reports whether every word of value begins some word of description, without
// regard to case.
func smDescriptionMatches(description, value string) bool {
	words := strings.Fields(strings.ToLower(description))
	for _, want := range strings.Fields(strings.ToLower(value)) {
		if !slices.ContainsFunc(words, func(w string) bool { return strings.HasPrefix(w, want) }) {
			return false
		}
	}
	return true
}

// smAllMatches reports whether any word of value begins any word of the secret's name, description,
// tag keys or tag values, without regard to case.
func smAllMatches(secret *SecretState, value string) bool {
	attrs := []string{secret.Name, secret.Description}
	for _, t := range secret.Tags {
		attrs = append(attrs, t.Key, t.Value)
	}
	var words []string
	for _, a := range attrs {
		words = append(words, smSearchWords(a)...)
	}
	for _, want := range smSearchWords(value) {
		if slices.ContainsFunc(words, func(w string) bool { return strings.HasPrefix(w, want) }) {
			return true
		}
	}
	return false
}

// smSearchWords splits s into lowercase words at the boundaries the "Find secrets" page names: a
// change from lowercase to uppercase, between a letter and a digit, and at anything that is neither.
// "credsDatabase#892" is "creds", "database" and "892", the page's own example.
func smSearchWords(s string) []string {
	var words []string
	var cur []rune
	flush := func() {
		if len(cur) > 0 {
			words = append(words, strings.ToLower(string(cur)))
			cur = cur[:0]
		}
	}
	var prev rune
	for _, r := range s {
		switch {
		case !unicode.IsLetter(r) && !unicode.IsDigit(r):
			flush()
		case len(cur) > 0 && (unicode.IsLower(prev) && unicode.IsUpper(r) ||
			unicode.IsLetter(prev) != unicode.IsLetter(r)):
			flush()
			cur = append(cur, r)
		default:
			cur = append(cur, r)
		}
		prev = r
	}
	flush()
	return words
}

// smListCursor is a secret's position in a ListSecrets order: the sort key, as microseconds of the
// date SortBy names, and the name that breaks ties. It is also the NextToken's content.
type smListCursor struct {
	T int64  `json:"t,omitempty"`
	N string `json:"n"`
}

// smCursorOf returns a secret's position under sortBy.
func smCursorOf(s *SecretState, sortBy string) smListCursor {
	switch sortBy {
	case smSortName, smSortLastAccessedDate:
		// LastAccessedDate is not modeled (see [smDescribeSecretBody]), so every secret ties on it
		// and the name decides, as it does for SortBy name.
		return smListCursor{N: s.Name}
	case smSortLastChangedDate:
		return smListCursor{T: s.LastChangedDate.UnixMicro(), N: s.Name}
	default:
		return smListCursor{T: s.CreatedDate.UnixMicro(), N: s.Name}
	}
}

// smCompareCursors orders two positions ascending: by sort key, then by name.
func smCompareCursors(a, b smListCursor) int {
	if a.T != b.T {
		if a.T < b.T {
			return -1
		}
		return 1
	}
	return strings.Compare(a.N, b.N)
}

// smEncodeListCursor renders a position as a NextToken.
func smEncodeListCursor(c smListCursor) (string, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("sm encode list cursor: %w", err)
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

// smDecodeListCursor reads a NextToken, reporting whether it is one [smEncodeListCursor] could have
// emitted. An empty token is the start of the listing. The rule is the round trip: a token that
// decodes but does not re-encode to itself is not one substrate issued.
func smDecodeListCursor(raw string) (cursor smListCursor, set, ok bool) {
	if raw == "" {
		return smListCursor{}, false, true
	}
	data, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return smListCursor{}, false, false
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cursor); err != nil || cursor.N == "" {
		return smListCursor{}, false, false
	}
	if again, encErr := smEncodeListCursor(cursor); encErr != nil || again != raw {
		return smListCursor{}, false, false
	}
	return cursor, true, true
}

// smSortSecrets orders secrets by sortBy, ascending unless desc, breaking ties by name.
func smSortSecrets(secrets []*SecretState, sortBy string, desc bool) {
	slices.SortStableFunc(secrets, func(a, b *SecretState) int {
		c := smCompareCursors(smCursorOf(a, sortBy), smCursorOf(b, sortBy))
		if desc {
			return -c
		}
		return c
	})
}

// smPageAfter returns up to pageSize of the sorted secrets that come strictly after cursor in the
// walk's direction, and the NextToken naming the page's last entry when more follow.
func smPageAfter(secrets []*SecretState, cursor smListCursor, set bool, sortBy string, desc bool, pageSize int) ([]*SecretState, string, error) {
	start := 0
	if set {
		start = len(secrets)
		for i, s := range secrets {
			c := smCompareCursors(smCursorOf(s, sortBy), cursor)
			if (!desc && c > 0) || (desc && c < 0) {
				start = i
				break
			}
		}
	}
	rest := secrets[start:]
	if len(rest) <= pageSize {
		return rest, "", nil
	}
	page := rest[:pageSize]
	token, err := smEncodeListCursor(smCursorOf(page[len(page)-1], sortBy))
	if err != nil {
		return nil, "", err
	}
	return page, token, nil
}

// smSecretListEntry renders a secret as a SecretListEntry; see this file's preamble for how it
// differs from DescribeSecret's response.
func smSecretListEntry(secret *SecretState, versions []SMSecretVersion) map[string]any {
	out := smDescribeSecretBody(secret)
	if !secret.RotationEnabled {
		delete(out, "RotationEnabled")
	}
	stages := make(map[string][]string, len(versions))
	for _, v := range versions {
		if len(v.Stages) > 0 {
			stages[v.VersionID] = v.Stages
		}
	}
	if len(stages) > 0 {
		out["SecretVersionsToStages"] = stages
	}
	return out
}

func (p *SecretsManagerPlugin) listSecrets(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Filters                []smListFilter `json:"Filters"`
		IncludePlannedDeletion bool           `json:"IncludePlannedDeletion"`
		MaxResults             *int           `json:"MaxResults"`
		NextToken              string         `json:"NextToken"`
		SortBy                 string         `json:"SortBy"`
		SortOrder              string         `json:"SortOrder"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, smInvalidBody()
		}
	}
	maxResults, maxOK := smListMaxResults.resolve(input.MaxResults)
	if !maxOK {
		return nil, smInvalidMaxResults(*input.MaxResults)
	}
	if filterErr := smValidateListFilters(input.Filters); filterErr != nil {
		return nil, filterErr
	}
	switch input.SortBy {
	case "", smSortCreatedDate, smSortLastAccessedDate, smSortLastChangedDate, smSortName:
	default:
		return nil, smInvalidListParameter("SortBy %q is not one of created-date, last-accessed-date, last-changed-date, name", input.SortBy)
	}
	switch input.SortOrder {
	case "", "asc", "desc":
	default:
		return nil, smInvalidListParameter("SortOrder %q is not one of asc, desc", input.SortOrder)
	}

	// A token substrate did not issue is refused rather than answered with page one, which
	// API_ListSecrets publishes InvalidNextTokenException for (#1086), and it is refused before the
	// secret names are read — #887's ordering criterion, since nothing about the refusal depends on
	// what the store holds.
	cursor, cursorSet, ok := smDecodeListCursor(input.NextToken)
	if !ok {
		return nil, smInvalidNextToken()
	}

	goCtx := context.Background()
	names, err := p.loadSecretNames(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	secrets := make([]*SecretState, 0, len(names))
	for _, name := range names {
		s, loadErr := p.loadSecret(goCtx, ctx.AccountID, ctx.Region, name)
		if loadErr != nil {
			return nil, fmt.Errorf("sm listSecrets: %w", loadErr)
		}
		if s == nil {
			continue
		}
		if !s.DeletionDate.IsZero() && !input.IncludePlannedDeletion {
			continue
		}
		if !smFilterMatches(s, input.Filters) {
			continue
		}
		secrets = append(secrets, s)
	}
	desc := input.SortOrder == "desc"
	smSortSecrets(secrets, input.SortBy, desc)
	page, nextToken, err := smPageAfter(secrets, cursor, cursorSet, input.SortBy, desc, maxResults)
	if err != nil {
		return nil, err
	}

	entries := make([]map[string]any, 0, len(page))
	for _, s := range page {
		versions, verErr := p.smVersions(goCtx, s)
		if verErr != nil {
			return nil, verErr
		}
		entries = append(entries, smSecretListEntry(s, versions))
	}

	out := map[string]any{"SecretList": entries}
	if nextToken != "" {
		out["NextToken"] = nextToken
	}
	return smJSONResponse(http.StatusOK, out)
}
