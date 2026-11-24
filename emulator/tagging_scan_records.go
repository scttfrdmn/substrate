package emulator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// GetResources coverage for EC2 security groups, launch templates and volumes, and IAM instance
// profiles (#1523).
//
// Each is reported under the ARN the Service Authorization Reference publishes for its type —
// arn:${Partition}:ec2:${Region}:${Account}:security-group/${SecurityGroupId},
// …:launch-template/${LaunchTemplateId}, …:volume/${VolumeId} and
// arn:${Partition}:iam::${Account}:instance-profile/${InstanceProfileNameWithPath} — and
// TagResources and UntagResources accept the same ARNs ([TaggingPlugin.resolveARN]). A record that
// cannot be read or decoded is reported as the scan's error while every readable record of the type
// is still listed ([taggingScanRecords]); [TaggingPlugin.scanAllResources] logs the failure. The
// three EC2 scans are EC2's own, beside its other tagged types ([ec2ListTaggedResources]).

// taggingScanRecords lists ns under prefix and decodes each record into a T for visit. A record
// that cannot be read or decoded is skipped and its failure joined into the returned error, so the
// caller reports every readable record *and* the failure (#1523): one unreadable record must not
// hide the rest of its type, and must not vanish silently either. A List failure reads nothing and
// is returned alone.
func taggingScanRecords[T any](ctx context.Context, state StateManager, ns, prefix string, visit func(T)) error {
	keys, err := state.List(ctx, ns, prefix)
	if err != nil {
		return fmt.Errorf("list %s: %w", prefix, err)
	}
	var errs []error
	for _, k := range keys {
		raw, getErr := state.Get(ctx, ns, k)
		if getErr != nil {
			errs = append(errs, fmt.Errorf("get %s: %w", k, getErr))
			continue
		}
		if raw == nil {
			continue
		}
		var rec T
		if err := json.Unmarshal(raw, &rec); err != nil {
			errs = append(errs, fmt.Errorf("unmarshal %s: %w", k, err))
			continue
		}
		visit(rec)
	}
	return errors.Join(errs...)
}

// scanIAMInstanceProfiles returns the caller's instance profiles, under the ARN each record holds
// — the one CreateInstanceProfile answered, path included.
func (p *TaggingPlugin) scanIAMInstanceProfiles(ctx context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	var out []resourceTagMapping
	err := taggingScanRecords(ctx, p.state, iamNamespace, iamInstanceProfilePrefix(reqCtx.AccountID), func(ip IAMInstanceProfile) {
		out = append(out, resourceTagMapping{
			ResourceARN: ip.ARN,
			Tags:        iamTagsToTaggingTags(ip.Tags),
			everTagged:  ip.EverTagged,
		})
	})
	if err != nil {
		return out, fmt.Errorf("scan iam instance profiles: %w", err)
	}
	return out, nil
}
