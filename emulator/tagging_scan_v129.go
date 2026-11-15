package emulator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
// is still listed ([taggingScanRecords]); [TaggingPlugin.scanAllResources] logs the failure.

// taggingEC2ScannedType is one EC2 type the tagging API scans by state-key prefix.
type taggingEC2ScannedType struct {
	// arnType is the type segment of the resource's ARN.
	arnType string

	// keyPrefix is the type segment of the resource's state key, colon included.
	keyPrefix string
}

// taggingEC2ScannedTypes are the EC2 types #1523 added, whose records [mergeResourceTags] merges on
// the raw record.
var taggingEC2ScannedTypes = []taggingEC2ScannedType{
	{arnType: "security-group", keyPrefix: "sg:"},
	{arnType: "launch-template", keyPrefix: "lt:"},
	{arnType: "volume", keyPrefix: "volume:"},
}

// taggingEC2RawMergedKey reports whether key belongs to one of [taggingEC2ScannedTypes].
func taggingEC2RawMergedKey(key string) bool {
	for _, t := range taggingEC2ScannedTypes {
		if strings.HasPrefix(key, t.keyPrefix) {
			return true
		}
	}
	return false
}

// mergeRecordEC2Tags merges tags into the EC2 record at key through [mergeRecordTagListTags], which
// stamps the previously-tagged flag from the pre-merge count and keeps every other member of the
// record as stored.
func mergeRecordEC2Tags(
	goCtx context.Context, state StateManager, key string, raw []byte,
	addTags map[string]string, removeKeys []string,
) error {
	updated, err := mergeRecordTagListTags(raw, "tags", "key", "value", addTags, removeKeys)
	if err != nil {
		return fmt.Errorf("tagging merge %s: %w", key, err)
	}
	if err := state.Put(goCtx, ec2Namespace, key, updated); err != nil {
		return fmt.Errorf("tagging put %s: %w", key, err)
	}
	return nil
}

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

// taggingEC2ARN builds an EC2 resource ARN.
func taggingEC2ARN(region, accountID, arnType, id string) string {
	return "arn:aws:ec2:" + region + ":" + accountID + ":" + arnType + "/" + id
}

// scanEC2SecurityGroups returns the caller's security groups.
func (p *TaggingPlugin) scanEC2SecurityGroups(ctx context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	var out []resourceTagMapping
	err := taggingScanRecords(ctx, p.state, ec2Namespace, taggingScanPrefix("sg:", reqCtx), func(sg EC2SecurityGroup) {
		out = append(out, resourceTagMapping{
			ResourceARN: taggingEC2ARN(sg.Region, sg.AccountID, "security-group", sg.GroupID),
			Tags:        ec2TagsToTaggingTags(sg.Tags),
			everTagged:  sg.EverTagged,
		})
	})
	if err != nil {
		return out, fmt.Errorf("scan ec2 security groups: %w", err)
	}
	return out, nil
}

// scanEC2LaunchTemplates returns the caller's launch templates.
func (p *TaggingPlugin) scanEC2LaunchTemplates(ctx context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	var out []resourceTagMapping
	err := taggingScanRecords(ctx, p.state, ec2Namespace, taggingScanPrefix("lt:", reqCtx), func(lt EC2LaunchTemplate) {
		out = append(out, resourceTagMapping{
			ResourceARN: taggingEC2ARN(lt.Region, lt.AccountID, "launch-template", lt.LaunchTemplateID),
			Tags:        ec2TagsToTaggingTags(lt.Tags),
			everTagged:  lt.EverTagged,
		})
	})
	if err != nil {
		return out, fmt.Errorf("scan ec2 launch templates: %w", err)
	}
	return out, nil
}

// scanEC2Volumes returns the caller's EBS volumes, including those a launch created from a
// TagSpecifications entry with ResourceType volume.
func (p *TaggingPlugin) scanEC2Volumes(ctx context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	var out []resourceTagMapping
	err := taggingScanRecords(ctx, p.state, ec2Namespace, ec2VolumeStatePrefix(reqCtx.AccountID, reqCtx.Region), func(v EC2Volume) {
		out = append(out, resourceTagMapping{
			ResourceARN: taggingEC2ARN(v.Region, v.AccountID, "volume", v.VolumeID),
			Tags:        ec2TagsToTaggingTags(v.Tags),
			everTagged:  v.EverTagged,
		})
	})
	if err != nil {
		return out, fmt.Errorf("scan ec2 volumes: %w", err)
	}
	return out, nil
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
