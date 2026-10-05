package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// What a repository holds, as opposed to what its tag index names, and the two settings that
// govern a push and a listing (#1379).
//
// Until #1379 every listing was built from the tag index, so an image no tag named — pushed
// without one, or left behind when its last tag was removed or moved — existed (BatchGetImage by
// digest found it) and appeared in no listing. ListImages' page describes the very workflow that
// needs it: "filter your results to return only UNTAGGED images and then pipe that result to a
// BatchDeleteImage operation to delete them". Listings now enumerate the image records themselves,
// and the tag index only says which tags each one carries.

// ecrImageFilter is the filter ListImages and DescribeImages publish: ListImagesFilter and
// DescribeImagesFilter carry the same two members with the same Valid Values.
type ecrImageFilter struct {
	TagStatus   string `json:"tagStatus"`
	ImageStatus string `json:"imageStatus"`
}

// ecrTagStatuses is the Valid Values set both filter shapes publish for tagStatus.
var ecrTagStatuses = map[string]bool{"TAGGED": true, "UNTAGGED": true, "ANY": true}

// ecrImageStatuses is the Valid Values set both filter shapes publish for imageStatus.
var ecrImageStatuses = map[string]bool{"ACTIVE": true, "ARCHIVED": true, "ACTIVATING": true, "ANY": true}

// validate refuses a filter value outside its published Valid Values with InvalidParameterException,
// the code both pages publish for an invalid parameter.
func (f ecrImageFilter) validate() *AWSError {
	if f.TagStatus != "" && !ecrTagStatuses[f.TagStatus] {
		return ecrInvalidFilter("tagStatus", f.TagStatus, "TAGGED, UNTAGGED, ANY")
	}
	if f.ImageStatus != "" && !ecrImageStatuses[f.ImageStatus] {
		return ecrInvalidFilter("imageStatus", f.ImageStatus, "ACTIVE, ARCHIVED, ACTIVATING, ANY")
	}
	return nil
}

// admitsTags reports whether the filter admits an image carrying tags (tagged true) or none.
//
// No tagStatus is ANY: ListImages' sample answers every image ID in the repository for a request
// naming no filter, and neither page states a narrower default.
func (f ecrImageFilter) admitsTags(tagged bool) bool {
	switch f.TagStatus {
	case "TAGGED":
		return tagged
	case "UNTAGGED":
		return !tagged
	default:
		return true
	}
}

// admitsActive reports whether the filter admits an ACTIVE image, which every image substrate holds
// is: it models no archiving, so ARCHIVED and ACTIVATING select nothing. An absent imageStatus is
// ACTIVE, as both filter pages state ("If not specified, only images with ACTIVE status are
// returned").
func (f ecrImageFilter) admitsActive() bool {
	return f.ImageStatus == "" || f.ImageStatus == "ACTIVE" || f.ImageStatus == "ANY"
}

// ecrInvalidFilter is InvalidParameterException naming the filter member and its Valid Values.
func ecrInvalidFilter(member, value, valid string) *AWSError {
	return &AWSError{
		Code: "InvalidParameterException",
		Message: "Invalid parameter at 'filter." + member + "' failed to satisfy constraint: " +
			"'Member must satisfy enum value set: [" + valid + "]', value '" + value + "'",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ecrImageTagAlreadyExists reports a push that would move a tag on a repository configured for tag
// immutability.
//
// API_PutImage publishes ImageTagAlreadyExistsException at 400, for "The specified image is tagged
// with a tag that already exists. The repository is configured for tag immutability".
func ecrImageTagAlreadyExists(repositoryName, tag string) *AWSError {
	return &AWSError{
		Code: "ImageTagAlreadyExistsException",
		Message: "The image tag '" + tag + "' already exists in the '" + repositoryName +
			"' repository and cannot be overwritten because the repository is immutable.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ecrTagsImmutable reports whether a repository's imageTagMutability forbids moving a tag.
//
// IMMUTABLE_WITH_EXCLUSION is immutable apart from the tags its imageTagMutabilityExclusionFilters
// name, and substrate models no exclusion filters (see [ecrImageTagMutabilityValues]), so it reads
// as IMMUTABLE: with no filter, no tag is excluded. MUTABLE_WITH_EXCLUSION reads as MUTABLE for the
// same reason. A record written before the setting existed holds "", the published MUTABLE default.
func ecrTagsImmutable(mutability string) bool {
	return mutability == "IMMUTABLE" || mutability == "IMMUTABLE_WITH_EXCLUSION"
}

// ecrRepositoryMutability returns the imageTagMutability of the repository whose record is data.
func ecrRepositoryMutability(data []byte) (string, error) {
	var repo ECRRepository
	if err := json.Unmarshal(data, &repo); err != nil {
		return "", fmt.Errorf("ecr repository unmarshal: %w", err)
	}
	return repo.ImageTagMutability, nil
}

// repositoryImageDigests returns the digest of every image record in one repository, sorted, which
// is the stable order pageByOffsetToken requires of its caller.
//
// A repository name may contain '/' ("team/app"), so the key prefix of "team" is also a prefix of
// every "team/app" image key. Only a key whose remainder is a bare digest — which never contains
// '/' — belongs to the repository asked for.
func (p *ECRPlugin) repositoryImageDigests(goCtx context.Context, ctx *RequestContext, repo string) ([]string, error) {
	prefix := ecrImageKey(ctx.AccountID, ctx.Region, repo, "")
	keys, err := p.state.List(goCtx, ecrNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("ecr list images state.List: %w", err)
	}
	digests := make([]string, 0, len(keys))
	for _, k := range keys {
		rest := strings.TrimPrefix(k, prefix)
		if rest == "" || strings.Contains(rest, "/") {
			continue
		}
		digests = append(digests, rest)
	}
	sort.Strings(digests)
	return digests, nil
}

// ecrTagsByDigest inverts a tag index into the sorted tags each digest carries.
func ecrTagsByDigest(tagsMap map[string]string) map[string][]string {
	byDigest := make(map[string][]string)
	for tag, digest := range tagsMap {
		byDigest[digest] = append(byDigest[digest], tag)
	}
	for d := range byDigest {
		sort.Strings(byDigest[d])
	}
	return byDigest
}
