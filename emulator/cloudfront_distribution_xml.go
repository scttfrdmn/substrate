package emulator

import (
	"encoding/xml"
	"fmt"
	"time"
)

// One published type, one renderer (#1115).
//
// API_Distribution has seven Required: Yes members — ARN, DistributionConfig, DomainName, Id,
// InProgressInvalidationBatches, LastModifiedTime and Status — and Comment and Enabled are not among
// them: they are DistributionConfig members, which Distribution reaches only through its nested
// DistributionConfig. Until #1115 substrate rendered the type at two sites with two member sets.
// GetDistribution and UpdateDistribution answered Comment and Enabled one level too high and no
// DistributionConfig or InProgressInvalidationBatches; CreateDistribution answered four members and
// not even LastModifiedTime. [CloudFrontPlugin.distributionResponse] is now the only renderer, so
// the three operations cannot drift on the member set again.
//
// The nested DistributionConfig is the stored configuration exactly as GetDistributionConfig
// answers it ([cfStoredConfig]), so whatever that document carries or lacks, this one carries or
// lacks identically: the divergence #1091 recorded stays one divergence rather than two that can
// drift apart. The three optional members (ActiveTrustedKeyGroups, ActiveTrustedSigners,
// AliasICPRecordals) are absent: they report signer and ICP-recordal state substrate does not model.

// cfDistributionXML is the published Distribution shape, members in the API model's order.
type cfDistributionXML struct {
	XMLName                       xml.Name     `xml:"Distribution"`
	ID                            string       `xml:"Id"`
	ARN                           string       `xml:"ARN"`
	Status                        string       `xml:"Status"`
	LastModifiedTime              string       `xml:"LastModifiedTime"`
	InProgressInvalidationBatches int          `xml:"InProgressInvalidationBatches"`
	DomainName                    string       `xml:"DomainName"`
	DistributionConfig            cfConfigNode `xml:"DistributionConfig"`
}

// cfInProgressInvalidationBatches is API_Distribution's InProgressInvalidationBatches, "The number
// of invalidation batches currently in progress".
//
// Substrate completes an invalidation when it creates one — there is no edge cache to purge, so
// nothing is ever in progress (createInvalidation) — so the count is 0 by construction, and is
// answered as a constant rather than by reading every invalidation the distribution has on every
// read.
// TODO(#1581): count the in-progress batches once invalidations gain a seeded InProgress window.
const cfInProgressInvalidationBatches = 0

// distributionResponse answers dist as a Distribution document at httpStatus, reporting status — the
// request's observation of the seeded window (#1381) — rather than the record's own, with the
// distribution's ETag header, which CreateDistribution, GetDistribution and UpdateDistribution all
// publish (#1271).
func (p *CloudFrontPlugin) distributionResponse(dist CloudFrontDistribution, status string, httpStatus int) (*AWSResponse, error) {
	cfg, err := cfStoredConfig(dist)
	if err != nil {
		return nil, fmt.Errorf("cloudfront render distribution %s config: %w", dist.ID, err)
	}
	resp, err := cloudfrontXMLResponse(httpStatus, cfDistributionXML{
		ID:                            dist.ID,
		ARN:                           dist.ARN,
		Status:                        status,
		LastModifiedTime:              dist.LastModifiedTime.UTC().Format(time.RFC3339),
		InProgressInvalidationBatches: cfInProgressInvalidationBatches,
		DomainName:                    dist.DomainName,
		DistributionConfig:            cfg,
	})
	if err != nil {
		return nil, err
	}
	resp.Headers["ETag"] = cfDistributionETag(dist)
	return resp, nil
}

// cfSummaryConfigMembers returns the DistributionSummary members that are DistributionConfig
// members, in the API model's DistributionSummary order. A summary copies each from the stored
// configuration. A function rather than a package variable, so no caller can mutate the list.
func cfSummaryConfigMembers() []string {
	return []string{
		"Aliases", "Origins", "OriginGroups", "DefaultCacheBehavior", "CacheBehaviors",
		"CustomErrorResponses", "Comment", "PriceClass", "Enabled", "ViewerCertificate", "Restrictions",
		"WebACLId", "HttpVersion", "IsIPV6Enabled", "Staging", "ConnectionMode", "AnycastIpListId",
		"ViewerMtlsConfig", "ConnectionFunctionAssociation",
	}
}

// distributionSummary renders one ListDistributions item as API_DistributionSummary publishes it,
// with Status still to be set: ListDistributions builds every summary before it spends any seeded
// observation, so nothing a summary does can consume a countdown for a list that then fails.
//
// Six members are the distribution's own (Id, ARN, ETag, Status, LastModifiedTime, DomainName); the
// rest are configuration members, copied from the stored configuration in the summary's order so a
// summary and GetDistributionConfig cannot disagree. Until #1115 the summary answered six members, and
// Comment only when non-empty, though it is Required: Yes.
//
// The summary marks fourteen configuration members Required: Yes. Aliases, Comment and Enabled are
// always present — CreateDistribution records all three (cfDefaultCreatedConfig). The other eleven
// (Origins, DefaultCacheBehavior, CacheBehaviors, CustomErrorResponses, PriceClass,
// ViewerCertificate, Restrictions, WebACLId, HttpVersion, IsIPV6Enabled, Staging) are answered when
// the configuration carries them and absent when it does not, for the reason GetDistributionConfig
// omits them: CloudFront fills them in on create, and substrate will not invent values it has no
// published default for. AliasICPRecordals is absent, as on Distribution.
//
// A stored configuration that does not parse does not fail the list: the summary falls back to the
// Comment and Enabled the record itself holds — what the summary answered before #1115 — and the
// fault is logged. One bad record should not hide every other distribution in the account.
func (p *CloudFrontPlugin) distributionSummary(dist CloudFrontDistribution) cfConfigNode {
	text := func(local, value string) cfConfigNode {
		return cfConfigNode{XMLName: xml.Name{Local: local}, Text: value}
	}
	out := cfConfigNode{XMLName: xml.Name{Local: "DistributionSummary"}, Nodes: []cfConfigNode{
		text("Id", dist.ID),
		text("ARN", dist.ARN),
		text("ETag", cfDistributionETag(dist)),
		text("Status", ""),
		text("LastModifiedTime", dist.LastModifiedTime.UTC().Format(time.RFC3339)),
		text("DomainName", dist.DomainName),
	}}
	cfg, err := cfStoredConfig(dist)
	if err != nil {
		p.warn("cloudfront: stored configuration does not parse; summarizing from the record",
			"distribution", dist.ID, "err", err)
		out.Nodes = append(out.Nodes, text("Comment", dist.Comment), text("Enabled", cfBoolText(dist.Enabled)))
		return out
	}
	for _, member := range cfSummaryConfigMembers() {
		if c := cfg.child(member); c != nil {
			out.Nodes = append(out.Nodes, *c)
		}
	}
	return out
}
