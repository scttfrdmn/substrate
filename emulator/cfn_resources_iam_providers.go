package emulator

import (
	"context"
	"slices"
	"strconv"
)

// CloudFormation's AWS::IAM::OIDCProvider and AWS::IAM::SAMLProvider. Both are named by their
// ARN: "Ref returns the ARN" on each page, so the physical ID is the ARN the create answers.
// Their identifying property — Url, Name — is Replacement (cfnRecreationProperties); every other
// property is No interruption and is applied by an updater here, diffed against the live record.

// cfnIAMListParams renders values as the `<name>.member.N` query list IAM's list parameters take.
func cfnIAMListParams(params map[string]string, name string, values []string) map[string]string {
	if params == nil {
		params = map[string]string{}
	}
	for i, v := range values {
		params[name+".member."+strconv.Itoa(i+1)] = v
	}
	return params
}

// cfnIAMProviderLive is what an updater reads back from a provider before diffing.
type cfnIAMProviderLive struct {
	ClientIDs   []string `xml:"GetOpenIDConnectProviderResult>ClientIDList>member"`
	Thumbprints []string `xml:"GetOpenIDConnectProviderResult>ThumbprintList>member"`
	OIDCTags    []IAMTag `xml:"GetOpenIDConnectProviderResult>Tags>member"`
	SAMLTags    []IAMTag `xml:"GetSAMLProviderResult>Tags>member"`
	UUID        string   `xml:"GetSAMLProviderResult>SAMLProviderUUID"`
	Metadata    string   `xml:"GetSAMLProviderResult>SAMLMetadataDocument"`
	Encryption  string   `xml:"GetSAMLProviderResult>AssertionEncryptionMode"`
}

// readIAMProvider reads a provider back through its Get operation.
func (d *StackDeployer) readIAMProvider(ctx context.Context, op, arnParam, arn, streamID string,
) (cfnIAMProviderLive, float64, error) {
	var live cfnIAMProviderLive
	resp, cost, err := d.iamCall(ctx, op, map[string]string{arnParam: arn}, nil, streamID)
	if err != nil {
		return live, cost, err
	}
	if resp != nil {
		if xmlErr := xmlUnmarshalIAM(resp.Body, &live); xmlErr != nil {
			return live, cost, xmlErr
		}
	}
	return live, cost, nil
}

// syncIAMProviderTags brings a provider's tags from live to want, through Tag… and Untag….
func (d *StackDeployer) syncIAMProviderTags(ctx context.Context, kind, arnParam, arn string,
	live []IAMTag, want map[string]string, streamID string) (float64, error) {
	var total float64
	liveMap := make(map[string]string, len(live))
	var removed []string
	for _, t := range live {
		liveMap[t.Key] = t.Value
		if _, ok := want[t.Key]; !ok {
			removed = append(removed, t.Key)
		}
	}
	if len(removed) > 0 {
		_, cost, err := d.iamCall(ctx, "Untag"+kind, map[string]string{arnParam: arn},
			cfnIAMListParams(nil, "TagKeys", removed), streamID)
		total += cost
		if err != nil {
			return total, err
		}
	}
	changed := false
	for k, v := range want {
		if lv, ok := liveMap[k]; !ok || lv != v {
			changed = true
			break
		}
	}
	if changed {
		_, cost, err := d.iamCall(ctx, "Tag"+kind, map[string]string{arnParam: arn}, cfnIAMTagParams(want), streamID)
		total += cost
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// deployIAMOIDCProvider creates an AWS::IAM::OIDCProvider through CreateOpenIDConnectProvider.
func (d *StackDeployer) deployIAMOIDCProvider(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::IAM::OIDCProvider"}
	params := cfnIAMTagParams(cfnTagMap(props["Tags"], cctx))
	params = cfnIAMListParams(params, "ClientIDList", resolveStringList(props["ClientIdList"], cctx))
	params = cfnIAMListParams(params, "ThumbprintList", resolveStringList(props["ThumbprintList"], cctx))
	resp, total, err := d.iamCall(ctx, "CreateOpenIDConnectProvider",
		map[string]string{"Url": resolveStringProp(props, "Url", "", cctx)}, params, streamID)
	if err != nil {
		dr.Error = err.Error()
		return dr, total, nil //nolint:nilerr // A refused create is reported on the resource.
	}
	var result struct {
		ARN string `xml:"CreateOpenIDConnectProviderResult>OpenIDConnectProviderArn"`
	}
	if resp != nil && xmlUnmarshalIAM(resp.Body, &result) == nil {
		dr.PhysicalID, dr.ARN = result.ARN, result.ARN
	}
	return dr, total, nil
}

// updateIAMOIDCProvider applies a changed ClientIdList, ThumbprintList or Tags in place.
func (d *StackDeployer) updateIAMOIDCProvider(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, _ []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	dr.Error = ""
	arn := prior.PhysicalID
	const arnParam = "OpenIDConnectProviderArn"
	live, total, err := d.readIAMProvider(ctx, "GetOpenIDConnectProvider", arnParam, arn, streamID)
	if err != nil {
		dr.Error = err.Error()
		return dr, total, nil //nolint:nilerr // A refused update is reported on the resource.
	}
	steps := []func() (float64, error){
		func() (float64, error) {
			want := resolveStringList(props["ClientIdList"], cctx)
			var cost float64
			for _, id := range want {
				if !slices.Contains(live.ClientIDs, id) {
					_, c, err := d.iamCall(ctx, "AddClientIDToOpenIDConnectProvider",
						map[string]string{arnParam: arn, "ClientID": id}, nil, streamID)
					cost += c
					if err != nil {
						return cost, err
					}
				}
			}
			for _, id := range live.ClientIDs {
				if !slices.Contains(want, id) {
					_, c, err := d.iamCall(ctx, "RemoveClientIDFromOpenIDConnectProvider",
						map[string]string{arnParam: arn, "ClientID": id}, nil, streamID)
					cost += c
					if err != nil {
						return cost, err
					}
				}
			}
			return cost, nil
		},
		func() (float64, error) {
			// A ThumbprintList the declaration no longer carries clears the live list: substrate
			// fetches no thumbprint in its place.
			want := resolveStringList(props["ThumbprintList"], cctx)
			if slices.Equal(want, live.Thumbprints) || (len(want) == 0 && len(live.Thumbprints) == 0) {
				return 0, nil
			}
			_, cost, err := d.iamCall(ctx, "UpdateOpenIDConnectProviderThumbprint",
				map[string]string{arnParam: arn}, cfnIAMListParams(nil, "ThumbprintList", want), streamID)
			return cost, err
		},
		func() (float64, error) {
			return d.syncIAMProviderTags(ctx, "OpenIDConnectProvider", arnParam, arn, live.OIDCTags,
				cfnTagMap(props["Tags"], cctx), streamID)
		},
	}
	for _, step := range steps {
		cost, err := step()
		total += cost
		if err != nil {
			dr.Error = err.Error()
			return dr, total, nil //nolint:nilerr // A refused update is reported on the resource.
		}
	}
	return dr, total, nil
}

// deployIAMSAMLProvider creates an AWS::IAM::SAMLProvider through CreateSAMLProvider, and records
// the SamlProviderUUID attribute GetSAMLProvider reports.
func (d *StackDeployer) deployIAMSAMLProvider(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::IAM::SAMLProvider"}
	body := map[string]string{
		"Name":                 resolveStringProp(props, "Name", cfnGeneratedName(cctx, "AWS::IAM::SAMLProvider", logicalID), cctx),
		"SAMLMetadataDocument": resolveStringProp(props, "SamlMetadataDocument", "", cctx),
	}
	if mode := resolveStringProp(props, "AssertionEncryptionMode", "", cctx); mode != "" {
		body["AssertionEncryptionMode"] = mode
	}
	resp, total, err := d.iamCall(ctx, "CreateSAMLProvider", body, cfnIAMTagParams(cfnTagMap(props["Tags"], cctx)), streamID)
	if err != nil {
		dr.Error = err.Error()
		return dr, total, nil //nolint:nilerr // A refused create is reported on the resource.
	}
	var result struct {
		ARN string `xml:"CreateSAMLProviderResult>SAMLProviderArn"`
	}
	if resp != nil && xmlUnmarshalIAM(resp.Body, &result) == nil {
		dr.PhysicalID, dr.ARN = result.ARN, result.ARN
	}
	live, cost, err := d.readIAMProvider(ctx, "GetSAMLProvider", "SAMLProviderArn", dr.PhysicalID, streamID)
	total += cost
	if err == nil && live.UUID != "" {
		dr.Metadata = map[string]interface{}{"SamlProviderUUID": live.UUID}
	}
	return dr, total, nil
}

// updateIAMSAMLProvider applies a changed SamlMetadataDocument, AssertionEncryptionMode or Tags in
// place through UpdateSAMLProvider and the tag operations.
func (d *StackDeployer) updateIAMSAMLProvider(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, _ []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	dr.Metadata = cfnCopyMetadata(prior.Metadata)
	dr.Error = ""
	arn := prior.PhysicalID
	const arnParam = "SAMLProviderArn"
	live, total, err := d.readIAMProvider(ctx, "GetSAMLProvider", arnParam, arn, streamID)
	if err != nil {
		dr.Error = err.Error()
		return dr, total, nil //nolint:nilerr // A refused update is reported on the resource.
	}
	body := map[string]string{arnParam: arn}
	if doc := resolveStringProp(props, "SamlMetadataDocument", "", cctx); doc != "" && doc != live.Metadata {
		body["SAMLMetadataDocument"] = doc
	}
	// An AssertionEncryptionMode the declaration no longer carries returns the provider to
	// Allowed, the mode that accepts the unencrypted assertions a provider created without one does.
	mode := resolveStringProp(props, "AssertionEncryptionMode", "", cctx)
	if mode == "" && live.Encryption == "Required" {
		mode = "Allowed"
	}
	if mode != "" && mode != live.Encryption {
		body["AssertionEncryptionMode"] = mode
	}
	if len(body) > 1 {
		_, cost, err := d.iamCall(ctx, "UpdateSAMLProvider", body, nil, streamID)
		total += cost
		if err != nil {
			dr.Error = err.Error()
			return dr, total, nil //nolint:nilerr // A refused update is reported on the resource.
		}
	}
	cost, err := d.syncIAMProviderTags(ctx, "SAMLProvider", arnParam, arn, live.SAMLTags,
		cfnTagMap(props["Tags"], cctx), streamID)
	total += cost
	if err != nil {
		dr.Error = err.Error()
	}
	return dr, total, nil
}
