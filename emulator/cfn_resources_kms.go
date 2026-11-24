package emulator

// StackDeployer helpers for CloudFormation's KMS resource types. The rules every in-place updater
// follows are stated with cfnUpdateUnmodelledProperties.

import (
	"context"
	"slices"
	"strconv"
)

// ----- AWS::KMS::Key --------------------------------------------------------------------

// configureKMSKey applies a key's Enabled, EnableKeyRotation/RotationPeriodInDays, KeyPolicy and
// Description for the properties in apply, and returns the first refusal.
//
// The create applies the declared ones its CreateKey does not take — rotation and Enabled — and the
// update every changed one. The order is the create's, and keeps each call legal whatever state
// the key is in: EnableKeyRotation and DisableKeyRotation refuse a disabled key, so a key that ends
// enabled is enabled first; one that ends disabled is disabled last; and a rotation change on a key
// that is disabled and stays so enables it for the rotation call and disables it again after. A
// rollback therefore re-applies a disabled key's rotation without being refused. Whether the key is
// enabled now is read from the previous declaration (priorProps), the create's being enabled.
//
// A removed property resets to the page's default: Enabled "The default value is true",
// EnableKeyRotation "By default, automatic key rotation is not enabled", RotationPeriodInDays "the
// default value is 365 days", and KeyPolicy "If you do not provide a key policy, AWS KMS attaches
// a default key policy" — KMS's default document, which PutKeyPolicy writes back. A removed
// Description is set to the empty string the create sends for an undeclared one.
func (d *StackDeployer) configureKMSKey(
	ctx context.Context, keyID string, props map[string]interface{}, apply []string, creating bool,
	streamID string, cctx *cfnContext,
) (float64, error) {
	var cost float64
	call := func(operation string, body map[string]interface{}) error {
		body["KeyId"] = keyID
		req, err := cfnJSONRequest("kms", operation, body)
		if err != nil {
			return err
		}
		return d.cfnUpdateDispatch(ctx, req, streamID, &cost)
	}
	enabled := true
	if v, ok := cfnBoolValue(resolveNested(props["Enabled"], cctx)); ok {
		enabled = v
	}
	wasEnabled := true
	if !creating {
		if v, ok := cfnBoolValue(cctx.priorProps["Enabled"]); ok {
			wasEnabled = v
		}
	}
	rotation := cfnChangedAny(apply, "EnableKeyRotation", "RotationPeriodInDays")
	if !wasEnabled && (enabled || rotation) {
		if err := call("EnableKey", map[string]interface{}{}); err != nil {
			return cost, err
		}
	}
	if rotation {
		rotate, _ := cfnBoolValue(resolveNested(props["EnableKeyRotation"], cctx))
		var err error
		switch {
		case rotate:
			body := map[string]interface{}{}
			if p := resolveStringProp(props, "RotationPeriodInDays", "", cctx); p != "" {
				if n, convErr := strconv.Atoi(p); convErr == nil {
					body["RotationPeriodInDays"] = n
				}
			}
			err = call("EnableKeyRotation", body)
		case !creating:
			err = call("DisableKeyRotation", map[string]interface{}{})
		}
		if err != nil {
			return cost, err
		}
	}
	if !creating && slices.Contains(apply, "KeyPolicy") {
		policy := cfnJSONDocument(props["KeyPolicy"], cctx)
		if policy == "" {
			policy = kmsDefaultKeyPolicy(cctx.accountID)
		}
		if err := call("PutKeyPolicy", map[string]interface{}{"PolicyName": kmsDefaultPolicyName, "Policy": policy}); err != nil {
			return cost, err
		}
	}
	if !creating && slices.Contains(apply, "Description") {
		if err := call("UpdateKeyDescription", map[string]interface{}{
			"Description": resolveStringProp(props, "Description", "", cctx),
		}); err != nil {
			return cost, err
		}
	}
	if !enabled && (wasEnabled || rotation) {
		if err := call("DisableKey", map[string]interface{}{}); err != nil {
			return cost, err
		}
	}
	return cost, nil
}

// updateKMSKey applies a changed AWS::KMS::Key through UpdateKeyDescription, PutKeyPolicy,
// EnableKeyRotation/DisableKeyRotation and EnableKey/DisableKey (see
// [StackDeployer.configureKMSKey]). KeySpec, KeyUsage, MultiRegion and Origin cannot change and
// fail the update (cfnCreateOnlyProperties). PendingWindowInDays and BypassPolicyLockoutSafetyCheck
// make no call: the first governs the delete, and the second only how a policy is checked.
func (d *StackDeployer) updateKMSKey(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	cost, err := d.configureKMSKey(ctx, prior.PhysicalID, props, changed, false, streamID, cctx)
	if err != nil {
		dr.Error = err.Error()
	}
	return dr, cost, nil
}
