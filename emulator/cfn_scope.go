package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// CloudFormation's state keys are scoped by account and Region (#1366).
//
// A stack name is unique per account per Region, and so is everything named under a stack. Until
// #1366 the keys carried neither: a stack lived at `stack:<name>`, a change set at
// `changeset:<stack>/<name>`, the names index at `stack_names` and a drift detection at
// `drift_detection:<id>`. Two accounts, or two Regions of one account, that each created `app`
// addressed one record, so the second CreateStack was refused AlreadyExistsException and a
// DescribeStacks in one Region answered the other Region's stack. The scope is the deployer's
// identity, which the CloudFormation plugin sets from the request (see [WithDeployerIdentity]); an
// in-process deployer built without one is scoped to substrate's default account and Region.
//
// # Records written before the scope
//
// A state snapshot taken by an earlier substrate (the file and sqlite event stores keep them) holds
// the unscoped keys, and a replay that restores one would otherwise find none of its stacks. So a
// read falls back to the unscoped key when the scoped one is absent, but only for a record whose
// own AccountID and Region place it in the reader's scope ([StackDeployer.sameScope], where an
// empty field matches, as a stack recorded before those fields existed has neither). Every write
// to such a stack first moves it, its change sets and its drift detections to their scoped keys,
// so a legacy stack is migrated by the first thing that changes it and is never written twice.
// A replay of a recorded event stream needs none of this: it re-executes the requests against
// fresh state, and they write scoped keys.

// cfnScope is the account/Region segment every scoped key carries.
func (d *StackDeployer) cfnScope() string {
	return d.identity.accountID + "/" + d.identity.region
}

func (d *StackDeployer) stackKey(name string) string { return "stack:" + d.cfnScope() + "/" + name }

func (d *StackDeployer) stackNamesKey() string { return "stack_names:" + d.cfnScope() }

func (d *StackDeployer) changeSetKey(stack, name string) string {
	return "changeset:" + d.cfnScope() + "/" + stack + "/" + name
}

func (d *StackDeployer) changeSetNamesKey(stack string) string {
	return "changeset_names:" + d.cfnScope() + "/" + stack
}

func (d *StackDeployer) driftDetectionKey(id string) string {
	return "drift_detection:" + d.cfnScope() + "/" + id
}

// The unscoped keys substrate wrote before #1366, read only through the fallback above.
const (
	cfnLegacyStackNamesKey     = "stack_names"
	cfnLegacyDriftDetectionPfx = "drift_detection:"
)

func cfnLegacyStackKey(name string) string { return "stack:" + name }

func cfnLegacyChangeSetKey(stack, name string) string { return "changeset:" + stack + "/" + name }

func cfnLegacyChangeSetNamesKey(stack string) string { return "changeset_names:" + stack }

// stackData returns a stack's record: the scoped key's, or else an in-scope legacy record's, or nil.
func (d *StackDeployer) stackData(ctx context.Context, name string) ([]byte, error) {
	data, err := d.state.Get(ctx, cfnNamespace, d.stackKey(name))
	if err != nil {
		return nil, fmt.Errorf("cfn read stack %q: %w", name, err)
	}
	if data != nil {
		return data, nil
	}
	legacy, _, err := d.legacyStack(ctx, name)
	return legacy, err
}

// legacyStack returns the unscoped record for name when it belongs to the deployer's scope, and
// reports whether one was found. A legacy record that does not decode has no scope to check; it is
// returned as found, so every caller treats it the way it treats a corrupt scoped record.
func (d *StackDeployer) legacyStack(ctx context.Context, name string) ([]byte, bool, error) {
	data, err := d.state.Get(ctx, cfnNamespace, cfnLegacyStackKey(name))
	if err != nil {
		return nil, false, fmt.Errorf("cfn read legacy stack %q: %w", name, err)
	}
	if data == nil {
		return nil, false, nil
	}
	var s CFNStackState
	if json.Unmarshal(data, &s) == nil && !d.sameScope(s) {
		return nil, false, nil
	}
	return data, true, nil
}

// stackNames returns the names of the stacks in scope: the scoped index, plus any in-scope legacy
// stack not yet migrated.
func (d *StackDeployer) stackNames(ctx context.Context) ([]string, error) {
	names, err := d.loadStackNames(ctx)
	if err != nil {
		return nil, err
	}
	legacy, err := d.loadNameList(ctx, cfnLegacyStackNamesKey)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(names))
	for _, n := range names {
		seen[n] = true
	}
	for _, n := range legacy {
		if seen[n] {
			continue
		}
		_, found, err := d.legacyStack(ctx, n)
		if err != nil {
			return nil, err
		}
		if found {
			names = append(names, n)
			seen[n] = true
		}
	}
	return names, nil
}

// loadNameList reads a JSON string list, nil when absent.
func (d *StackDeployer) loadNameList(ctx context.Context, key string) ([]string, error) {
	data, err := d.state.Get(ctx, cfnNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("cfn read %s: %w", key, err)
	}
	if data == nil {
		return nil, nil
	}
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return nil, fmt.Errorf("cfn decode %s: %w", key, err)
	}
	return names, nil
}

// saveNameList writes a JSON string list.
func (d *StackDeployer) saveNameList(ctx context.Context, key string, names []string) error {
	data, err := json.Marshal(names)
	if err != nil {
		return fmt.Errorf("cfn encode %s: %w", key, err)
	}
	if err := d.state.Put(ctx, cfnNamespace, key, data); err != nil {
		return fmt.Errorf("cfn write %s: %w", key, err)
	}
	return nil
}

// migrateLegacyStack moves an in-scope legacy stack, with its change sets and drift detections, to
// the scoped keys, and removes the legacy keys and index entry. It does nothing when there is no
// such stack, which after the first write is always.
func (d *StackDeployer) migrateLegacyStack(ctx context.Context, name string) error {
	data, found, err := d.legacyStack(ctx, name)
	if err != nil || !found {
		return err
	}
	if err := d.state.Put(ctx, cfnNamespace, d.stackKey(name), data); err != nil {
		return fmt.Errorf("cfn migrate stack %q: %w", name, err)
	}
	if err := d.addStackName(ctx, name); err != nil {
		return err
	}

	csNames, err := d.loadNameList(ctx, cfnLegacyChangeSetNamesKey(name))
	if err != nil {
		return err
	}
	for _, cs := range csNames {
		csData, err := d.state.Get(ctx, cfnNamespace, cfnLegacyChangeSetKey(name, cs))
		if err != nil {
			return fmt.Errorf("cfn migrate change set %q: %w", cs, err)
		}
		if csData != nil {
			if err := d.state.Put(ctx, cfnNamespace, d.changeSetKey(name, cs), csData); err != nil {
				return fmt.Errorf("cfn migrate change set %q: %w", cs, err)
			}
		}
		if err := d.state.Delete(ctx, cfnNamespace, cfnLegacyChangeSetKey(name, cs)); err != nil {
			return fmt.Errorf("cfn migrate change set %q: %w", cs, err)
		}
	}
	if len(csNames) > 0 {
		if err := d.saveNameList(ctx, d.changeSetNamesKey(name), csNames); err != nil {
			return err
		}
	}
	if err := d.state.Delete(ctx, cfnNamespace, cfnLegacyChangeSetNamesKey(name)); err != nil {
		return fmt.Errorf("cfn migrate change set index of %q: %w", name, err)
	}

	if err := d.migrateLegacyDriftDetections(ctx, name); err != nil {
		return err
	}

	if err := d.state.Delete(ctx, cfnNamespace, cfnLegacyStackKey(name)); err != nil {
		return fmt.Errorf("cfn migrate stack %q: %w", name, err)
	}
	legacy, err := d.loadNameList(ctx, cfnLegacyStackNamesKey)
	if err != nil {
		return err
	}
	if len(legacy) > 0 {
		return d.saveNameList(ctx, cfnLegacyStackNamesKey, removeStr(legacy, name))
	}
	return nil
}

// migrateLegacyDriftDetections moves the unscoped drift detections of stack name to scoped keys. A
// legacy key's id carries no '/', which is how it is told apart from a scoped key under the same
// prefix.
func (d *StackDeployer) migrateLegacyDriftDetections(ctx context.Context, name string) error {
	keys, err := d.state.List(ctx, cfnNamespace, cfnLegacyDriftDetectionPfx)
	if err != nil {
		return fmt.Errorf("cfn list drift detections: %w", err)
	}
	for _, key := range keys {
		id := strings.TrimPrefix(key, cfnLegacyDriftDetectionPfx)
		if strings.Contains(id, "/") {
			continue
		}
		data, err := d.state.Get(ctx, cfnNamespace, key)
		if err != nil {
			return fmt.Errorf("cfn migrate drift detection %q: %w", id, err)
		}
		var status CFNDriftDetectionStatus
		if data == nil || json.Unmarshal(data, &status) != nil || status.StackName != name {
			continue
		}
		if err := d.state.Put(ctx, cfnNamespace, d.driftDetectionKey(id), data); err != nil {
			return fmt.Errorf("cfn migrate drift detection %q: %w", id, err)
		}
		if err := d.state.Delete(ctx, cfnNamespace, key); err != nil {
			return fmt.Errorf("cfn migrate drift detection %q: %w", id, err)
		}
	}
	return nil
}

// addStackName records name in the scoped index if it is not there.
func (d *StackDeployer) addStackName(ctx context.Context, name string) error {
	names, err := d.loadStackNames(ctx)
	if err != nil {
		return err
	}
	for _, n := range names {
		if n == name {
			return nil
		}
	}
	return d.saveStackNames(ctx, append(names, name))
}

// changeSetData returns a change set's record: the scoped key's, or an unmigrated in-scope legacy
// stack's, or nil.
func (d *StackDeployer) changeSetData(ctx context.Context, stack, name string) ([]byte, error) {
	data, err := d.state.Get(ctx, cfnNamespace, d.changeSetKey(stack, name))
	if err != nil {
		return nil, fmt.Errorf("cfn read change set %q: %w", name, err)
	}
	if data != nil {
		return data, nil
	}
	_, found, err := d.legacyStack(ctx, stack)
	if err != nil || !found {
		return nil, err
	}
	data, err = d.state.Get(ctx, cfnNamespace, cfnLegacyChangeSetKey(stack, name))
	if err != nil {
		return nil, fmt.Errorf("cfn read legacy change set %q: %w", name, err)
	}
	return data, nil
}

// driftDetectionData returns a drift detection's record: the scoped key's, or an unscoped one whose
// stack is an unmigrated in-scope legacy stack, or nil.
func (d *StackDeployer) driftDetectionData(ctx context.Context, id string) ([]byte, error) {
	data, err := d.state.Get(ctx, cfnNamespace, d.driftDetectionKey(id))
	if err != nil {
		return nil, fmt.Errorf("cfn read drift detection %q: %w", id, err)
	}
	if data != nil || strings.Contains(id, "/") {
		return data, nil
	}
	data, err = d.state.Get(ctx, cfnNamespace, cfnLegacyDriftDetectionPfx+id)
	if err != nil {
		return nil, fmt.Errorf("cfn read legacy drift detection %q: %w", id, err)
	}
	if data == nil {
		return nil, nil
	}
	var status CFNDriftDetectionStatus
	if decodeErr := json.Unmarshal(data, &status); decodeErr != nil {
		// A record that does not decode names no stack to scope it by. It is returned, so the
		// caller reports the decode error rather than an absent detection.
		return data, nil //nolint:nilerr // the caller decodes data again and returns that error
	}
	_, found, err := d.legacyStack(ctx, status.StackName)
	if err != nil || !found {
		return nil, err
	}
	return data, nil
}
