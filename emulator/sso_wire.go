package emulator

// The wire is a different thing from the state, and the type below exists to keep them apart.
//
// SSOPermissionSet (sso_types.go) is a persisted state record, and createPermissionSet and
// describePermissionSet handed it straight to the caller under `PermissionSet`. That answered
// AccountID, which no IAM Identity Center shape publishes (#756), and InstanceArn, which
// API_PermissionSet does not publish either: the instance is what the request names. It also rendered
// CreatedDate as an RFC3339 string (#1345).
//
// Do not "fix" that by adding `json:"-"` to a state field or by retyping the date in the record.
// Either changes the format of every recorded run, because MemoryStateManager snapshots those bytes
// and a replay reads them back. Projecting leaves the stored bytes unchanged.
//
// # Why IAM Identity Center's dates are EpochSeconds
//
// The service speaks awsJson1_1, where a Timestamp is published as epoch seconds with fractional
// precision: API_DescribePermissionSet renders `"CreatedDate": number`, and API_ListInstances renders
// each instance's `"CreatedDate": number`. A Go time.Time marshals to RFC3339, which an awsJson1_1
// timestamp deserializer refuses, so a typed SDK could decode neither response (#1345).

// ssoPermissionSetOut is the PermissionSet element of CreatePermissionSet's and DescribePermissionSet's
// responses: all six of API_PermissionSet's members.
type ssoPermissionSetOut struct {
	CreatedDate      EpochSeconds `json:"CreatedDate"`
	Description      string       `json:"Description,omitempty"`
	Name             string       `json:"Name"`
	PermissionSetArn string       `json:"PermissionSetArn"`
	RelayState       string       `json:"RelayState,omitempty"`
	SessionDuration  string       `json:"SessionDuration,omitempty"`
}

// ssoPermissionSetToWire projects a persisted permission set onto the published shape.
func ssoPermissionSetToWire(ps SSOPermissionSet) ssoPermissionSetOut {
	return ssoPermissionSetOut{
		CreatedDate:      EpochSeconds(ps.CreatedDate),
		Description:      ps.Description,
		Name:             ps.Name,
		PermissionSetArn: ps.PermissionSetArn,
		RelayState:       ps.RelayState,
		SessionDuration:  ps.SessionDuration,
	}
}
