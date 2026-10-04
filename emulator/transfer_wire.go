package emulator

import "encoding/json"

// The wire is a different thing from the state, and the types below exist to keep them apart.
//
// TransferServer and TransferUser (transfer_types.go) are persisted state records, and each was
// handed straight to the caller at the one site that answers it: describeServer under `Server`,
// describeUser under `User`. Two fields of each are substrate's own and neither carries omitempty,
// so both responses answered AccountID and Region, which no Transfer shape publishes (#756), and
// the server also answered CreatedAt, which API_DescribedServer does not publish either (#1199
// recorded both). The user answered ServerId inside `User` as well, where API_DescribedUser has no
// such member; the published home for it is the top-level ServerId of DescribeUserResponse, which
// the handler already answers.
//
// Do not "fix" that by adding `json:"-"` to a state field. That works for the one field and leaves
// the next one to be remembered rather than prevented, and it changes the format of every recorded
// run, because MemoryStateManager snapshots those bytes and a replay reads them back. Projecting
// leaves the stored bytes unchanged, so a run recorded before this replays identically.

// transferServerOut is the Server element of DescribeServer's response.
//
// API_DescribedServer's members, less two the record does not model, which are absent rather than
// present and empty (#1013's rule, #1199):
//   - HostKeyFingerprint: substrate holds no host key to fingerprint. A HostKey sent to CreateServer
//     or UpdateServer is a private key and is not stored.
//   - As2ServiceManagedEgressIpAddresses: no AS2 endpoint is modeled, so no address is assigned.
//
// UserCount is counted from the server's user index when the response is built (#1199). The stored
// field was declared and never assigned, so it reported 0 however many users existed.
type transferServerOut struct {
	Arn                           string          `json:"Arn"`
	Certificate                   string          `json:"Certificate,omitempty"`
	Domain                        string          `json:"Domain,omitempty"`
	EndpointDetails               json.RawMessage `json:"EndpointDetails,omitempty"`
	EndpointType                  string          `json:"EndpointType,omitempty"`
	IdentityProviderDetails       json.RawMessage `json:"IdentityProviderDetails,omitempty"`
	IdentityProviderType          string          `json:"IdentityProviderType,omitempty"`
	IPAddressType                 string          `json:"IpAddressType,omitempty"`
	LoggingRole                   string          `json:"LoggingRole,omitempty"`
	PostAuthenticationLoginBanner string          `json:"PostAuthenticationLoginBanner,omitempty"`
	PreAuthenticationLoginBanner  string          `json:"PreAuthenticationLoginBanner,omitempty"`
	ProtocolDetails               json.RawMessage `json:"ProtocolDetails,omitempty"`
	Protocols                     []string        `json:"Protocols,omitempty"`
	S3StorageOptions              json.RawMessage `json:"S3StorageOptions,omitempty"`
	SecurityPolicyName            string          `json:"SecurityPolicyName,omitempty"`
	ServerID                      string          `json:"ServerId"`
	State                         string          `json:"State"`
	StructuredLogDestinations     []string        `json:"StructuredLogDestinations,omitempty"`
	Tags                          []TransferTag   `json:"Tags,omitempty"`
	UserCount                     int             `json:"UserCount"`
	WorkflowDetails               json.RawMessage `json:"WorkflowDetails,omitempty"`
}

// transferServerToWire projects a persisted server onto the published shape, with userCount
// counted by the caller.
func transferServerToWire(server TransferServer, userCount int) transferServerOut {
	return transferServerOut{
		Arn:                           server.Arn,
		Certificate:                   server.Certificate,
		Domain:                        server.Domain,
		EndpointDetails:               server.EndpointDetails,
		EndpointType:                  server.EndpointType,
		IdentityProviderDetails:       server.IdentityProviderDetails,
		IdentityProviderType:          server.IdentityProviderType,
		IPAddressType:                 server.IPAddressType,
		LoggingRole:                   server.LoggingRole,
		PostAuthenticationLoginBanner: server.PostAuthenticationLoginBanner,
		PreAuthenticationLoginBanner:  server.PreAuthenticationLoginBanner,
		ProtocolDetails:               server.ProtocolDetails,
		Protocols:                     server.Protocols,
		S3StorageOptions:              server.S3StorageOptions,
		SecurityPolicyName:            server.SecurityPolicyName,
		ServerID:                      server.ServerID,
		State:                         server.State,
		StructuredLogDestinations:     server.StructuredLogDestinations,
		Tags:                          server.Tags,
		UserCount:                     userCount,
		WorkflowDetails:               server.WorkflowDetails,
	}
}

// transferListedServerOut is one element of ListServers' Servers array: all eight of
// API_ListedServer's members.
type transferListedServerOut struct {
	Arn                  string `json:"Arn"`
	Domain               string `json:"Domain,omitempty"`
	EndpointType         string `json:"EndpointType,omitempty"`
	IdentityProviderType string `json:"IdentityProviderType,omitempty"`
	LoggingRole          string `json:"LoggingRole,omitempty"`
	ServerID             string `json:"ServerId"`
	State                string `json:"State"`
	UserCount            int    `json:"UserCount"`
}

// transferServerToListed projects a persisted server onto ListedServer.
func transferServerToListed(server TransferServer, userCount int) transferListedServerOut {
	return transferListedServerOut{
		Arn:                  server.Arn,
		Domain:               server.Domain,
		EndpointType:         server.EndpointType,
		IdentityProviderType: server.IdentityProviderType,
		LoggingRole:          server.LoggingRole,
		ServerID:             server.ServerID,
		State:                server.State,
		UserCount:            userCount,
	}
}

// transferUserOut is the User element of DescribeUser's response: all ten of API_DescribedUser's
// members, and no ServerId. The shape publishes none, and the response carries the server's ID at
// the top level instead.
type transferUserOut struct {
	Arn                   string                    `json:"Arn"`
	HomeDirectory         string                    `json:"HomeDirectory,omitempty"`
	HomeDirectoryMappings json.RawMessage           `json:"HomeDirectoryMappings,omitempty"`
	HomeDirectoryType     string                    `json:"HomeDirectoryType,omitempty"`
	Policy                string                    `json:"Policy,omitempty"`
	PosixProfile          json.RawMessage           `json:"PosixProfile,omitempty"`
	Role                  string                    `json:"Role,omitempty"`
	SSHPublicKeys         []transferSSHPublicKeyOut `json:"SshPublicKeys"`
	Tags                  []TransferTag             `json:"Tags,omitempty"`
	UserName              string                    `json:"UserName"`
}

// transferSSHPublicKeyOut is API_SshPublicKey. DateImported is a Timestamp, which the awsJson1_1
// protocol Transfer speaks publishes as epoch seconds.
type transferSSHPublicKeyOut struct {
	DateImported     EpochSeconds `json:"DateImported"`
	SSHPublicKeyBody string       `json:"SshPublicKeyBody"`
	SSHPublicKeyID   string       `json:"SshPublicKeyId"`
}

// transferUserToWire projects a persisted user onto the published shape. SSHPublicKeys is answered
// as an array even when empty: the shape publishes it as 0–5 items, and an empty array is what the
// page's own "SshPublicKeys: []" note shows for a user with none.
func transferUserToWire(user TransferUser) transferUserOut {
	keys := make([]transferSSHPublicKeyOut, 0, len(user.SSHPublicKeys))
	for _, k := range user.SSHPublicKeys {
		keys = append(keys, transferSSHPublicKeyOut{
			DateImported:     EpochSeconds(k.DateImported),
			SSHPublicKeyBody: k.SSHPublicKeyBody,
			SSHPublicKeyID:   k.SSHPublicKeyID,
		})
	}
	return transferUserOut{
		Arn:                   user.Arn,
		HomeDirectory:         user.HomeDirectory,
		HomeDirectoryMappings: user.HomeDirectoryMappings,
		HomeDirectoryType:     user.HomeDirectoryType,
		Policy:                user.Policy,
		PosixProfile:          user.PosixProfile,
		Role:                  user.Role,
		SSHPublicKeys:         keys,
		Tags:                  user.Tags,
		UserName:              user.UserName,
	}
}

// transferListedUserOut is one element of ListUsers' Users array: all six of API_ListedUser's
// members.
type transferListedUserOut struct {
	Arn               string `json:"Arn"`
	HomeDirectory     string `json:"HomeDirectory,omitempty"`
	HomeDirectoryType string `json:"HomeDirectoryType,omitempty"`
	Role              string `json:"Role,omitempty"`
	SSHPublicKeyCount int    `json:"SshPublicKeyCount"`
	UserName          string `json:"UserName"`
}

// transferUserToListed projects a persisted user onto ListedUser.
func transferUserToListed(user TransferUser) transferListedUserOut {
	return transferListedUserOut{
		Arn:               user.Arn,
		HomeDirectory:     user.HomeDirectory,
		HomeDirectoryType: user.HomeDirectoryType,
		Role:              user.Role,
		SSHPublicKeyCount: len(user.SSHPublicKeys),
		UserName:          user.UserName,
	}
}
