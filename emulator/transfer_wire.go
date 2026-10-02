package emulator

// The wire is a different thing from the state, and the two types below exist to keep them
// apart.
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
// Eight of API_DescribedServer's members; the rest are not modeled and are absent rather than
// present and empty (#1013's rule, #1199's gap). UserCount is carried as the record holds it, which
// is never assigned and so always 0 — that is #1199's criterion, not this type's.
type transferServerOut struct {
	Arn                  string        `json:"Arn"`
	Domain               string        `json:"Domain,omitempty"`
	EndpointType         string        `json:"EndpointType,omitempty"`
	IdentityProviderType string        `json:"IdentityProviderType,omitempty"`
	ServerID             string        `json:"ServerId"`
	State                string        `json:"State"`
	Tags                 []TransferTag `json:"Tags,omitempty"`
	UserCount            int           `json:"UserCount"`
}

// transferServerToWire projects a persisted server onto the published shape.
func transferServerToWire(server TransferServer) transferServerOut {
	return transferServerOut{
		Arn:                  server.Arn,
		Domain:               server.Domain,
		EndpointType:         server.EndpointType,
		IdentityProviderType: server.IdentityProviderType,
		ServerID:             server.ServerID,
		State:                server.State,
		Tags:                 server.Tags,
		UserCount:            server.UserCount,
	}
}

// transferUserOut is the User element of DescribeUser's response.
//
// Five of API_DescribedUser's members, and no ServerId: the shape publishes none, and the response
// carries the server's ID at the top level instead.
type transferUserOut struct {
	Arn           string        `json:"Arn"`
	HomeDirectory string        `json:"HomeDirectory,omitempty"`
	Role          string        `json:"Role,omitempty"`
	Tags          []TransferTag `json:"Tags,omitempty"`
	UserName      string        `json:"UserName"`
}

// transferUserToWire projects a persisted user onto the published shape.
func transferUserToWire(user TransferUser) transferUserOut {
	return transferUserOut{
		Arn:           user.Arn,
		HomeDirectory: user.HomeDirectory,
		Role:          user.Role,
		Tags:          user.Tags,
		UserName:      user.UserName,
	}
}
