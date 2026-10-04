package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// TransferPlugin emulates the AWS Transfer Family service.
// It handles SFTP/FTP server and user CRUD operations using the
// Transfer Family JSON-target protocol (X-Amz-Target: TransferService.{Op}).
type TransferPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "transfer".
func (p *TransferPlugin) Name() string { return transferNamespace }

// Initialize sets up the TransferPlugin with the provided configuration.
func (p *TransferPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for TransferPlugin.
func (p *TransferPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Transfer Family JSON-target request to the appropriate handler.
func (p *TransferPlugin) HandleRequest(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "CreateServer":
		return p.createServer(reqCtx, req)
	case "DescribeServer":
		return p.describeServer(reqCtx, req)
	case "UpdateServer":
		return p.updateServer(reqCtx, req)
	case "DeleteServer":
		return p.deleteServer(reqCtx, req)
	case "ListServers":
		return p.listServers(reqCtx, req)
	case "CreateUser":
		return p.createUser(reqCtx, req)
	case "DescribeUser":
		return p.describeUser(reqCtx, req)
	case "UpdateUser":
		return p.updateUser(reqCtx, req)
	case "DeleteUser":
		return p.deleteUser(reqCtx, req)
	case "ListUsers":
		return p.listUsers(reqCtx, req)
	default:
		return nil, unknownActionError(p.Name(), req.Operation)
	}
}

// transferServerSettings is the set of server members CreateServer and UpdateServer share.
//
// Every member is a pointer, or a json.RawMessage, so an update can tell a member the request
// omitted (leave the server's value) from one it sent (replace it). CreateServer reads the same
// struct and applies the published defaults to what it omitted.
type transferServerSettings struct {
	Certificate                   *string         `json:"Certificate"`
	EndpointDetails               json.RawMessage `json:"EndpointDetails"`
	EndpointType                  *string         `json:"EndpointType"`
	HostKey                       *string         `json:"HostKey"`
	IdentityProviderDetails       json.RawMessage `json:"IdentityProviderDetails"`
	IdentityProviderType          *string         `json:"IdentityProviderType"`
	IPAddressType                 *string         `json:"IpAddressType"`
	LoggingRole                   *string         `json:"LoggingRole"`
	PostAuthenticationLoginBanner *string         `json:"PostAuthenticationLoginBanner"`
	PreAuthenticationLoginBanner  *string         `json:"PreAuthenticationLoginBanner"`
	ProtocolDetails               json.RawMessage `json:"ProtocolDetails"`
	Protocols                     *[]string       `json:"Protocols"`
	S3StorageOptions              json.RawMessage `json:"S3StorageOptions"`
	SecurityPolicyName            *string         `json:"SecurityPolicyName"`
	StructuredLogDestinations     *[]string       `json:"StructuredLogDestinations"`
	WorkflowDetails               json.RawMessage `json:"WorkflowDetails"`
}

// validate checks each member the request sent against its own published constraints.
func (s *transferServerSettings) validate() error {
	str := func(v *string) string {
		if v == nil {
			return ""
		}
		return *v
	}
	if err := transferCheckEnum("EndpointType", str(s.EndpointType), transferEndpointTypes); err != nil {
		return err
	}
	if err := transferCheckEnum("IdentityProviderType", str(s.IdentityProviderType), transferIdentityProviderTypes); err != nil {
		return err
	}
	if err := transferCheckEnum("IpAddressType", str(s.IPAddressType), transferIPAddressTypes); err != nil {
		return err
	}
	if err := transferCheckString("Certificate", str(s.Certificate), 1600, nil); err != nil {
		return err
	}
	if err := transferCheckString("HostKey", str(s.HostKey), 4096, nil); err != nil {
		return err
	}
	if err := transferCheckString("LoggingRole", str(s.LoggingRole), 2048, transferRolePattern); err != nil {
		return err
	}
	if err := transferCheckString("SecurityPolicyName", str(s.SecurityPolicyName), 100, transferSecurityPolicyPattern); err != nil {
		return err
	}
	if err := transferCheckString("PreAuthenticationLoginBanner", str(s.PreAuthenticationLoginBanner), 4096, transferBannerPattern); err != nil {
		return err
	}
	if err := transferCheckString("PostAuthenticationLoginBanner", str(s.PostAuthenticationLoginBanner), 4096, transferBannerPattern); err != nil {
		return err
	}
	if s.Protocols != nil {
		if err := transferCheckProtocols(*s.Protocols); err != nil {
			return err
		}
	}
	if s.StructuredLogDestinations != nil {
		if err := transferCheckLogDestinations(*s.StructuredLogDestinations); err != nil {
			return err
		}
	}
	for _, obj := range []struct {
		member string
		raw    json.RawMessage
	}{
		{"EndpointDetails", s.EndpointDetails},
		{"IdentityProviderDetails", s.IdentityProviderDetails},
		{"ProtocolDetails", s.ProtocolDetails},
		{"S3StorageOptions", s.S3StorageOptions},
		{"WorkflowDetails", s.WorkflowDetails},
	} {
		if _, err := transferRawJSONKind(obj.member, obj.raw, false); err != nil {
			return err
		}
	}
	return nil
}

// applyTo writes every member the request sent onto server. HostKey is validated and not stored:
// it is a private key, and its only published echo, HostKeyFingerprint, is not modeled.
func (s *transferServerSettings) applyTo(server *TransferServer) {
	setStr := func(dst *string, v *string) {
		if v != nil {
			*dst = *v
		}
	}
	setRaw := func(dst *json.RawMessage, member string, v json.RawMessage) {
		if present, _ := transferRawJSONKind(member, v, false); present {
			*dst = v
		}
	}
	setStr(&server.Certificate, s.Certificate)
	setStr(&server.EndpointType, s.EndpointType)
	setStr(&server.IdentityProviderType, s.IdentityProviderType)
	setStr(&server.IPAddressType, s.IPAddressType)
	setStr(&server.LoggingRole, s.LoggingRole)
	setStr(&server.PostAuthenticationLoginBanner, s.PostAuthenticationLoginBanner)
	setStr(&server.PreAuthenticationLoginBanner, s.PreAuthenticationLoginBanner)
	setStr(&server.SecurityPolicyName, s.SecurityPolicyName)
	if s.Protocols != nil {
		server.Protocols = *s.Protocols
	}
	if s.StructuredLogDestinations != nil {
		// An empty array clears the destination, as the page documents.
		server.StructuredLogDestinations = *s.StructuredLogDestinations
	}
	setRaw(&server.EndpointDetails, "EndpointDetails", s.EndpointDetails)
	setRaw(&server.IdentityProviderDetails, "IdentityProviderDetails", s.IdentityProviderDetails)
	setRaw(&server.ProtocolDetails, "ProtocolDetails", s.ProtocolDetails)
	setRaw(&server.S3StorageOptions, "S3StorageOptions", s.S3StorageOptions)
	setRaw(&server.WorkflowDetails, "WorkflowDetails", s.WorkflowDetails)
}

// createServer handles CreateServer.
//
// Three defaults are the ones API_CreateServer states in so many words: Domain "The default value
// is S3", IdentityProviderType "The default value is SERVICE_MANAGED", IPAddressType "The default
// value is IPV4". Domain used to default to SFTP, which is a Protocols value, not a Domain one, and
// was stored, so every later read reported a storage domain of SFTP (#1198).
//
// EndpointType defaults to PUBLIC, and that is a different kind of decision: the page states no
// default for it. PUBLIC is the first of its three Valid Values and is what a server created with no
// endpoint configuration reports, so it is kept rather than "fixed" alongside Domain. Protocols and
// SecurityPolicyName have no stated default either and are absent unless sent.
func (p *TransferPlugin) createServer(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		transferServerSettings
		Domain string        `json:"Domain"`
		Tags   []TransferTag `json:"Tags"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, transferInvalidBody()
		}
	}
	if err := transferCheckEnum("Domain", input.Domain, transferDomains); err != nil {
		return nil, err
	}
	if err := input.validate(); err != nil {
		return nil, err
	}
	if input.Domain == "" {
		input.Domain = "S3"
	}

	serverID := generateTransferServerID(reqCtx.IDs)
	server := TransferServer{
		ServerID:             serverID,
		Arn:                  fmt.Sprintf("arn:aws:transfer:%s:%s:server/%s", reqCtx.Region, reqCtx.AccountID, serverID),
		Domain:               input.Domain,
		EndpointType:         "PUBLIC",
		IdentityProviderType: "SERVICE_MANAGED",
		IPAddressType:        "IPV4",
		State:                "ONLINE",
		Tags:                 input.Tags,
		CreatedAt:            p.tc.Now(),
		AccountID:            reqCtx.AccountID,
		Region:               reqCtx.Region,
	}
	input.applyTo(&server)

	goCtx := context.Background()
	data, err := json.Marshal(server)
	if err != nil {
		return nil, fmt.Errorf("transfer createServer marshal: %w", err)
	}
	key := transferServerKey(reqCtx.AccountID, reqCtx.Region, serverID)
	if err := p.state.Put(goCtx, transferNamespace, key, data); err != nil {
		return nil, fmt.Errorf("transfer createServer put: %w", err)
	}
	updateStringIndex(goCtx, p.state, transferNamespace, transferServerIDsKey(reqCtx.AccountID, reqCtx.Region), serverID)

	return transferJSONResponse(http.StatusOK, map[string]interface{}{
		"ServerId": serverID,
	})
}

func (p *TransferPlugin) describeServer(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServerID string `json:"ServerId"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, transferInvalidBody()
		}
	}
	server, err := p.loadServer(reqCtx.AccountID, reqCtx.Region, input.ServerID)
	if err != nil {
		return nil, err
	}
	count, err := p.userCount(reqCtx.AccountID, reqCtx.Region, server.ServerID)
	if err != nil {
		return nil, err
	}
	return transferJSONResponse(http.StatusOK, map[string]interface{}{
		"Server": transferServerToWire(*server, count),
	})
}

// updateServer handles UpdateServer.
//
// It reads every member API_UpdateServer publishes, and a member the request sends replaces the
// server's value while one it omits is left alone (#1199). It used to read EndpointType and Tags
// only, so a caller adding FTPS by setting Protocols and Certificate got a 200 and an unchanged
// server. Tags is not an UpdateServer member — tagging a server is TagResource, which is not routed
// — so it is no longer read. Domain is not one either: the page says the domain cannot be changed
// after the server is created.
func (p *TransferPlugin) updateServer(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		transferServerSettings
		ServerID string `json:"ServerId"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, transferInvalidBody()
		}
	}
	if err := input.validate(); err != nil {
		return nil, err
	}
	server, err := p.loadServer(reqCtx.AccountID, reqCtx.Region, input.ServerID)
	if err != nil {
		return nil, err
	}
	input.applyTo(server)

	goCtx := context.Background()
	data, err := json.Marshal(server)
	if err != nil {
		return nil, fmt.Errorf("transfer updateServer marshal: %w", err)
	}
	key := transferServerKey(reqCtx.AccountID, reqCtx.Region, server.ServerID)
	if err := p.state.Put(goCtx, transferNamespace, key, data); err != nil {
		return nil, fmt.Errorf("transfer updateServer put: %w", err)
	}

	return transferJSONResponse(http.StatusOK, map[string]interface{}{
		"ServerId": server.ServerID,
	})
}

// deleteServer handles DeleteServer, cascading to the server's users.
//
// # The response body
//
// It answers `{}`. API_DeleteServer and API_DeleteUser are self-inconsistent: the Response Syntax
// is a bare `HTTP/1.1 200` and the prose says "an HTTP 200 response with an empty HTTP body", while
// each page's Example shows `{ }` as the response body. `{}` is chosen, for both deletes, because it
// is what the Example shows, because an awsJson1_1 client decodes an empty object and an empty body
// alike for an operation with no response members, and because it is what substrate has always
// answered, so a recorded run replays byte-identically (#1206).
func (p *TransferPlugin) deleteServer(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServerID string `json:"ServerId"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, transferInvalidBody()
		}
	}
	if _, err := p.loadServer(reqCtx.AccountID, reqCtx.Region, input.ServerID); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	namesKey := transferUserNamesKey(reqCtx.AccountID, reqCtx.Region, input.ServerID)
	userNames, err := loadStringIndex(goCtx, p.state, transferNamespace, namesKey)
	if err != nil {
		return nil, fmt.Errorf("transfer deleteServer load user index: %w", err)
	}
	for _, userName := range userNames {
		userKey := transferUserKey(reqCtx.AccountID, reqCtx.Region, input.ServerID, userName)
		if err := p.state.Delete(goCtx, transferNamespace, userKey); err != nil {
			return nil, fmt.Errorf("transfer deleteServer delete user %s: %w", userName, err)
		}
	}
	if err := p.state.Delete(goCtx, transferNamespace, namesKey); err != nil {
		return nil, fmt.Errorf("transfer deleteServer delete user index: %w", err)
	}

	key := transferServerKey(reqCtx.AccountID, reqCtx.Region, input.ServerID)
	if err := p.state.Delete(goCtx, transferNamespace, key); err != nil {
		return nil, fmt.Errorf("transfer deleteServer delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, transferNamespace, transferServerIDsKey(reqCtx.AccountID, reqCtx.Region), input.ServerID)

	return transferJSONResponse(http.StatusOK, map[string]interface{}{})
}

// transferPageDefault is the page size a list uses when the request sends no MaxResults. Neither
// list page states a default, so the published maximum is used: a caller that sends no limit gets
// every record it can, and one that sends a limit gets exactly that.
const transferPageDefault = 1000

// transferPage decodes a Transfer collection's MaxResults and NextToken, refusing each as its page
// publishes (#1195).
//
// MaxResults outside the published Valid Range 1–1000 is InvalidRequestException, since neither list
// page publishes a code specific to the range. A NextToken substrate did not issue is
// InvalidNextTokenException at 400, which both pages publish with the sentence used here. An empty
// NextToken is read as absent: the published Length Constraints make "" an illegal value, but
// refusing it would break a caller still echoing the "" substrate used to send.
func transferPage(maxResults *int, nextToken string) (offset, pageSize int, err error) {
	pageSize = transferPageDefault
	if maxResults != nil {
		if *maxResults < 1 || *maxResults > 1000 {
			return 0, 0, transferInvalidRequest("MaxResults must be between 1 and 1000")
		}
		pageSize = *maxResults
	}
	offset, ok := decodeOffsetPaginationToken(nextToken)
	if !ok {
		return 0, 0, &AWSError{
			Code:       "InvalidNextTokenException",
			Message:    "The NextToken parameter that was passed is invalid.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return offset, pageSize, nil
}

// transferListBody returns body with NextToken set only when a further page exists.
//
// It used to be "" on every response. The published Length Constraints are 1–6144, so the empty
// string is not a terminator but an illegal value, and a paginator written `while "NextToken" in
// response` never stopped (#1195).
func transferListBody(body map[string]interface{}, token string) map[string]interface{} {
	if token != "" {
		body["NextToken"] = token
	}
	return body
}

// listServers handles ListServers. Servers are listed in server-ID order: the account's server index
// is kept sorted (updateStringIndex), so the order is stable across pages.
func (p *TransferPlugin) listServers(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		MaxResults *int   `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, transferInvalidBody()
		}
	}
	offset, pageSize, err := transferPage(input.MaxResults, input.NextToken)
	if err != nil {
		return nil, err
	}

	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, transferNamespace, transferServerIDsKey(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return nil, fmt.Errorf("transfer listServers load index: %w", err)
	}
	page, token := pageByOffsetToken(ids, offset, pageSize)
	summaries := make([]transferListedServerOut, 0, len(page))
	for _, id := range page {
		server, err := p.loadServer(reqCtx.AccountID, reqCtx.Region, id)
		if transferIsNotFound(err) {
			continue // an index entry whose record is gone is not listed
		}
		if err != nil {
			return nil, err
		}
		count, err := p.userCount(reqCtx.AccountID, reqCtx.Region, id)
		if err != nil {
			return nil, err
		}
		summaries = append(summaries, transferServerToListed(*server, count))
	}
	return transferJSONResponse(http.StatusOK, transferListBody(map[string]interface{}{
		"Servers": summaries,
	}, token))
}

// createUser handles CreateUser.
//
// API_CreateUser marks three members Required: Yes — Role, ServerId and UserName — and all three are
// checked before the server is looked up, so a request that omits one is refused as malformed
// whether or not its server exists. Role used to be unchecked, so a user with no access role was
// created and reported successful (#1197). Role, UserName and every optional member the request
// sends are held to their published Length Constraints, Pattern and Valid Values.
//
// A duplicate user name is ResourceExistsException at 400, the only already-exists code the page
// publishes. It was ConflictException at 409, which is published on UpdateServer for a concurrent
// update, not on this operation (#1198).
func (p *TransferPlugin) createUser(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServerID              string          `json:"ServerId"`
		UserName              string          `json:"UserName"`
		HomeDirectory         string          `json:"HomeDirectory"`
		HomeDirectoryType     string          `json:"HomeDirectoryType"`
		HomeDirectoryMappings json.RawMessage `json:"HomeDirectoryMappings"`
		Policy                string          `json:"Policy"`
		PosixProfile          json.RawMessage `json:"PosixProfile"`
		Role                  string          `json:"Role"`
		SSHPublicKeyBody      string          `json:"SshPublicKeyBody"`
		Tags                  []TransferTag   `json:"Tags"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, transferInvalidBody()
		}
	}
	if input.ServerID == "" || input.UserName == "" {
		return nil, transferInvalidRequest("ServerId and UserName are required")
	}
	if input.Role == "" {
		return nil, transferInvalidRequest("Role is required")
	}
	if !transferUserNamePattern.MatchString(input.UserName) {
		return nil, transferInvalidRequest("UserName %q does not match the pattern %s", input.UserName, transferUserNamePattern.String())
	}
	if len(input.Role) < 20 {
		return nil, transferInvalidRequest("Role is shorter than 20 characters")
	}
	if err := transferCheckString("Role", input.Role, 2048, transferRolePattern); err != nil {
		return nil, err
	}
	if err := transferCheckString("HomeDirectory", input.HomeDirectory, 1024, transferHomeDirectoryPattern); err != nil {
		return nil, err
	}
	if err := transferCheckEnum("HomeDirectoryType", input.HomeDirectoryType, transferHomeDirectoryTypes); err != nil {
		return nil, err
	}
	if err := transferCheckString("Policy", input.Policy, 2048, nil); err != nil {
		return nil, err
	}
	if err := transferCheckString("SshPublicKeyBody", input.SSHPublicKeyBody, 2048, transferSSHPublicKeyPattern); err != nil {
		return nil, err
	}
	hasMappings, err := transferRawJSONKind("HomeDirectoryMappings", input.HomeDirectoryMappings, true)
	if err != nil {
		return nil, err
	}
	hasPosix, err := transferRawJSONKind("PosixProfile", input.PosixProfile, false)
	if err != nil {
		return nil, err
	}

	if _, err := p.loadServer(reqCtx.AccountID, reqCtx.Region, input.ServerID); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	userKey := transferUserKey(reqCtx.AccountID, reqCtx.Region, input.ServerID, input.UserName)
	existing, err := p.state.Get(goCtx, transferNamespace, userKey)
	if err != nil {
		return nil, fmt.Errorf("transfer createUser get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{
			Code:       "ResourceExistsException",
			Message:    "User " + input.UserName + " already exists on server " + input.ServerID + ".",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	user := TransferUser{
		UserName:          input.UserName,
		Arn:               fmt.Sprintf("arn:aws:transfer:%s:%s:user/%s/%s", reqCtx.Region, reqCtx.AccountID, input.ServerID, input.UserName),
		ServerID:          input.ServerID,
		HomeDirectory:     input.HomeDirectory,
		HomeDirectoryType: input.HomeDirectoryType,
		Policy:            input.Policy,
		Role:              input.Role,
		Tags:              input.Tags,
		AccountID:         reqCtx.AccountID,
		Region:            reqCtx.Region,
	}
	if hasMappings {
		user.HomeDirectoryMappings = input.HomeDirectoryMappings
	}
	if hasPosix {
		user.PosixProfile = input.PosixProfile
	}
	if input.SSHPublicKeyBody != "" {
		user.SSHPublicKeys = []TransferSSHPublicKey{{
			SSHPublicKeyBody: input.SSHPublicKeyBody,
			SSHPublicKeyID:   generateTransferSSHKeyID(reqCtx.IDs),
			DateImported:     p.tc.Now(),
		}}
	}

	data, err := json.Marshal(user)
	if err != nil {
		return nil, fmt.Errorf("transfer createUser marshal: %w", err)
	}
	if err := p.state.Put(goCtx, transferNamespace, userKey, data); err != nil {
		return nil, fmt.Errorf("transfer createUser put: %w", err)
	}
	updateStringIndex(goCtx, p.state, transferNamespace, transferUserNamesKey(reqCtx.AccountID, reqCtx.Region, input.ServerID), input.UserName)

	return transferJSONResponse(http.StatusOK, map[string]interface{}{
		"ServerId": input.ServerID,
		"UserName": input.UserName,
	})
}

func (p *TransferPlugin) describeUser(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServerID string `json:"ServerId"`
		UserName string `json:"UserName"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, transferInvalidBody()
		}
	}
	user, err := p.loadUser(reqCtx.AccountID, reqCtx.Region, input.ServerID, input.UserName)
	if err != nil {
		return nil, err
	}
	return transferJSONResponse(http.StatusOK, map[string]interface{}{
		"ServerId": input.ServerID,
		"User":     transferUserToWire(*user),
	})
}

// updateUser handles UpdateUser. It reads HomeDirectory and Role only; the other members
// API_UpdateUser publishes are outside #1199's table and are not yet applied.
func (p *TransferPlugin) updateUser(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServerID      string `json:"ServerId"`
		UserName      string `json:"UserName"`
		HomeDirectory string `json:"HomeDirectory"`
		Role          string `json:"Role"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, transferInvalidBody()
		}
	}
	user, err := p.loadUser(reqCtx.AccountID, reqCtx.Region, input.ServerID, input.UserName)
	if err != nil {
		return nil, err
	}

	if input.HomeDirectory != "" {
		user.HomeDirectory = input.HomeDirectory
	}
	if input.Role != "" {
		user.Role = input.Role
	}

	goCtx := context.Background()
	data, err := json.Marshal(user)
	if err != nil {
		return nil, fmt.Errorf("transfer updateUser marshal: %w", err)
	}
	key := transferUserKey(reqCtx.AccountID, reqCtx.Region, input.ServerID, input.UserName)
	if err := p.state.Put(goCtx, transferNamespace, key, data); err != nil {
		return nil, fmt.Errorf("transfer updateUser put: %w", err)
	}

	return transferJSONResponse(http.StatusOK, map[string]interface{}{
		"ServerId": input.ServerID,
		"UserName": input.UserName,
	})
}

// deleteUser handles DeleteUser. It answers `{}`, for the reason [TransferPlugin.deleteServer]
// records (#1206).
func (p *TransferPlugin) deleteUser(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServerID string `json:"ServerId"`
		UserName string `json:"UserName"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, transferInvalidBody()
		}
	}
	if _, err := p.loadUser(reqCtx.AccountID, reqCtx.Region, input.ServerID, input.UserName); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	key := transferUserKey(reqCtx.AccountID, reqCtx.Region, input.ServerID, input.UserName)
	if err := p.state.Delete(goCtx, transferNamespace, key); err != nil {
		return nil, fmt.Errorf("transfer deleteUser delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, transferNamespace, transferUserNamesKey(reqCtx.AccountID, reqCtx.Region, input.ServerID), input.UserName)

	return transferJSONResponse(http.StatusOK, map[string]interface{}{})
}

// listUsers handles ListUsers. Users are listed in user-name order, the sorted order of the server's user
// index. A server that does not exist is ResourceNotFoundException, which the page publishes; it
// used to list as empty.
func (p *TransferPlugin) listUsers(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		MaxResults *int   `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
		ServerID   string `json:"ServerId"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, transferInvalidBody()
		}
	}
	if input.ServerID == "" {
		return nil, transferInvalidRequest("ServerId is required")
	}
	offset, pageSize, err := transferPage(input.MaxResults, input.NextToken)
	if err != nil {
		return nil, err
	}
	if _, err := p.loadServer(reqCtx.AccountID, reqCtx.Region, input.ServerID); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, transferNamespace, transferUserNamesKey(reqCtx.AccountID, reqCtx.Region, input.ServerID))
	if err != nil {
		return nil, fmt.Errorf("transfer listUsers load index: %w", err)
	}
	page, token := pageByOffsetToken(names, offset, pageSize)
	summaries := make([]transferListedUserOut, 0, len(page))
	for _, name := range page {
		user, err := p.loadUser(reqCtx.AccountID, reqCtx.Region, input.ServerID, name)
		if transferIsNotFound(err) {
			continue // an index entry whose record is gone is not listed
		}
		if err != nil {
			return nil, err
		}
		summaries = append(summaries, transferUserToListed(*user))
	}
	return transferJSONResponse(http.StatusOK, transferListBody(map[string]interface{}{
		"ServerId": input.ServerID,
		"Users":    summaries,
	}, token))
}

// userCount reports how many users the server has, from its user index.
func (p *TransferPlugin) userCount(acct, region, serverID string) (int, error) {
	names, err := loadStringIndex(context.Background(), p.state, transferNamespace, transferUserNamesKey(acct, region, serverID))
	if err != nil {
		return 0, fmt.Errorf("transfer user count: %w", err)
	}
	return len(names), nil
}

// loadServer loads a TransferServer from state or returns a not-found error.
//
// A ServerId that does not match the published s-([0-9a-f]{17}) pattern is not refused as
// malformed: it names no server, so it answers ResourceNotFoundException like any other absent ID,
// which keeps a caller's existence probe on one code.
func (p *TransferPlugin) loadServer(acct, region, serverID string) (*TransferServer, error) {
	if serverID == "" {
		return nil, transferInvalidRequest("ServerId is required")
	}
	goCtx := context.Background()
	key := transferServerKey(acct, region, serverID)
	data, err := p.state.Get(goCtx, transferNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("transfer loadServer get: %w", err)
	}
	if data == nil {
		return nil, transferNotFound("Server " + serverID + " does not exist.")
	}
	var server TransferServer
	if err := json.Unmarshal(data, &server); err != nil {
		return nil, fmt.Errorf("transfer loadServer unmarshal: %w", err)
	}
	return &server, nil
}

// loadUser loads a TransferUser from state or returns a not-found error.
func (p *TransferPlugin) loadUser(acct, region, serverID, userName string) (*TransferUser, error) {
	if serverID == "" || userName == "" {
		return nil, transferInvalidRequest("ServerId and UserName are required")
	}
	goCtx := context.Background()
	key := transferUserKey(acct, region, serverID, userName)
	data, err := p.state.Get(goCtx, transferNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("transfer loadUser get: %w", err)
	}
	if data == nil {
		return nil, transferNotFound("User " + userName + " does not exist.")
	}
	var user TransferUser
	if err := json.Unmarshal(data, &user); err != nil {
		return nil, fmt.Errorf("transfer loadUser unmarshal: %w", err)
	}
	return &user, nil
}

// transferJSONResponse serializes v to JSON and returns an AWSResponse with
// Content-Type application/json.
func transferJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("transfer json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}
