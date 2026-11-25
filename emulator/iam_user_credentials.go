package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"time"
)

// IAM login profiles: a user's console password, observable through GetLoginProfile as the
// profile's CreateDate and PasswordResetRequired. The password itself is never stored —
// substrate models no console sign-in, and nothing reads a password back — so a profile record
// holds only what the API reports.

// IAMLoginProfile is a stored IAM user login profile.
type IAMLoginProfile struct {
	// UserName is the user the profile belongs to.
	UserName string `json:"UserName"`

	// CreateDate is when the password was first created.
	CreateDate time.Time `json:"CreateDate"`

	// PasswordResetRequired reports whether the user must set a new password at next sign-in.
	PasswordResetRequired bool `json:"PasswordResetRequired"`
}

// iamLoginProfileKey returns the state key for a user's login profile.
func iamLoginProfileKey(accountID, userName string) string {
	return iamStateKey("login_profile", accountID, userName)
}

// iamLoginProfileNotFound is GetLoginProfile's NoSuchEntity, in the wording AWS returns (observed
// through the AWS CLI and Terraform; the API reference publishes none).
func iamLoginProfileNotFound(userName string) *AWSResponse {
	return iamErrorResponse("NoSuchEntity",
		fmt.Sprintf("Login Profile for User %s cannot be found.", userName), http.StatusNotFound)
}

// loadLoginProfile returns the user's login profile, or nil when there is none.
func (p *IAMPlugin) loadLoginProfile(goCtx context.Context, accountID, userName string) (*IAMLoginProfile, error) {
	raw, err := p.state.Get(goCtx, iamNamespace, iamLoginProfileKey(accountID, userName))
	if err != nil {
		return nil, fmt.Errorf("get login profile: %w", err)
	}
	if raw == nil {
		return nil, nil //nolint:nilnil // absent is the caller's NoSuchEntity.
	}
	var lp IAMLoginProfile
	if err := json.Unmarshal(raw, &lp); err != nil {
		return nil, fmt.Errorf("unmarshal login profile: %w", err)
	}
	return &lp, nil
}

// putLoginProfile stores lp under its user.
func (p *IAMPlugin) putLoginProfile(goCtx context.Context, accountID string, lp *IAMLoginProfile) error {
	raw, err := json.Marshal(lp)
	if err != nil {
		return fmt.Errorf("marshal login profile: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, iamLoginProfileKey(accountID, lp.UserName), raw); err != nil {
		return fmt.Errorf("put login profile: %w", err)
	}
	return nil
}

// iamLoginProfileXML renders the LoginProfile structure.
func iamLoginProfileXML(lp *IAMLoginProfile) string {
	return "<LoginProfile><UserName>" + xmlEsc(lp.UserName) + "</UserName><CreateDate>" +
		lp.CreateDate.UTC().Format("2006-01-02T15:04:05Z") + "</CreateDate><PasswordResetRequired>" +
		iamBoolXML(lp.PasswordResetRequired) + "</PasswordResetRequired></LoginProfile>"
}

// loginProfileUser resolves the user a login-profile operation names — UserName, or the calling
// user where the page says it "defaults to the principal making the request" — and loads it.
func (p *IAMPlugin) loginProfileUser(goCtx context.Context, ctx *RequestContext, name string,
	callerDefault bool) (string, *AWSResponse, error) {
	if name == "" && callerDefault {
		name = iamCallerUserName(ctx)
	}
	if name == "" {
		return "", iamErrorResponse("ValidationError", "UserName is required", http.StatusBadRequest), nil
	}
	user, err := p.loadUser(goCtx, ctx.AccountID, name)
	if err != nil {
		return "", nil, err
	}
	if user == nil {
		return "", iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The user with name %s cannot be found.", name), http.StatusNotFound), nil
	}
	return name, nil, nil
}

func (p *IAMPlugin) createLoginProfile(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		UserName              string  `json:"UserName"`
		Password              string  `json:"Password"`
		PasswordResetRequired iamBool `json:"PasswordResetRequired"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	// The page makes Password optional only for an AssumeRoot session, which substrate does not
	// model, so it is required here. The message is substrate's own.
	if params.Password == "" {
		return iamErrorResponse("ValidationError", "Password is required", http.StatusBadRequest), nil
	}
	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:CreateLoginProfile", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	name, refusal, err := p.loginProfileUser(goCtx, ctx, params.UserName, true)
	if err != nil || refusal != nil {
		return refusal, err
	}
	existing, err := p.loadLoginProfile(goCtx, ctx.AccountID, name)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		// The message is substrate's own: no AWS source publishes this refusal's wording.
		return iamErrorResponse("EntityAlreadyExists",
			fmt.Sprintf("Login Profile for user %s already exists.", name), http.StatusConflict), nil
	}
	lp := &IAMLoginProfile{UserName: name, CreateDate: p.now().UTC(), PasswordResetRequired: params.PasswordResetRequired.Bool()}
	if err := p.putLoginProfile(goCtx, ctx.AccountID, lp); err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, "CreateLoginProfile", iamLoginProfileXML(lp))
}

func (p *IAMPlugin) getLoginProfile(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		UserName string `json:"UserName"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:GetLoginProfile", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	name := params.UserName
	if name == "" {
		name = iamCallerUserName(ctx)
	}
	if name == "" {
		return iamErrorResponse("ValidationError", "UserName is required", http.StatusBadRequest), nil
	}
	// "If the user does not exist or does not have a password, the operation returns a 404
	// (NoSuchEntity) error."
	lp, err := p.loadLoginProfile(goCtx, ctx.AccountID, name)
	if err != nil {
		return nil, err
	}
	if lp == nil {
		return iamLoginProfileNotFound(name), nil
	}
	return iamXMLResponse(http.StatusOK, "GetLoginProfile", iamLoginProfileXML(lp))
}

// updateLoginProfile changes PasswordResetRequired when the request carries it. A new Password is
// accepted and, like every password, not stored.
func (p *IAMPlugin) updateLoginProfile(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		UserName              string   `json:"UserName"`
		Password              string   `json:"Password"`
		PasswordResetRequired *iamBool `json:"PasswordResetRequired"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.UserName == "" {
		return iamErrorResponse("ValidationError", "UserName is required", http.StatusBadRequest), nil
	}
	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:UpdateLoginProfile", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	lp, err := p.loadLoginProfile(goCtx, ctx.AccountID, params.UserName)
	if err != nil {
		return nil, err
	}
	if lp == nil {
		return iamLoginProfileNotFound(params.UserName), nil
	}
	if params.PasswordResetRequired != nil {
		lp.PasswordResetRequired = params.PasswordResetRequired.Bool()
		if err := p.putLoginProfile(goCtx, ctx.AccountID, lp); err != nil {
			return nil, err
		}
	}
	return iamXMLEmptyResponse("UpdateLoginProfile"), nil
}

func (p *IAMPlugin) deleteLoginProfile(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		UserName string `json:"UserName"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:DeleteLoginProfile", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	name := params.UserName
	if name == "" {
		name = iamCallerUserName(ctx)
	}
	if name == "" {
		return iamErrorResponse("ValidationError", "UserName is required", http.StatusBadRequest), nil
	}
	lp, err := p.loadLoginProfile(goCtx, ctx.AccountID, name)
	if err != nil {
		return nil, err
	}
	if lp == nil {
		return iamLoginProfileNotFound(name), nil
	}
	if err := p.state.Delete(goCtx, iamNamespace, iamLoginProfileKey(ctx.AccountID, name)); err != nil {
		return nil, fmt.Errorf("delete login profile: %w", err)
	}
	return iamXMLEmptyResponse("DeleteLoginProfile"), nil
}

// --- The credential listings force_destroy reads ---------------------------------

// listUserCredentials answers the four per-user credential listings Terraform's
// `aws_iam_user` force_destroy reads before DeleteUser: MFA devices, SSH public keys, signing
// certificates and service-specific credentials. Substrate models none of the four, so nothing can
// create one and each listing is truthfully empty — for a user that exists, since each page
// declares NoSuchEntity. UserName defaults to the caller, as each page documents.
func (p *IAMPlugin) listUserCredentials(ctx *RequestContext, req *AWSRequest, wrapper string) (*AWSResponse, error) {
	var params struct {
		UserName string `json:"UserName"`
		Marker   string `json:"Marker"`
		MaxItems iamInt `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if _, errResp := iamMarker(req, params.MaxItems, params.Marker); errResp != nil {
		return errResp, nil
	}
	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:"+req.Operation, p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	name := params.UserName
	if name == "" {
		name = iamCallerUserName(ctx)
	}
	if name != "" {
		user, err := p.loadUser(goCtx, ctx.AccountID, name)
		if err != nil {
			return nil, err
		}
		if user == nil {
			return iamErrorResponse("NoSuchEntity",
				fmt.Sprintf("The user with name %s cannot be found.", name), http.StatusNotFound), nil
		}
	}
	return iamXMLResponse(http.StatusOK, req.Operation, "<"+wrapper+"></"+wrapper+"><IsTruncated>false</IsTruncated>")
}

// iamVirtualMFAAssignmentStatuses are ListVirtualMFADevices' AssignmentStatus values.
var iamVirtualMFAAssignmentStatuses = []string{"Assigned", "Unassigned", "Any"}

// listVirtualMFADevices answers the account's virtual MFA devices, of which substrate models none.
func (p *IAMPlugin) listVirtualMFADevices(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		AssignmentStatus string `json:"AssignmentStatus"`
		Marker           string `json:"Marker"`
		MaxItems         iamInt `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.AssignmentStatus != "" && !slices.Contains(iamVirtualMFAAssignmentStatuses, params.AssignmentStatus) {
		return iamErrorResponse("ValidationError",
			iamEnumMessage("assignmentStatus", params.AssignmentStatus, iamVirtualMFAAssignmentStatuses),
			http.StatusBadRequest), nil
	}
	if _, errResp := iamMarker(req, params.MaxItems, params.Marker); errResp != nil {
		return errResp, nil
	}
	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:ListVirtualMFADevices", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	return iamXMLResponse(http.StatusOK, "ListVirtualMFADevices",
		"<VirtualMFADevices></VirtualMFADevices><IsTruncated>false</IsTruncated>")
}
