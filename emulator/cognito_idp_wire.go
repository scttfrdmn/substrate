package emulator

import "time"

// Cognito answered from its records rather than from a projection when #756 reached it: four of the
// five records were embedded straight into a response struct — a UserPool member typed
// CognitoUserPool, and its three siblings — so every persisted member marshaled into the body.
// The types below are the published shapes those eleven sites now answer from, on the pattern
// emulator/ecr_wire.go established (#1090) and emulator/efs_wire.go (#1304),
// emulator/ecs_wire.go (#1309), emulator/glue_wire.go (#1310), emulator/rds_wire.go (#1311),
// emulator/elasticache_wire.go (#1312) and emulator/elb_wire.go (#1314) followed.
//
// # The eleven sites, and the four that were already right
//
//	record                   sites answering it
//	CognitoUserPool          CreateUserPool, DescribeUserPool
//	CognitoUserPoolClient    CreateUserPoolClient, DescribeUserPoolClient, UpdateUserPoolClient
//	CognitoGroup             CreateGroup, GetGroup, ListGroups, AdminListGroupsForUser
//	CognitoUser              AdminCreateUser, ListUsers
//
// CognitoIdentityPool is the fifth record and needs no struct here: all four of its sites
// (cognito_identity_plugin.go's CreateIdentityPool, DescribeIdentityPool, ListIdentityPools and
// GetIdentityPoolRoles) already build a local response struct, as do ListUserPools,
// ListUserPoolClients, AdminGetUser and SignUp on this side. Those were ECS's position at #1308 —
// reachable by declaration with nothing pinning them shut — and the TestCognitoWire_* tests in
// emulator/cognito_wire_test.go are the pin for them.
//
// # Two members were reported under a name AWS does not publish
//
//	record member                  was tagged     published as    authority
//	CognitoUserPool.UserPoolID     UserPoolId     Id              API_UserPoolType
//	CognitoUserPool.Tags           Tags           UserPoolTags    API_UserPoolType
//
// The first is the one that bites, and it is a reporting defect rather than a leak:
// API_UserPoolType publishes no UserPoolId at all, so an SDK decoding DescribeUserPool reads Id,
// finds nothing, and hands its caller an empty pool ID from a call that did report one. Substrate
// also already contradicted itself — ListUserPools' own summary tags it `json:"Id"`, correctly, per
// API_UserPoolDescriptionType — so the same identifier came back under two different member names
// depending on which door the caller knocked on, and only one of them was published.
//
// # Three more unpublished members reached a body
//
// These are outside scripts/check-wire-bookkeeping.sh's count, which matches only fields named
// AccountID, Region, CreatedAt, UpdatedAt and EverTagged, but they are squarely #756's AC3:
//
//   - CognitoUserPool.ProviderName. Checked against all thirty-six members of API_UserPoolType: the
//     IdP endpoint is not among them. It is derivable from the pool ID and the Region — which is how
//     createUserPool mints it and how deployCognitoUserPool now recovers it for
//     AWS::Cognito::UserPool's ProviderName and ProviderURL attributes.
//   - CognitoUser.UserPoolID. API_UserType publishes seven members and UserPoolId is not one; it is
//     how a user is keyed, not something the shape reports.
//   - CognitoUser.Groups. Likewise unpublished — AdminListGroupsForUser is the operation that
//     reports a user's groups. ELBRule.ListenerARN was the same case (#1314).
//
// # The leaked account and Region have no published home
//
// Unlike EFS, where the leaked AccountID became the published OwnerId (#1304), and Glue, where it
// became CatalogId (#1310), no Cognito shape publishes an account or a Region member. Checked
// member-by-member against API_UserPoolType, API_UserPoolClientType, API_GroupType, API_UserType and
// API_IdentityPool. UserPoolType publishes the Arn instead, from which both are recoverable, which is
// what the real API requires of a caller.
//
// # Every Cognito timestamp was the wrong type
//
// Each of these members carries the same sentence: "Amazon Cognito returns this timestamp in UNIX
// epoch time format." Both services speak AWS JSON, where a Timestamp is a JSON number — and
// API_AdminGetUser's Response Syntax says `"UserCreateDate": number` outright, with a sample
// response of 1.682955829578E9. A Go time.Time marshals to an RFC3339 string, so the SDK's
// ParseEpochSeconds decoder was handed a string: #1305's class, a live decode failure rather than a
// cosmetic one.
//
// #1305 names ACM, Redshift Data and Firehose because it was derived from #756's baseline, which
// matches only fields named CreatedAt or UpdatedAt. Cognito's are CreationDate, LastModifiedDate,
// UserCreateDate, UserLastModifiedDate and Expiration, so that grep never saw them. All nine are
// fixed here, including at the four sites that were already projecting correctly — a service
// answering the same published member in two formats is worse than either end state.
//
// [EpochSeconds] is the existing fix, introduced for ecrRepositoryOut.CreatedAt (#1090) and named by
// #1305 as what right looks like. It goes in these structs rather than on the record, per the replay
// rule below, even though EpochSeconds.UnmarshalJSON would read the old RFC3339 state back.
//
// # What stays on the records
//
// AccountID, Region, Tags and EverTagged stay in cognito_idp_types.go, untouched, and the eleven
// baseline lines stay in scripts/wire-bookkeeping-baseline.txt. ecr_wire.go's rule forbids removing
// them: `json:"-"` and retyping a field in place both change the format of every recorded run, since
// MemoryStateManager snapshots those bytes and a replay reads them back. Tags and EverTagged are also
// read off the stored record by [TaggingPlugin.scanCognitoUserPools] to answer the Resource Groups
// Tagging API's GetResources, and ProviderName and Groups are still persisted and still answered —
// by DescribeUserPool's caller deriving it and by AdminListGroupsForUser respectively.
//
// # What substrate does not model
//
// Each type below lists the published members it omits. They are absent rather than present and
// zero, per #1013: a count, a threshold or an interval reported as its zero value reads as a
// measurement, and substrate has measured nothing. Two are worth naming because a consumer is likely
// to look for them. API_UserPoolClientType.LastModifiedDate and API_GroupType.LastModifiedDate are
// absent because no Cognito record but the user pool persists a modification time at all, so there
// is nothing to report, and persisting one is a record change rather than a projection one. And
// API_UserType.MFAOptions is absent because substrate records no MFA enrolment for a user.

// cognitoTimeOrNil converts a record's time.Time to the epoch-seconds form Cognito's JSON protocol
// publishes, returning nil for the zero time so an unset optional timestamp is omitted from the body
// rather than reported as JSON null or as 1970.
//
// The twin of [glueTimeOrNil], which is the twin of ecsTimeOrNil: each service's wire layer carries
// its own because ECS already stored the right type and the other two do not. Converting here rather
// than retyping the record is the whole point — see ecr_wire.go on why a persisted field's encoding
// cannot change.
func cognitoTimeOrNil(t time.Time) *EpochSeconds {
	if t.IsZero() {
		return nil
	}
	e := EpochSeconds(t)
	return &e
}

// cognitoUserPoolOut is the UserPool of CreateUserPool and DescribeUserPool.
//
// Member names follow API_UserPoolType, on which every member is Required: No. Status is here
// because the page marks it *deprecated*, which is not the same as unpublished; substrate reports
// every pool Enabled.
//
// The members substrate does not model are absent rather than present and zero:
// AccountRecoverySetting, AdminCreateUserConfig, AliasAttributes, AutoVerifiedAttributes,
// CustomDomain, DeletionProtection, DeviceConfiguration, Domain, EmailConfiguration,
// EmailConfigurationFailure, EmailVerificationMessage, EmailVerificationSubject,
// EstimatedNumberOfUsers, IssuerConfiguration, KeyConfiguration, SmsAuthenticationMessage,
// SmsConfiguration, SmsConfigurationFailure, SmsVerificationMessage, UserAttributeUpdateSettings,
// UsernameAttributes, UsernameConfiguration, UserPoolAddOns, UserPoolTier and
// VerificationMessageTemplate.
type cognitoUserPoolOut struct {
	ID               string            `json:"Id"`
	Name             string            `json:"Name"`
	Arn              string            `json:"Arn"`
	Status           string            `json:"Status"`
	Policies         any               `json:"Policies,omitempty"`
	LambdaConfig     any               `json:"LambdaConfig,omitempty"`
	SchemaAttributes []any             `json:"SchemaAttributes,omitempty"`
	MfaConfiguration string            `json:"MfaConfiguration,omitempty"`
	UserPoolTags     map[string]string `json:"UserPoolTags,omitempty"`
	CreationDate     *EpochSeconds     `json:"CreationDate,omitempty"`
	LastModifiedDate *EpochSeconds     `json:"LastModifiedDate,omitempty"`
}

// poolToOut projects a persisted user pool onto the published shape.
func poolToOut(pool CognitoUserPool) cognitoUserPoolOut {
	return cognitoUserPoolOut{
		ID:               pool.UserPoolID,
		Name:             pool.Name,
		Arn:              pool.Arn,
		Status:           pool.Status,
		Policies:         pool.Policies,
		LambdaConfig:     pool.LambdaConfig,
		SchemaAttributes: pool.SchemaAttributes,
		MfaConfiguration: pool.MfaConfiguration,
		UserPoolTags:     pool.Tags,
		CreationDate:     cognitoTimeOrNil(pool.CreationDate),
		LastModifiedDate: cognitoTimeOrNil(pool.LastModifiedDate),
	}
}

// cognitoUserPoolClientOut is the UserPoolClient of CreateUserPoolClient, DescribeUserPoolClient and
// UpdateUserPoolClient.
//
// Member names follow API_UserPoolClientType, on which every member is Required: No. The members
// substrate does not model are absent rather than present and zero: AccessTokenValidity,
// AllowedOAuthFlows, AllowedOAuthFlowsUserPoolClient, AllowedOAuthScopes, AnalyticsConfiguration,
// AuthSessionValidity, CallbackURLs, DefaultRedirectURI,
// EnablePropagateAdditionalUserContextData, EnableTokenRevocation, IdTokenValidity,
// LastModifiedDate, LogoutURLs, PreventUserExistenceErrors, ReadAttributes, RefreshTokenRotation,
// RefreshTokenValidity, SupportedIdentityProviders, TokenValidityUnits and WriteAttributes.
type cognitoUserPoolClientOut struct {
	ClientID          string        `json:"ClientId"`
	ClientName        string        `json:"ClientName"`
	UserPoolID        string        `json:"UserPoolId"`
	ClientSecret      string        `json:"ClientSecret,omitempty"`
	ExplicitAuthFlows []string      `json:"ExplicitAuthFlows,omitempty"`
	CreationDate      *EpochSeconds `json:"CreationDate,omitempty"`
}

// clientToOut projects a persisted app client onto the published shape.
func clientToOut(client CognitoUserPoolClient) cognitoUserPoolClientOut {
	return cognitoUserPoolClientOut{
		ClientID:          client.ClientID,
		ClientName:        client.ClientName,
		UserPoolID:        client.UserPoolID,
		ClientSecret:      client.ClientSecret,
		ExplicitAuthFlows: client.ExplicitAuthFlows,
		CreationDate:      cognitoTimeOrNil(client.CreationDate),
	}
}

// cognitoGroupOut is the Group of CreateGroup and GetGroup, and one member of ListGroups' and
// AdminListGroupsForUser's Groups.
//
// Member names follow API_GroupType, on which every member is Required: No. LastModifiedDate is the
// one member substrate does not model.
//
// Precedence is reported as the stored value rather than omitted when zero. The page's default is
// null and zero is its highest valid precedence, so the two are distinguishable in the published
// shape but not in the record, which holds a plain int — telling them apart is a record change
// rather than a projection one.
type cognitoGroupOut struct {
	GroupName    string        `json:"GroupName"`
	UserPoolID   string        `json:"UserPoolId"`
	Description  string        `json:"Description,omitempty"`
	RoleArn      string        `json:"RoleArn,omitempty"`
	Precedence   int           `json:"Precedence"`
	CreationDate *EpochSeconds `json:"CreationDate,omitempty"`
}

// groupToOut projects a persisted group onto the published shape.
func groupToOut(group CognitoGroup) cognitoGroupOut {
	return cognitoGroupOut{
		GroupName:    group.GroupName,
		UserPoolID:   group.UserPoolID,
		Description:  group.Description,
		RoleArn:      group.RoleArn,
		Precedence:   group.Precedence,
		CreationDate: cognitoTimeOrNil(group.CreationDate),
	}
}

// cognitoAttributeOut is one Attribute of a user's Attributes.
//
// Member names follow API_AttributeType, whose two members are the whole shape: Name is its one
// Required: Yes member and Value is Required: No.
//
// This is the one projection written as a conversion from the record rather than field-by-field, since
// the two shapes agree member-for-member and only the `,omitempty` differs. A member added to
// [CognitoAttribute] therefore fails the build here rather than reaching the wire unexamined, which is
// the behavior this file exists to produce.
type cognitoAttributeOut struct {
	Name  string `json:"Name"`
	Value string `json:"Value,omitempty"`
}

// cognitoUserOut is the User of AdminCreateUser and one member of ListUsers' Users.
//
// Member names follow API_UserType, on which every member is Required: No. MFAOptions is the one
// member substrate does not model.
//
// The record's UserPoolID and Groups are not here, and that is the published shape rather than an
// omission: API_UserType declares neither. The pool ID is how a user is keyed and
// AdminListGroupsForUser is what reports a user's groups.
type cognitoUserOut struct {
	Username             string                `json:"Username"`
	UserStatus           string                `json:"UserStatus"`
	Enabled              bool                  `json:"Enabled"`
	Attributes           []cognitoAttributeOut `json:"Attributes,omitempty"`
	UserCreateDate       *EpochSeconds         `json:"UserCreateDate,omitempty"`
	UserLastModifiedDate *EpochSeconds         `json:"UserLastModifiedDate,omitempty"`
}

// userToOut projects a persisted user onto the published shape.
func userToOut(user CognitoUser) cognitoUserOut {
	out := cognitoUserOut{
		Username:             user.Username,
		UserStatus:           user.UserStatus,
		Enabled:              user.Enabled,
		UserCreateDate:       cognitoTimeOrNil(user.UserCreateDate),
		UserLastModifiedDate: cognitoTimeOrNil(user.UserLastModifiedDate),
	}
	for _, a := range user.Attributes {
		out.Attributes = append(out.Attributes, cognitoAttributeOut(a))
	}
	return out
}
