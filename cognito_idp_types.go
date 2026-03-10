package substrate

import "time"

// cognitoIDPNamespace is the state namespace for Cognito User Pools (IDP).
const cognitoIDPNamespace = "cognito-idp"

// cognitoIdentityNamespace is the state namespace for Cognito Identity Pools.
const cognitoIdentityNamespace = "cognito-identity"

// CognitoUserPool holds the persisted state of a Cognito User Pool.
type CognitoUserPool struct {
	// UserPoolID is the unique pool identifier in the format {region}_{12-char alphanum}.
	UserPoolID string `json:"UserPoolId"`

	// Name is the human-readable name for the user pool.
	Name string `json:"Name"`

	// Arn is the Amazon Resource Name for the user pool.
	Arn string `json:"Arn"`

	// ProviderName is the IdP endpoint: cognito-idp.{region}.amazonaws.com/{poolId}.
	ProviderName string `json:"ProviderName"`

	// Status is the pool status (e.g., Enabled).
	Status string `json:"Status"`

	// Policies holds the optional password policy configuration.
	Policies interface{} `json:"Policies,omitempty"`

	// LambdaConfig holds optional Lambda trigger configuration.
	LambdaConfig interface{} `json:"LambdaConfig,omitempty"`

	// SchemaAttributes holds optional custom schema attribute definitions.
	SchemaAttributes []interface{} `json:"SchemaAttributes,omitempty"`

	// MfaConfiguration is the MFA setting: OFF, ON, or OPTIONAL.
	MfaConfiguration string `json:"MfaConfiguration,omitempty"`

	// Tags holds arbitrary key-value metadata attached to the pool.
	Tags map[string]string `json:"Tags,omitempty"`

	// CreationDate is the time the pool was created.
	CreationDate time.Time `json:"CreationDate"`

	// LastModifiedDate is the time the pool was last modified.
	LastModifiedDate time.Time `json:"LastModifiedDate"`

	// AccountID is the AWS account that owns the pool.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where the pool resides.
	Region string `json:"Region"`
}

// CognitoUserPoolClient holds the persisted state of a User Pool app client.
type CognitoUserPoolClient struct {
	// ClientID is the unique app client identifier.
	ClientID string `json:"ClientId"`

	// ClientName is the human-readable name for the app client.
	ClientName string `json:"ClientName"`

	// UserPoolID is the ID of the user pool this client belongs to.
	UserPoolID string `json:"UserPoolId"`

	// ClientSecret is the optional secret for confidential clients.
	ClientSecret string `json:"ClientSecret,omitempty"`

	// ExplicitAuthFlows lists the authentication flows permitted for this client.
	ExplicitAuthFlows []string `json:"ExplicitAuthFlows,omitempty"`

	// CreationDate is the time the client was created.
	CreationDate time.Time `json:"CreationDate"`

	// AccountID is the AWS account that owns this client.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where the client resides.
	Region string `json:"Region"`
}

// CognitoGroup holds the persisted state of a User Pool group.
type CognitoGroup struct {
	// GroupName is the name of the group.
	GroupName string `json:"GroupName"`

	// UserPoolID is the ID of the user pool this group belongs to.
	UserPoolID string `json:"UserPoolId"`

	// Description is an optional description for the group.
	Description string `json:"Description,omitempty"`

	// RoleArn is the optional IAM role ARN associated with the group.
	RoleArn string `json:"RoleArn,omitempty"`

	// Precedence determines group priority when a user belongs to multiple groups.
	Precedence int `json:"Precedence"`

	// CreationDate is the time the group was created.
	CreationDate time.Time `json:"CreationDate"`

	// AccountID is the AWS account that owns this group.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where the group resides.
	Region string `json:"Region"`
}

// CognitoUser holds the persisted state of a User Pool user.
type CognitoUser struct {
	// Username is the unique login name for the user.
	Username string `json:"Username"`

	// UserStatus is the current status: CONFIRMED, UNCONFIRMED, or FORCE_CHANGE_PASSWORD.
	UserStatus string `json:"UserStatus"`

	// Enabled indicates whether the user account is active.
	Enabled bool `json:"Enabled"`

	// Attributes holds the user's attribute name-value pairs.
	Attributes []CognitoAttribute `json:"Attributes,omitempty"`

	// Groups lists the names of groups this user belongs to.
	Groups []string `json:"Groups,omitempty"`

	// UserCreateDate is the time the user was created.
	UserCreateDate time.Time `json:"UserCreateDate"`

	// UserLastModifiedDate is the time the user was last modified.
	UserLastModifiedDate time.Time `json:"UserLastModifiedDate"`

	// UserPoolID is the ID of the user pool this user belongs to.
	UserPoolID string `json:"UserPoolId"`

	// AccountID is the AWS account that owns this user.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where the user resides.
	Region string `json:"Region"`
}

// CognitoAttribute is a name-value attribute attached to a Cognito user.
type CognitoAttribute struct {
	// Name is the attribute name.
	Name string `json:"Name"`

	// Value is the attribute value.
	Value string `json:"Value"`
}

// CognitoIdentityPool holds the persisted state of a Cognito Identity Pool.
type CognitoIdentityPool struct {
	// IdentityPoolID is the unique pool identifier in the format {region}:{uuid}.
	IdentityPoolID string `json:"IdentityPoolId"`

	// IdentityPoolName is the human-readable name for the identity pool.
	IdentityPoolName string `json:"IdentityPoolName"`

	// AllowUnauthenticatedIdentities indicates whether unauthenticated access is permitted.
	AllowUnauthenticatedIdentities bool `json:"AllowUnauthenticatedIdentities"`

	// Roles maps role types (authenticated, unauthenticated) to IAM role ARNs.
	Roles map[string]string `json:"Roles,omitempty"`

	// Tags holds arbitrary key-value metadata attached to the identity pool.
	Tags map[string]string `json:"Tags,omitempty"`

	// CreationDate is the time the identity pool was created.
	CreationDate time.Time `json:"CreationDate"`

	// AccountID is the AWS account that owns this identity pool.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where the identity pool resides.
	Region string `json:"Region"`
}

// cognitoUserPoolKey returns the state key for a Cognito User Pool.
func cognitoUserPoolKey(accountID, region, poolID string) string {
	return "userpool:" + accountID + "/" + region + "/" + poolID
}

// cognitoUserPoolIDsKey returns the state index key for all user pool IDs in an account/region.
func cognitoUserPoolIDsKey(accountID, region string) string {
	return "userpool_ids:" + accountID + "/" + region
}

// cognitoUserPoolClientKey returns the state key for a User Pool app client.
func cognitoUserPoolClientKey(accountID, region, poolID, clientID string) string {
	return "userpoolclient:" + accountID + "/" + region + "/" + poolID + "/" + clientID
}

// cognitoUserPoolClientIDsKey returns the state index key for all client IDs in a pool.
func cognitoUserPoolClientIDsKey(accountID, region, poolID string) string {
	return "userpoolclient_ids:" + accountID + "/" + region + "/" + poolID
}

// cognitoGroupKey returns the state key for a User Pool group.
func cognitoGroupKey(accountID, region, poolID, groupName string) string {
	return "userpoolgroup:" + accountID + "/" + region + "/" + poolID + "/" + groupName
}

// cognitoGroupNamesKey returns the state index key for all group names in a pool.
func cognitoGroupNamesKey(accountID, region, poolID string) string {
	return "group_names:" + accountID + "/" + region + "/" + poolID
}

// cognitoUserKey returns the state key for a User Pool user.
func cognitoUserKey(accountID, region, poolID, username string) string {
	return "user:" + accountID + "/" + region + "/" + poolID + "/" + username
}

// cognitoUserNamesKey returns the state index key for all usernames in a pool.
func cognitoUserNamesKey(accountID, region, poolID string) string {
	return "user_names:" + accountID + "/" + region + "/" + poolID
}

// cognitoIdentityPoolKey returns the state key for a Cognito Identity Pool.
func cognitoIdentityPoolKey(accountID, region, poolID string) string {
	return "identitypool:" + accountID + "/" + region + "/" + poolID
}

// cognitoIdentityPoolIDsKey returns the state index key for all identity pool IDs in an account/region.
func cognitoIdentityPoolIDsKey(accountID, region string) string {
	return "identitypool_ids:" + accountID + "/" + region
}
