# SSO / Identity Store

**Endpoint:** `sso.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: SWBExternalService.{Op}`)

`SWBExternalService` is the `sso-admin` API's target prefix — not a name that appears
in any published SDK surface, but what every client sends. Substrate accepts the
plausible-looking `AWSSSOAdminService` as well, so a caller that constructs the target
header by hand from the service name also reaches this plugin.

Responses carry `Content-Type: application/x-amz-json-1.1` and errors are shaped as AWS
JSON RPC, with the code in the body's `__type` member. Both were wrong until #758:
substrate sent the unversioned `application/json` and shaped errors as REST-JSON, which
puts the code in an `x-amzn-errortype` header that botocore's JSON parser never reads —
so a refused call reported the stringified HTTP status instead of the error code. The
cause of both was reading this plugin as the `sso` service (the OIDC token and
account-list API, which really is REST-JSON) rather than as `sso-admin`, whose model is
`"protocol": "json"` with `"jsonVersion": "1.1"`.

## Supported operations

| Operation | Notes |
|-----------|-------|
| ListInstances | One instance per account, created on first read |
| CreatePermissionSet | |
| DescribePermissionSet | |
| UpdatePermissionSet | |
| DeletePermissionSet | |
| ListPermissionSets | |
| AttachManagedPolicyToPermissionSet | |
| DetachManagedPolicyFromPermissionSet | |
| ListManagedPoliciesInPermissionSet | |
| CreateAccountAssignment | |
| DeleteAccountAssignment | |
| ListAccountAssignments | |
