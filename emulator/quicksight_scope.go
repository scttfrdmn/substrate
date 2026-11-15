package emulator

import "net/http"

// QuickSight resources are identified by account, Region and ID, and the account in the path is the
// one the request addresses (#1167).
//
// # The path's AwsAccountId
//
// Every QuickSight operation substrate routes takes `AwsAccountId` in its URI — Required: Yes, "Fixed
// length of 12", Pattern `^[0-9]{12}$` — and every handler bound it to `_` and used the caller's
// account instead, so `POST /accounts/999999999999/data-sources` signed by 123456789012 created the
// data source in 123456789012 and answered 201. The path account is now read:
//
//   - One that does not match the pattern is InvalidParameterValueException/400, which each page
//     publishes as "One or more parameters has a value that isn't valid."
//   - One that is well formed but is not the caller's is AccessDeniedException at **401**. The status is
//     unusual and is not a mistake: API_CreateDataSource, API_DescribeDataSource, API_CreateDataSet and
//     API_DescribeIngestion each publish AccessDeniedException at "HTTP Status Code: 401", not the 403
//     most services use. The message is the page's own gloss. Substrate has no cross-account QuickSight
//     access to model — no resource policy grants another account a QuickSight resource here — so every
//     mismatch is refused.
//
// # State keys carry the Region
//
// API_CreateDataSource glosses `DataSourceId` "This ID is unique per AWS Region for each AWS account",
// and API_CreateDataSet says the same of `DataSetId`, so the identity is (account, Region, ID). The keys
// were `datasource:{account}/{id}` and `dataset:{account}/{id}` — two of the three — so a data source
// created in us-east-1 was visible to a describe in eu-west-1, and the same ID could not exist in two
// Regions. They are now `datasource:{account}/{region}/{id}` and `dataset:{account}/{region}/{id}`, the
// shape lambdaFunctionStateKey uses.
//
// Compatibility: state written before this change sits at the old keys and is unreachable after it, and
// a recording made before it replays to ResourceNotFoundException at the first describe of something it
// created, because the replayed create writes the new key while the old snapshot holds the old one.
// Nothing migrates the old keys, since a key that omits the Region cannot say which Region it belonged
// to.

// quicksightCheckPathAccount refuses a path AwsAccountId that is malformed or is not the caller's.
func quicksightCheckPathAccount(ctx *RequestContext, pathAccount string) error {
	if !isAccountID(pathAccount) {
		return quicksightInvalidParameterValue("AwsAccountId must be a 12-digit account ID")
	}
	if pathAccount != ctx.AccountID {
		return &AWSError{
			Code: "AccessDeniedException",
			// The published gloss, from API_DescribeDataSource's Errors section.
			Message: "You don't have access to this item. The provided credentials couldn't be validated. " +
				"You might not be authorized to carry out the request. Make sure that your account is " +
				"authorized to use the Amazon Quick Sight service, that your policies have the correct " +
				"permissions, and that you are using the correct credentials.",
			HTTPStatus: http.StatusUnauthorized,
		}
	}
	return nil
}

// quicksightDataSourceKey is the state key a data source is stored at.
func quicksightDataSourceKey(acct, region, id string) string {
	return "datasource:" + acct + "/" + region + "/" + id
}

// quicksightDataSetKey is the state key a dataset is stored at.
func quicksightDataSetKey(acct, region, id string) string {
	return "dataset:" + acct + "/" + region + "/" + id
}
