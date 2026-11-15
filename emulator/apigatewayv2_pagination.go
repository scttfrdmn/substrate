package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// The maxResults and nextToken pair every routed API Gateway v2 collection publishes (#1423).
//
// GetApis, GetRoutes, GetIntegrations, GetStages, GetAuthorizers and GetApiMappings each publish two
// query parameters, in the same words on every page: nextToken, "The next page of elements from this
// collection. Not valid for the last element of the collection.", and maxResults, "The maximum number
// of elements to be returned for this resource." Each also publishes BadRequestException/400, "One of
// the parameters in the request is invalid." Until #1423 none read either parameter, so every list
// answered its whole collection in one page.
//
// The pages publish no default, no maximum and no minimum for maxResults, and no order for the
// elements. So these readings are substrate's:
//
//   - **An absent maxResults is no limit**: the whole remainder in one page with no nextToken, which is
//     what every list answered before #1423, so an existing fixture is byte-identical.
//   - **maxResults must be a positive integer.** The member is typed String on the wire; a value that
//     is not a decimal integer, or is below one, is refused with BadRequestException, because a page of
//     zero elements describes a walk that never ends.
//   - **nextToken is the base64 offset the v1 collections use** ([encodeOffsetPaginationToken]), and a
//     token substrate could not have issued is refused with BadRequestException rather than answered
//     with page one (#915).
//   - **Elements are in index order**, which is lexicographic by ID or name, since every v2 index is
//     written through [updateStringIndex], which sorts. That stable order is the obligation
//     [pageByOffsetToken] states.
//
// Both parameters are read before any state, per #887, so what a malformed request is answered with
// does not depend on what the collection holds. The messages are substrate's own.

// apigwv2PageParams reads maxResults and nextToken off a v2 collection request. A pageSize of zero
// means no limit.
func apigwv2PageParams(req *AWSRequest) (pageSize, offset int, awsErr *AWSError) {
	if raw := req.Params["maxResults"]; raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			return 0, 0, &AWSError{
				Code:       "BadRequestException",
				Message:    "maxResults must be a positive integer: " + raw,
				HTTPStatus: http.StatusBadRequest,
			}
		}
		pageSize = n
	}
	token := req.Params["nextToken"]
	offset, ok := decodeOffsetPaginationToken(token)
	if !ok {
		return 0, 0, &AWSError{
			Code:       "BadRequestException",
			Message:    "Invalid nextToken: " + token,
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return pageSize, offset, nil
}

// apigwv2Page cuts one page of a v2 collection into its published envelope.
func apigwv2Page[T any](items []T, pageSize, offset int) apigwV2ItemsOut[T] {
	page, next := pageByOffsetToken(items, offset, pageSize)
	return apigwV2ItemsOut[T]{Items: page, NextToken: next}
}

// apigwv2LoadListed reads one record a v2 listing's index names. found is false only when the index
// entry outlived its record, which the listing skips; a store fault or a record that does not decode
// is an error, so it fails the listing rather than shortening a page without saying so (#1538), as
// GetApiMappings already did.
func apigwv2LoadListed[T any](ctx context.Context, state StateManager, key, op string) (T, bool, error) {
	var out T
	data, err := state.Get(ctx, apigatewayv2Namespace, key)
	if err != nil {
		return out, false, fmt.Errorf("apigatewayv2 %s state.Get %s: %w", op, key, err)
	}
	if data == nil {
		return out, false, nil
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return out, false, fmt.Errorf("apigatewayv2 %s unmarshal %s: %w", op, key, err)
	}
	return out, true, nil
}
