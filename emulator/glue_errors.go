package emulator

import "net/http"

// Glue's refusals for a request substrate could not use.
//
// #950 corrected Glue's body-parse guards to InvalidInputException and recorded that
// InvalidParameterValueException — the code the rest of the plugin answered — appears
// nowhere in Glue's reference. It deferred the guards that complain about a member or an
// identifier rather than about the body, and #1063 is all of them: there were exactly
// **four**, not the twenty-six [glueInvalidBody]'s comment claimed, and after this change
// the string InvalidParameterValueException does not occur in glue_plugin.go at all. The
// four are CreateDatabase's DatabaseInput.Name check and the ARN resolution behind
// TagResource, UntagResource and GetTags.
//
// InvalidInputException at 400, "The input provided was not valid.", is published on every
// page that governs those four sites — API_CreateDatabase, API_TagResource,
// API_UntagResource and API_GetTags — and InvalidParameterValueException is on none of
// them. DatabaseInput's Name is Required: Yes (API_DatabaseInput) and so is ResourceArn on
// each of the three tagging pages, so all four conditions are real refusals rather than
// substrate being strict.
//
// Each of the three tagging pages publishes exactly one other 400 that names the caller's
// input, and it is the one #1063 left open:
//
//   - EntityNotFoundException, "A specified entity does not exist". Declined, and the
//     reason is structural rather than a judgement: [resolveGlueARN]'s four failures are
//     all about the shape of the string — a missing "arn:aws:glue:" prefix, fewer than
//     three colon-separated fields, no slash in the resource segment, a resource type Glue
//     does not name — and not one of them reads state. Nothing has been looked up, so there
//     is no entity to report absent.
//
//     An ARN that parses and then addresses nothing is a different condition and it *is*
//     EntityNotFoundException's. Substrate does not distinguish it, because mergeGlueTags
//     and loadGlueTags treat an absent record as an empty tag set — recorded here as a
//     divergence rather than fixed, since making it a refusal changes what GetTags answers
//     for an untagged resource, which is not #1063's question.
//
// The messages are substrate's own. The three tagging sites read
//
//	Message: err.Error()
//
// which was not a decoder leak — the text was [resolveGlueARN]'s own fmt.Errorf — but it
// left the wire text decided in a function that does not know which code carries it, which
// is how three sites came to share one code no Glue page publishes. resolveGlueARN now
// returns the refusal itself, following loadClusterByARN's shape (msk_plugin.go) so that
// the resolver names both halves of what it reports.

// glueInvalidInput reports that a request cannot be used as given.
//
// One constructor for every such refusal in the plugin, so the code and the status cannot
// come to differ between the operation that creates a database and the one that tags it.
func glueInvalidInput(message string) *AWSError {
	return &AWSError{
		Code:       "InvalidInputException",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}
