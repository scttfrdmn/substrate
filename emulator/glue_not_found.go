package emulator

import "net/http"

// Glue's twelve not-found refusals answer the status its pages publish, through one constructor (#1098).
//
// Every site answered `EntityNotFoundException` at **404**. Glue publishes that code at **400** on every
// page that lists it — `API_GetTable`'s Errors section reads *"EntityNotFoundException / A specified
// entity does not exist / HTTP Status Code: 400"*, and the three tagging pages (`API_TagResource`,
// `API_UntagResource`, `API_GetTags`) publish the same figure. No Glue page publishes a 404 at all:
// the service's consolidated error list carries fifteen codes and none of them is a 404.
//
// #1063 corrected the *codes* in this file and deliberately left the statuses, which is how twelve sites
// came to drift together. #910 is the in-tree precedent for the correction and for its reasoning: an
// SDK's error parser reads the code, so most callers are unaffected — which is exactly why this
// survived — but a raw HTTP client, a retry classifier keyed on 4xx sub-ranges, a gateway with
// per-status handling and any assertion written against the response line all read the status, and for
// them substrate was answering a shape AWS never sends.
//
// # Why a constructor rather than twelve corrected literals
//
// The twelve sites were twelve inline `&AWSError{…}` literals for one condition, differing only in the
// noun and the name. That is the repeated-literal shape #950 found wrong codes hiding in, and it is what
// let the status be wrong in twelve places without any one of them looking wrong. One constructor means
// the next entity Glue models cannot arrive with a thirteenth spelling, and a future correction — the
// message wording, say — is one edit rather than twelve.
//
// The message keeps the wording the twelve already shared, `"<Entity> <name> not found."`, rather than
// adopting the published gloss. The gloss (*"A specified entity does not exist"*) names neither which
// entity nor which name, so a caller that passed a database and a table could not tell the two cases
// apart from it; naming them is substrate's addition, as it is everywhere else in the tree.

// glueEntityNotFound refuses a request naming a Glue entity that has no record.
//
// entity is the noun as the message spells it — "Database", "Table", "Connection", "Crawler", "Job",
// "Job run" — and name is the identifier the request carried, so a caller is told which of the two it
// got wrong. The status is [http.StatusBadRequest] for the reason this file's comment gives; it is set
// here rather than at each site so that the twelve cannot drift again.
func glueEntityNotFound(entity, name string) *AWSError {
	return &AWSError{
		Code:       "EntityNotFoundException",
		Message:    entity + " " + name + " not found.",
		HTTPStatus: http.StatusBadRequest,
	}
}
