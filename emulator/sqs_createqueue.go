package emulator

import "sort"

// sqsAttributeDefaults are the values GetQueueAttributes reports for a queue that
// was created without naming them, used to resolve an existing queue's unset
// attributes before comparing them against a CreateQueue request (#429).
//
// Resolving through defaults is what keeps a legitimate re-create idempotent. A
// queue created with no attributes has an empty map, but its VisibilityTimeout is
// observably 30 — GetQueueAttributes says so. A request naming VisibilityTimeout=30
// therefore asks for exactly what the queue already has, and raising
// QueueNameExists for it would reject a request AWS accepts. Comparing raw map
// entries would do precisely that, because "30" is not absent.
//
// The values deliberately mirror getQueueAttributes' inline fallbacks rather than
// the AWS reference, so the two cannot disagree about what a queue's effective
// attributes are — a comparison that used different defaults than the read path
// would reject requests matching what a caller had just read back. That does carry
// one known staleness through: MaximumMessageSize is 262144 (256 KiB) here, while
// the current CreateQueue reference documents 1,048,576 (1 MiB). Correcting it
// changes GetQueueAttributes output, so it is tracked separately rather than
// smuggled into this change.
// TODO(#439): reconcile MaximumMessageSize with the documented 1 MiB default.
var sqsAttributeDefaults = map[string]string{
	"VisibilityTimeout":             "30",
	"MaximumMessageSize":            "262144",
	"MessageRetentionPeriod":        "345600",
	"DelaySeconds":                  "0",
	"ReceiveMessageWaitTimeSeconds": "0",
}

// sqsEffectiveAttribute returns the value a queue observably reports for an
// attribute, and whether it has one at all.
//
// FifoQueue is resolved from the queue's own flag rather than its attribute map,
// because substrate derives it from the ".fifo" name suffix and does not copy it
// into Attributes. Without this, re-creating a FIFO queue with FifoQueue=true — the
// request every SDK and CloudFormation template sends — would compare "true"
// against an absent value and be rejected.
func sqsEffectiveAttribute(q *SQSQueue, name string) (string, bool) {
	if v, ok := q.Attributes[name]; ok {
		return v, true
	}
	if name == "FifoQueue" {
		if q.FifoQueue {
			return "true", true
		}
		return "false", true
	}
	if v, ok := sqsAttributeDefaults[name]; ok {
		return v, true
	}
	return "", false
}

// sqsConflictingAttribute returns the name of the first attribute whose requested
// value differs from the existing queue's, or "" when the request asks for nothing
// the queue does not already have.
//
// Only attributes **present in the request** are compared; one the request omits is
// treated as "no opinion" rather than as an assertion of its default. That reading
// comes from the error's own definition in the CreateQueue reference — "Amazon SQS
// returns this error only if the request includes attributes whose values differ
// from those of the existing queue" — which scopes the comparison to what the
// request includes. The page's other statement, that providing "the exact same
// names and values for all its attributes" returns the existing URL, gives a
// sufficient condition for idempotency, not a necessary one, so it does not
// contradict the narrower error definition.
//
// It is also the only reading that keeps substrate's own CloudFormation path
// working: deploySQSQueue forwards only the properties a template declares, so a
// stack that dropped a property between deploys would send a strict subset on
// re-deploy and fail to update at all under the stricter reading.
//
// Comparison is exact string equality on the resolved values. That means Policy and
// RedrivePolicy are compared as serialized text, not as JSON documents, so two
// semantically identical policies differing in whitespace or key order read as a
// conflict. Any SDK or template re-sending its own serialization matches, which is
// the case that has to work; semantic JSON comparison is not attempted.
//
// The attributes are checked in sorted order so the attribute named in the error is
// deterministic — a message that varied by map iteration order would make the
// error itself irreproducible, which is the opposite of what this emulator is for.
func sqsConflictingAttribute(q *SQSQueue, requested map[string]string) string {
	names := make([]string, 0, len(requested))
	for name := range requested {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		want := requested[name]
		got, known := sqsEffectiveAttribute(q, name)
		if !known {
			// An attribute the queue has no value for at all — neither stored nor
			// defaulted. The request is asking for something the queue does not
			// have, so it differs.
			return name
		}
		if want != got {
			return name
		}
	}
	return ""
}
