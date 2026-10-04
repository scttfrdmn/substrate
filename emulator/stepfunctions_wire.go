package emulator

// The wire is a different thing from the state, and the type below exists to keep them apart.
//
// HistoryEvent (stepfunctions_types.go) is persisted: it is the element type of
// ExecutionState.History, which saveExecution marshals into state. getExecutionHistory handed the
// slice straight to the caller, so each event's Timestamp, a time.Time, answered as an RFC3339 string
// where API_HistoryEvent publishes `timestamp` as a Timestamp, Required: Yes — "seconds and fractional
// milliseconds since the Unix epoch". The sfn client's decoder expects a number there, so the whole
// GetExecutionHistory response failed to decode (#1323). Every other date the plugin answers already
// goes through sfnEpoch; this was the one that did not.
//
// Do not "fix" that by retyping HistoryEvent.Timestamp. MemoryStateManager snapshots the stored bytes
// and a replay reads them back, so a type change in place would change the encoding of every recorded
// run. Projecting leaves the stored bytes unchanged, and an execution recorded before #1323 reads back
// and answers the number like any other.

// sfnHistoryEventOut is one element of GetExecutionHistory's `events`, in API_HistoryEvent's shape.
//
// The members are HistoryEvent's own, under the same published names, with timestamp projected through
// sfnEpoch.
type sfnHistoryEventOut struct {
	ID                             int64                   `json:"id"`
	Type                           string                  `json:"type"`
	Timestamp                      float64                 `json:"timestamp"`
	ExecutionStartedEventDetails   *map[string]interface{} `json:"executionStartedEventDetails,omitempty"`
	ExecutionSucceededEventDetails *map[string]interface{} `json:"executionSucceededEventDetails,omitempty"`
	ExecutionFailedEventDetails    *map[string]interface{} `json:"executionFailedEventDetails,omitempty"`
	StateEnteredEventDetails       *map[string]interface{} `json:"stateEnteredEventDetails,omitempty"`
	StateExitedEventDetails        *map[string]interface{} `json:"stateExitedEventDetails,omitempty"`
	TaskScheduledEventDetails      *map[string]interface{} `json:"taskScheduledEventDetails,omitempty"`
	TaskSucceededEventDetails      *map[string]interface{} `json:"taskSucceededEventDetails,omitempty"`
	TaskFailedEventDetails         *map[string]interface{} `json:"taskFailedEventDetails,omitempty"`
}

// sfnHistoryToWire projects a persisted history onto GetExecutionHistory's published events.
func sfnHistoryToWire(history []HistoryEvent) []sfnHistoryEventOut {
	out := make([]sfnHistoryEventOut, 0, len(history))
	for _, ev := range history {
		out = append(out, sfnHistoryEventOut{
			ID:                             ev.ID,
			Type:                           ev.Type,
			Timestamp:                      sfnEpoch(ev.Timestamp),
			ExecutionStartedEventDetails:   ev.ExecutionStartedEventDetails,
			ExecutionSucceededEventDetails: ev.ExecutionSucceededEventDetails,
			ExecutionFailedEventDetails:    ev.ExecutionFailedEventDetails,
			StateEnteredEventDetails:       ev.StateEnteredEventDetails,
			StateExitedEventDetails:        ev.StateExitedEventDetails,
			TaskScheduledEventDetails:      ev.TaskScheduledEventDetails,
			TaskSucceededEventDetails:      ev.TaskSucceededEventDetails,
			TaskFailedEventDetails:         ev.TaskFailedEventDetails,
		})
	}
	return out
}
