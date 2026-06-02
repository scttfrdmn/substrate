package substrate

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// sagemakerCtrlNamespace is the state namespace for SageMaker control-plane data.
const sagemakerCtrlNamespace = "sagemaker-ctrl"

// sagemakerCtrlTrainingJobStatusKey returns the state key for a seeded training
// job status override.
func sagemakerCtrlTrainingJobStatusKey(name string) string {
	return "trainingjob_status:" + name
}

// handleSageMakerSeedTrainingJobStatus handles POST /v1/sagemaker/training-job-status.
// It overrides the terminal status (and optional FailureReason) returned by
// DescribeTrainingJob for a given training job name (or wildcard "*"), letting
// tests drive a job to a Failed/CapacityError outcome without simulated time.
// Body: {"trainingJobName": "...", "status": "Failed", "failureReason": "CapacityError: ..."}
// The "trainingJobName" field defaults to "*" (wildcard) if omitted or empty.
func (s *Server) handleSageMakerSeedTrainingJobStatus(w http.ResponseWriter, r *http.Request) {
	var body struct {
		TrainingJobName string `json:"trainingJobName"`
		Status          string `json:"status"`
		FailureReason   string `json:"failureReason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if body.Status == "" {
		http.Error(w, `{"error":"status is required"}`, http.StatusBadRequest)
		return
	}
	name := body.TrainingJobName
	if name == "" {
		name = "*"
	}
	seed, err := json.Marshal(map[string]string{"status": body.Status, "failureReason": body.FailureReason})
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), sagemakerCtrlNamespace, sagemakerCtrlTrainingJobStatusKey(name), seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true, "trainingJobName": name})
}

// handleSageMakerClearTrainingJobStatus handles DELETE /v1/sagemaker/training-job-status.
// With ?trainingJobName=... it removes the seeded status for that specific job.
// Without a query param it removes all seeded training job statuses.
func (s *Server) handleSageMakerClearTrainingJobStatus(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("trainingJobName")
	if name != "" {
		if err := s.state.Delete(r.Context(), sagemakerCtrlNamespace, sagemakerCtrlTrainingJobStatusKey(name)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
		return
	}
	keys, err := s.state.List(r.Context(), sagemakerCtrlNamespace, "trainingjob_status:")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), sagemakerCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
}
