package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/dharmendrasdlt/tenant-control-plane/internal/domain"
)

// statusForCode is the one documented mapping from a domain error code to an
// HTTP status. Keep it in sync with the codes in internal/domain/errors.go.
var statusForCode = map[string]int{
	domain.ErrTenantNotFound.Code:   http.StatusNotFound,
	domain.ErrTenantExists.Code:     http.StatusConflict,
	domain.ErrUpdateNotAllowed.Code: http.StatusConflict,
	domain.ErrVersionConflict.Code:  http.StatusConflict,
	domain.ErrTaskNotFound.Code:     http.StatusNotFound,
}

func writeError(w http.ResponseWriter, err error) {
	var de *domain.Error
	if errors.As(err, &de) {
		status, ok := statusForCode[de.Code]
		if !ok {
			status = http.StatusInternalServerError
		}
		writeJSON(w, status, errorBody{Error: errorDetail{Code: de.Code, Message: de.Message}})
		return
	}
	writeJSON(w, http.StatusInternalServerError, errorBody{
		Error: errorDetail{Code: "internal_error", Message: "unexpected server error"},
	})
}

func writeValidationError(w http.ResponseWriter, message string) {
	writeJSON(w, http.StatusBadRequest, errorBody{
		Error: errorDetail{Code: "invalid_request", Message: message},
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
