package main

import (
	"encoding/json"
	u "github.com/araddon/gou"
	"net/http"
)

// ErrorResponse - Return an error response.
func ErrorResponse(w *http.ResponseWriter, code int, responseText string, logMessage string, err error) {
	errorMessage := ""
	writer := *w

	if err != nil {
		errorMessage = err.Error()
	}

	u.Error(logMessage, errorMessage)
	writer.WriteHeader(code)
	writer.Header().Add("Access-Control-Allow-Origin", "*")
	writer.Header().Add("Access-Control-Allow", "*")
	writer.Write([]byte(responseText))
}

// SuccessResponse - Return an success response.
func SuccessResponse(w *http.ResponseWriter, result interface{}) {
	writer := *w
	marshalled, err := json.Marshal(result)

	if err != nil {
		ErrorResponse(w, 500, "Internal Server Error", "Error marshalling response JSON", err)
		return
	}

	writer.Header().Add("Content-Type", "application/json")
	writer.Header().Add("Access-Control-Allow-Origin", "*")
	writer.Header().Add("Access-Control-Allow", "*")
	writer.WriteHeader(200)
	writer.Write(marshalled)
}
