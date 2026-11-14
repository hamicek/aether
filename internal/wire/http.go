package wire

import "encoding/json"

// OpHTTPRequest is the single call op a web thrall answers: it receives an HTTPRequest payload and
// replies with an HTTPResponse. There is no separate response op - http_response is the reply
// payload, not a message of its own.
//
// This is the "web thrall is pure NATS" contract (AE-091): a web thrall is an ordinary genserver
// whose one operation is the whole HTTP request/response. It never owns a socket or a port; a
// separate ingress translates HTTP <-> envelope and calls it over NATS. That makes it the mirror
// image of the [[edge.http]] model (internal/edge), where the edge owns the socket and maps a
// route to an operation and the thrall never sees HTTP. Both models coexist: use [[edge.http]] to
// expose an existing thrall's operations over HTTP, and this contract to write a thrall that *is* a
// web server.
const OpHTTPRequest = "http_request"

// HTTPRequest is the payload of an http_request call: a whole inbound HTTP request carried over
// NATS. The ingress fills it from the real request; the web thrall reads it and does its own
// routing on Method and Path.
//
// Body is a []byte, which Go's json marshals as a base64 string, so the contract is binary-safe
// (an image or any non-text body survives round-trip) and uniform across languages - the TS and
// Python SDKs carry Body as a base64 string. Headers mirror http.Header (a key maps to one or more
// values, so a repeated header such as Set-Cookie is preserved). Query is the raw query string
// without the leading '?' (lossless; the thrall parses it if it cares). The fields with omitempty
// are absent on the wire when empty, matching the TS/Python mirrors.
type HTTPRequest struct {
	Method  string              `json:"method"`
	Path    string              `json:"path"`
	Query   string              `json:"query,omitempty"`
	Headers map[string][]string `json:"headers,omitempty"`
	Body    []byte              `json:"body,omitempty"`
}

// HTTPResponse is the reply payload of an http_request call: the status, headers and body the
// ingress writes back to the HTTP client. Body is base64 in JSON (see HTTPRequest), Status has no
// omitempty because every response has one, and 0 would be a malformed response rather than an
// omitted field.
type HTTPResponse struct {
	Status  int                 `json:"status"`
	Headers map[string][]string `json:"headers,omitempty"`
	Body    []byte              `json:"body,omitempty"`
}

// Header builds a single-value header map, the common case when constructing a response. Header
// keys are used verbatim (the ingress applies them as given), so pass the canonical MIME casing,
// e.g. "Content-Type".
func Header(key, value string) map[string][]string {
	return map[string][]string{key: {value}}
}

// TextResponse builds a plain-text response with the given status and a text/plain content type.
func TextResponse(status int, text string) HTTPResponse {
	return HTTPResponse{Status: status, Headers: Header("Content-Type", "text/plain; charset=utf-8"), Body: []byte(text)}
}

// HTMLResponse builds an HTML response with the given status and a text/html content type.
func HTMLResponse(status int, html string) HTTPResponse {
	return HTTPResponse{Status: status, Headers: Header("Content-Type", "text/html; charset=utf-8"), Body: []byte(html)}
}

// JSONResponse marshals v and builds an application/json response with the given status. It returns
// an error only when v cannot be marshalled, so the caller can decide the failure mode rather than
// shipping a broken body.
func JSONResponse(status int, v any) (HTTPResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return HTTPResponse{}, err
	}
	return HTTPResponse{Status: status, Headers: Header("Content-Type", "application/json"), Body: body}, nil
}
