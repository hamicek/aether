package thrall

import (
	"encoding/json"
	"strconv"
	"testing"
)

// state used to prove a web handler can carry state across requests.
type webState struct{ hits int }

func TestWebCallFnServesRequest(t *testing.T) {
	handle := func(req HTTPRequest, s webState, ctx *Ctx) (HTTPResponse, webState, error) {
		s.hits++
		return HTMLResponse(200, "path="+req.Path+" hits="+strconv.Itoa(s.hits)), s, nil
	}
	fn := webCallFn(handle)

	payload, _ := json.Marshal(HTTPRequest{Method: "GET", Path: "/hello"})
	reply, next, err := fn(payload, webState{hits: 4}, &Ctx{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next.hits != 5 {
		t.Errorf("state not threaded through: got hits=%d, want 5", next.hits)
	}
	resp, ok := reply.(HTTPResponse)
	if !ok {
		t.Fatalf("reply is not an HTTPResponse: %T", reply)
	}
	if resp.Status != 200 || string(resp.Body) != "path=/hello hits=5" {
		t.Errorf("unexpected response: %+v body=%q", resp, resp.Body)
	}
	// The reply marshals to the http_response wire form.
	if b, _ := json.Marshal(resp); string(b) == "" {
		t.Error("response did not marshal")
	}
}

func TestWebCallFnRejectsMalformedPayload(t *testing.T) {
	fn := webCallFn(func(HTTPRequest, struct{}, *Ctx) (HTTPResponse, struct{}, error) {
		t.Fatal("handler must not run on a malformed payload")
		return HTTPResponse{}, struct{}{}, nil
	})
	if _, _, err := fn(json.RawMessage(`not json`), struct{}{}, &Ctx{}); err == nil {
		t.Error("expected a decode error for a malformed payload")
	}
}

func TestWebCallFnPropagatesHandlerError(t *testing.T) {
	const sentinel = "handler blew up"
	fn := webCallFn(func(HTTPRequest, struct{}, *Ctx) (HTTPResponse, struct{}, error) {
		return HTTPResponse{}, struct{}{}, errString(sentinel)
	})
	payload, _ := json.Marshal(HTTPRequest{Method: "GET", Path: "/"})
	if _, _, err := fn(payload, struct{}{}, &Ctx{}); err == nil || err.Error() != sentinel {
		t.Errorf("handler error not propagated: %v", err)
	}
}

func TestStartWebRequiresHandle(t *testing.T) {
	if err := StartWeb(WebDef[struct{}]{Name: "web"}); err == nil {
		t.Error("expected an error when Handle is nil")
	}
}

func TestResponseConstructors(t *testing.T) {
	if r := TextResponse(200, "hi"); r.Headers["Content-Type"][0] != "text/plain; charset=utf-8" || string(r.Body) != "hi" {
		t.Errorf("TextResponse: %+v", r)
	}
	if r := HTMLResponse(200, "<b>x</b>"); r.Headers["Content-Type"][0] != "text/html; charset=utf-8" {
		t.Errorf("HTMLResponse: %+v", r)
	}
	r, err := JSONResponse(200, map[string]bool{"ok": true})
	if err != nil || string(r.Body) != `{"ok":true}` {
		t.Errorf("JSONResponse: %+v err=%v", r, err)
	}
}

// errString is a minimal error with an exact message, so a test can assert the handler's error is
// propagated verbatim.
type errString string

func (e errString) Error() string { return string(e) }
