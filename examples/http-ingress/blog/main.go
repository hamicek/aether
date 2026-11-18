// Command blog is a demo web thrall: a plain genserver that IS a web server (AE-091). It owns no
// socket and binds no port - the ingress forwards HTTP to it over NATS as http_request calls. It does
// its own tiny path routing and returns HTML, which is the whole point: "write a thrall as a web
// server" instead of wiring up an HTTP server.
package main

import (
	"log"
	"net/http"

	"github.com/hamicek/aether/sdk/go/thrall"
)

func main() {
	// Serve is the shape of a web thrall that keeps nothing between requests: requests run
	// concurrently (up to thrall.DefaultWebConcurrency), so a slow one does not hold up the rest.
	def := thrall.WebDef[struct{}]{
		Serve: func(req thrall.HTTPRequest, _ struct{}, ctx *thrall.Ctx) (thrall.HTTPResponse, error) {
			switch req.Path {
			case "/":
				return thrall.HTMLResponse(http.StatusOK,
					page("the blog", "<p>Served straight from a NATS thrall - it owns no port of its own.</p>")), nil
			case "/about":
				return thrall.HTMLResponse(http.StatusOK,
					page("about", "<p>A web thrall answers <code>http_request</code>; the ingress does the HTTP.</p>")), nil
			default:
				return thrall.HTMLResponse(http.StatusNotFound, page("not found", "<p>No such page.</p>")), nil
			}
		},
		Version: "0.1.0",
	}
	if err := thrall.StartWeb(def); err != nil {
		log.Fatalf("blog: %v", err)
	}
}

func page(title, body string) string {
	return "<!doctype html><html><head><title>" + title + "</title></head>" +
		"<body><h1>" + title + "</h1>" + body + "</body></html>"
}
