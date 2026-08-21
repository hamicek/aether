# Canonical Go image for aether (AE-064). Builds the aether runtime and the Go counter thrall,
# then runs a Go-only demo manifest on an embedded NATS. Thralls are just `cmd`, so a polyglot
# deployment (TS/Python thralls) builds its own image on top of the same idea - see the README
# "Deployment (Docker)" section for the recipe.

# ---- build stage ---------------------------------------------------------
FROM golang:1.23-bookworm AS build

# The toolchain in the base image already matches go.mod (1.23); never fetch a different one.
ENV GOTOOLCHAIN=local
WORKDIR /src

# Warm the module cache on the manifests before copying the full tree, so code-only edits do not
# re-download dependencies.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static binaries (CGO off) so they run on a slim base without libc surprises.
RUN CGO_ENABLED=0 go build -o /out/aether       ./cmd/aether \
 && CGO_ENABLED=0 go build -o /out/counter-go   ./examples/counter

# ---- runtime stage -------------------------------------------------------
# debian-slim (not distroless/scratch) is the batteries-included default: `procps` gives the `ps`
# format the RSS/CPU dashboard needs (AE-034), and `tini` is a real PID 1 that reaps orphaned
# grandchildren. A distroless/scratch image is smaller but drops metrics (they degrade gracefully -
# a skipped tick, not a crash); see the README for that tradeoff.
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
 && apt-get install -y --no-install-recommends procps tini \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /out/aether     /app/bin/aether
COPY --from=build /out/counter-go /app/bin/counter-go
COPY examples/counter/aether-docker.toml /app/aether-docker.toml

# tini as PID 1 reaps the `sh -c <cmd>` grandchildren that the lord does not Wait on directly.
# Equivalent to `docker run --init`; baking it in means a plain `docker run` is already correct.
ENTRYPOINT ["/usr/bin/tini", "--"]

# SIGTERM (docker stop) reaches the lord, which drains within its 2s grace, inside Docker's 10s
# window. Override CMD to point at your own manifest.
CMD ["/app/bin/aether", "up", "-f", "/app/aether-docker.toml"]
