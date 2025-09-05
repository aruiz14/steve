#!/bin/bash
set -euo pipefail

echo "Building binaries with GOARCH=$(go env GOARCH)"
CGO_ENABLED=0 go build -gcflags="all=-N -l" -o ./bin/steve
CGO_ENABLED=0 go build -gcflags="all=-N -l" -o ./bin/proxylimiter ./exp/cmd/proxylimiter
