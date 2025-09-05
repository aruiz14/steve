IMAGE ?= rancher/steve:dev
PLATFORM ?= linux/$(shell go env GOARCH)

build:
	docker build -t $(IMAGE) .

import:
	k3d image import -c st-upstream $(IMAGE)

push:
	docker tag $(IMAGE) aruiz14/experiments:steve-tracing
	docker push aruiz14/experiments:steve-tracing

debug:
	env GOOS=linux make build-bin
	docker build -t $(IMAGE) --platform $(PLATFORM) -f Dockerfile.local .

build-bin:
	bash scripts/build-bin.sh

run: build
	docker run $(DOCKER_ARGS) --rm -p 8989:9080 -it -v ${HOME}/.kube:/root/.kube steve --https-listen-port 0

run-host: build
	docker run $(DOCKER_ARGS) --net=host --uts=host --rm -it -v ${HOME}/.kube:/root/.kube steve --kubeconfig /root/.kube/config --http-listen-port 8989 --https-listen-port 0

test:
	bash scripts/test.sh

validate:
	bash scripts/validate.sh