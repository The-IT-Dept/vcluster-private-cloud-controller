# Development entrypoints. CI runs the same targets.

CONTROLLER_GEN := go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.19.0

.PHONY: generate
generate: ## deepcopy methods + CRD manifests (checked in; CI verifies no drift)
	$(CONTROLLER_GEN) object paths=./api/...
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:artifacts:config=charts/tenant-syncer/crds

.PHONY: test
test:
	go vet ./...
	go test ./...

.PHONY: build
build:
	CGO_ENABLED=0 go build -o bin/tenant-syncer ./cmd

.PHONY: template-test
template-test:
	bash hack/template-test.sh
	bash hack/template-test-syncer.sh
