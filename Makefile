.PHONY: help
help: ## - Show this help message
	@printf "\033[32m\xE2\x9c\x93 usage: make [target]\n\n\033[0m"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: build-common
build-common: ## - execute build common tasks clean and mod tidy
	@ go version
	@ go clean
	@ go mod download && go mod tidy
	@ go mod verify

.PHONY: build
build: build-common ## - build a debug binary to the current platform (windows, linux or darwin(mac))
	@ echo building
	@ go build -v ./...
	@ echo "done"

.PHONY: format-check
format-check: ## - check files format using gofmt
	@RESULT=$$(gofmt -l .); \
	if [ -n "$$RESULT" ]; then \
		echo "You need to run \"gofmt -w ./\" to fix your formatting."; \
		echo "$$RESULT"; \
		exit 1; \
	fi

.PHONY: format
format: ## - apply golang file format using gofmt
	@ gofmt -w ./

.PHONY: test
test: build-common ## - execute go test command for unit and mocked tests
	@ go list ./... | grep -v /test | xargs -I% go test % -v -cover -race

.PHONY: integration-test
integration-test: ## - execute go test command for integration tests (aws credentials needed)
	@ go test -v -cover -race ./test

.PHONY: scan
scan: ## - execute static code analysis
	@ gosec -fmt=sarif -out=results.sarif -exclude-dir=internal -exclude-dir=vendor -severity=high ./...

.PHONY: lint
lint: ## - runs golangci-lint
	@ golangci-lint run --timeout=600s --verbose
