.PHONY: build build-release test test-rust test-python test-ui test-integration test-e2e \
       lint fmt clean serve dev run compile ui docker docker-dev docker-test \
       backfill extension install-extension

# ── Build ────────────────────────────────────────────────────────────────────

build:                          ## Build all Rust crates (debug)
	cargo build --workspace

build-release:                  ## Build optimized release binary
	cargo build --release --bin conduit

ui:                             ## Build React UI
	cd conduit-ui && npm install && npm run build

all: build-release ui           ## Build everything (Rust + UI)

# ── Test ─────────────────────────────────────────────────────────────────────

test: test-rust test-python test-ui ## Run all tests

test-rust:                      ## Run Rust workspace tests
	./scripts/test.sh

test-python:                    ## Run Python SDK tests
	cd sdk/python && python3 -m pytest tests/ -v

test-ui:                        ## Run React UI tests
	cd conduit-ui && npm install --silent && npm test

test-integration:               ## Run SQL provider tests (requires live databases)
	cargo test --package conduit-providers --test sql_providers_test -- --ignored

test-e2e:                       ## Run Docker Compose E2E tests
	docker compose --profile e2e up --build --abort-on-container-exit --exit-code-from e2e-test

lint:                           ## Type-check and lint
	cargo clippy --workspace -- -W warnings
	cd conduit-vscode && npm run lint

fmt:                            ## Format all code
	cargo fmt --all
	cd conduit-ui && npx prettier --write src/

# ── Run ──────────────────────────────────────────────────────────────────────

serve: build-release ui         ## Build and start the server (API + UI)
	CONDUIT_UI_DIR=./conduit-ui/dist ./target/release/conduit serve \
		--host 0.0.0.0 --port 9091 --dags-path ./dags/

dev:                            ## Start in dev mode (hot reload UI)
	docker compose --profile dev up --build

compile:                        ## Compile DAGs and show results
	./target/release/conduit compile ./dags/

run:                            ## Run a DAG (usage: make run DAG=demo_pipeline)
	PYTHONPATH=./dags:./sdk/python:$$PYTHONPATH \
		./target/release/conduit run $(DAG) --dags-path ./dags/

backfill:                       ## Run a backfill (usage: make backfill DAG=demo_pipeline START=2026-03-01 END=2026-03-08)
	PYTHONPATH=./dags:./sdk/python:$$PYTHONPATH \
		./target/release/conduit backfill $(DAG) \
		--start $(START) --end $(END) \
		--granularity $(or $(GRANULARITY),day) \
		--dags-path ./dags/

# ── Docker ───────────────────────────────────────────────────────────────────

docker:                         ## Build and start production Docker
	docker compose up --build -d

docker-dev:                     ## Start dev mode with hot reload
	docker compose --profile dev up --build

docker-test:                    ## Run tests inside Docker
	docker compose --profile test run --rm test

docker-stop:                    ## Stop all Docker containers
	docker compose down

# ── VS Code Extension ───────────────────────────────────────────────────────

extension:                      ## Build and package the VS Code extension
	cd conduit-vscode && npm install && npm run compile && \
		npx @vscode/vsce package --no-dependencies

install-extension: extension    ## Install the VS Code extension locally
	code --install-extension conduit-vscode/conduit-vscode-0.1.0.vsix --force

# ── Cleanup ──────────────────────────────────────────────────────────────────

clean:                          ## Remove build artifacts
	cargo clean
	rm -rf conduit-ui/dist conduit-ui/node_modules
	rm -rf conduit-vscode/dist conduit-vscode/node_modules conduit-vscode/*.vsix
	rm -rf logs/ .conduit/

# ── Help ─────────────────────────────────────────────────────────────────────

help:                           ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
