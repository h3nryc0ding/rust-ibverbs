LOCAL_HOST = 141.76.47.8
REMOTE_HOST = 141.76.47.9
REMOTE_USER = richter
REMOTE_DIR = /home/richter/MemConnect

CARGO = /home/richter/.cargo/bin/cargo
FLAGS = --release

.DEFAULT_GOAL := help
.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

build: ./**/src Cargo.lock
	$(CARGO) build

.PHONY: run-local-server
run-local-server:
	$(CARGO) run

.PHONY: run-local-client
run-local-client:
	$(CARGO) run $(REMOTE_HOST)

sync: ./**/src Cargo.lock
	rsync -avz --filter=':- .gitignore' . $(REMOTE_USER)@$(REMOTE_HOST):$(REMOTE_DIR)

.PHONY: run-remote-server
run-remote-server: sync
	ssh -t $(REMOTE_USER)@$(REMOTE_HOST) 'cd $(REMOTE_DIR) && $(CARGO) run $(FLAGS)'

.PHONY: run-remote-client
run-remote-client: sync
	ssh -t $(REMOTE_USER)@$(REMOTE_HOST) 'cd $(REMOTE_DIR) && $(CARGO) run $(FLAGS) -- $(LOCAL_HOST)'