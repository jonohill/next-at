UNAME := $(shell uname | tr 'A-Z' 'a-z')
ARCH := $(shell uname -m)

ifeq ($(UNAME), linux)
	export PATH := /opt/zig:$(PATH)
endif

build: build-x86_64-unknown-linux-musl build-aarch64-unknown-linux-musl

install-zig-linux:
	set -e; \
	url=$$(curl -sL https://ziglang.org/download/index.json | jq -r --arg arch "${ARCH}-linux" '.master[$$arch].tarball'); \
	mkdir -p /opt/zig; \
	curl -sL "$$url" | tar -xJf - -C /opt/zig --strip-components=1

install-zig-darwin:
	brew install zig

install-zigbuild:
	cargo install cargo-zigbuild

install-deps: install-zig-${UNAME} install-zigbuild

build-%: install-deps
	rustup target add $*
	export RUSTFLAGS="-C link-arg=-s"
	cargo zigbuild --target $* --release

TAG ?= next-at

docker: build
	mkdir -p target/linux/amd64/release
	mkdir -p target/linux/arm64/release
	rm target/linux/amd64/release/next-at-rs || true
	rm target/linux/arm64/release/next-at-rs || true
	cp target/x86_64-unknown-linux-musl/release/next-at-rs target/linux/amd64/release/next-at-rs
	cp target/aarch64-unknown-linux-musl/release/next-at-rs target/linux/arm64/release/next-at-rs

	docker build -t ${TAG} .
