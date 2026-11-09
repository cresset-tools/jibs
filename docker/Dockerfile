# syntax=docker/dockerfile:1

# jibs base image: static musl client binary on `scratch`, following bougie's
# docker pattern. This image is binary-only — the jibs client shells out to
# the system `ssh` binary at import time, and scratch has none, so it exists
# to be a `COPY --from` source. The *runnable* images (alpine / debian-slim,
# built from docker/Dockerfile.extra) add openssh-client on top.
#
# The build stage compiles natively per-arch (amd64 and arm64 legs each run
# on a native runner — see .github/workflows/build-docker.yml), which is also
# what gives the image an aarch64 Linux client that the dist release targets
# don't ship. The embedded jibs-server binaries are always cross-built for
# BOTH Linux arches with zig (mirroring scripts/build.sh and
# .github/build-setup.yml), so either image arch can deploy to either
# remote-host arch.

ARG RUST_VERSION=1.96

FROM rust:${RUST_VERSION}-alpine AS build

# C toolchain for the bundled MySQL client (mysql_common -> cc/cmake) and
# zstd; zig is the cross C toolchain for the embedded servers' foreign arch.
RUN apk add --no-cache musl-dev make cmake clang linux-headers zig

# Own layer so source changes don't recompile cargo-zigbuild.
RUN cargo install --locked cargo-zigbuild

WORKDIR /src
COPY . .

# Everything in one RUN so the servers exist in the (cache-mounted) target
# dir when build.rs embeds them. JIBS_REQUIRE_EMBEDDED_SERVER=1 turns a
# missing server binary from a build warning into a hard error — same
# guarantee the release builds enforce.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    set -eux; \
    rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl; \
    cargo zigbuild -p jibs_server --release \
        --target x86_64-unknown-linux-musl \
        --target aarch64-unknown-linux-musl; \
    JIBS_REQUIRE_EMBEDDED_SERVER=1 cargo build --release -p jibs; \
    cp target/release/jibs /jibs

FROM scratch
COPY --from=build /jibs /
# Runnable only where an `ssh` binary exists — see the variant images.
ENTRYPOINT ["/jibs"]
