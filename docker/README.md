# jibs container images

Published to GitHub Container Registry on every release:

| Image | Base | Use |
|-------|------|-----|
| `ghcr.io/cresset-tools/jibs:<version>` | `scratch` | Binary-only. Lift `/jibs` into your own image. |
| `ghcr.io/cresset-tools/jibs:latest` | `scratch` | Same, tracking the latest stable release. |
| `ghcr.io/cresset-tools/jibs:<version>-alpine` | `alpine:3.22` | Runnable, musl + openssh-client. |
| `ghcr.io/cresset-tools/jibs:<version>-debian-slim` | `debian:trixie-slim` | Runnable, glibc + openssh-client. |

Stable releases also publish rolling `<major>.<minor>` and bare-variant tags
(`alpine`, `debian-slim`); prereleases publish only the exact `<version>` /
`<version>-<variant>` tags.

Both `linux/amd64` and `linux/arm64` are published as a single multi-arch
manifest, so `docker pull` / `FROM` / `COPY --from` resolve the right arch
automatically. The images are in fact the only prebuilt **aarch64 Linux**
client jibs ships — the release tarballs cover x86_64 Linux and Apple
Silicon macOS.

## Why the base image isn't runnable

The jibs client drives the system `ssh` binary (with ControlMaster) to reach
the remote host, and `scratch` has no ssh. The `alpine` / `debian-slim`
variants exist to be run: they add `openssh-client` on top of the base
binary. Use the scratch image only as a `COPY --from` source.

The embedded `jibs-server` binaries inside the client are always built for
*both* Linux arches, so either image arch can import from either remote-host
arch.

## Run an import

The container needs your SSH key (mount `~/.ssh` read-only — this also
brings your `known_hosts` and any `Host` config along) and network access to
both the SSH host and your local MySQL. With a MySQL on the docker host,
`--network host` is the simplest wiring:

```sh
docker run --rm -it \
  --network host \
  -v ~/.ssh:/root/.ssh:ro \
  -v "$PWD":/work -w /work \
  ghcr.io/cresset-tools/jibs:alpine \
  import config.jibs \
    --host user@prod.example.com \
    --remote-mysql 'mysql://ro_user:...@127.0.0.1:3306/production' \
    --local-mysql 'mysql://root:...@127.0.0.1:3306/imported' \
    --parallel 4
```

With a compose-managed MySQL instead, drop `--network host`, attach the
container to the compose network, and point `--local-mysql` at the service
name. Unknown host keys are accepted-and-pinned by default (`accept-new`);
pass `--strict-host-key-checking` to require a pre-populated `known_hosts`,
e.g. in CI.

## Copy the binary into your own image

The `scratch` image exists to be a source for `COPY --from` — the binary is
static (musl, no libc), so it runs in whatever stage you copy it into, as
long as an `ssh` client is on `PATH` at runtime:

```dockerfile
FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends openssh-client \
    && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/cresset-tools/jibs:latest /jibs /usr/local/bin/
```

## Provenance

Every pushed manifest carries a GitHub build-provenance attestation. Verify
with:

```sh
gh attestation verify oci://ghcr.io/cresset-tools/jibs:latest --repo cresset-tools/jibs
```
