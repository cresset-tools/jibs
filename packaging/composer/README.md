# cresset/jibs

[jibs](https://github.com/cresset-tools/jibs) copies a production MySQL
database into a local development environment: selectively (follow foreign
keys from a handful of rows instead of dumping everything), anonymized on
the production side before data leaves the host, streamed compressed over
SSH into parallel `LOAD DATA` workers.

```bash
composer require --dev cresset/jibs
vendor/bin/jibs check shop.jibs
vendor/bin/jibs import shop.jibs --host deploy@prod --dry-run
```

Or install it globally:

```bash
composer global require cresset/jibs
```

jibs is a single Rust binary. This package ships **only a thin PHP
launcher** — no Rust source. On first run it downloads the prebuilt `jibs`
binary matching this package's version for your platform, caches it
(`$XDG_CACHE_HOME/jibs/<version>/`), verifies its SHA-256, and execs it. The
package version maps 1:1 to the jibs release: `cresset/jibs:0.1.0` runs
`jibs-v0.1.0`.

Prebuilt targets: Linux x86_64 (gnu/musl) and macOS arm64. There are no
Windows binaries (jibs drives the system OpenSSH binary) — use WSL.
`ext-curl` is recommended.

This is the Composer distribution branch of the jibs repo — it is generated
from `packaging/composer/` on `main` and contains no application code of its
own. EUPL-1.2.
