# Security Policy

## Reporting a vulnerability

Open a GitHub issue at https://github.com/scttfrdmn/substrate/issues, or for
sensitive reports use GitHub's private vulnerability reporting
("Report a vulnerability" on the Security tab).

## Module tag integrity advisory

### Poisoned tags: v0.45.1 and v0.45.2 — do not use

Both tags were re-cut (the git tag was moved to new tree content) **after** they
had already been published and fetched by downstream consumers. Go's module
checksum database (`sum.golang.org`) is an immutable transparency log: it
permanently records the hash from the *first* publication, and every consumer's
`go.sum` is verified against that record.

Because the tag content was later changed, a fresh fetch now downloads bytes
whose hash no longer matches the transparency-log record, so module verification
fails for **every** consumer using the default `GOSUMDB`:

```
verifying github.com/scttfrdmn/substrate@v0.45.2: checksum mismatch
  downloaded: h1:yvO1DwkfgjQHengH1El2gdbQX7sKIbIsVXCGYDcepjo=
  go.sum:     h1:IRsSYw2NRTiJQw1XauVvw0bqnT0TegZmjX1KNp3Hjo8=
SECURITY ERROR
```

| Tag | sum.golang.org (original, authoritative) | Current GitHub tag content |
|-----|------------------------------------------|----------------------------|
| v0.45.1 | `h1:yaEPwC78cAWh+jyJatSG3pesL3HeVfpjTC5jJcixed4=` | `h1:Y5Q7XOC87AdCiHR9IsShyWSZdPTx2sQjdVvDPbqJJV4=` |
| v0.45.2 | `h1:IRsSYw2NRTiJQw1XauVvw0bqnT0TegZmjX1KNp3Hjo8=` | `h1:yvO1DwkfgjQHengH1El2gdbQX7sKIbIsVXCGYDcepjo=` |

The `/go.mod` hashes were **not** affected — only the source tree changed.

### Scope

Verified poisoned: **v0.45.1, v0.45.2**. Verified clean (GitHub content matches
the transparency log): v0.45.0 and v0.45.3 through the current release. These two
tags are an isolated incident, not an ongoing compromise.

### Resolution

- **Consumers:** upgrade to any clean release — `v0.45.3` or later
  (`go get github.com/scttfrdmn/substrate@v0.65.0`) and regenerate `go.sum`.
- These tags are **not** being re-cut again. Re-cutting cannot satisfy the
  transparency log (the original bytes are not recoverable) and a third mutation
  would only compound the divergence. v0.45.1 and v0.45.2 are abandoned in place.
- As a temporary, **not recommended** escape hatch, a consumer pinned to a bad
  tag can set `GOFLAGS=-insecure` or `GONOSUMCHECK`/`GONOSUMDB` — but the correct
  fix is to move off the poisoned versions.

## Out-of-order tag advisory

### Void tag: v0.67.0 — do not use; skipped in versioning

`v0.67.0` was tagged out of order: it points at commit `d4a93f2` (the #303
NetworkInterface/SSM change), which is an **ancestor** of `v0.66.0` and
`v0.66.1`. As a result the tag's content is *older* than — and a strict subset
of — the `v0.66.x` releases: it predates the CFN drift-detection work (#290),
the scope/philosophy documentation, the `DescribeInstances` filter fix (#305),
and the Go 1.26.4 security toolchain bump. It was created by a stray manual
`git tag` during the #303 merge, not by the documented release process, and has
**no GitHub release and no CHANGELOG entry**.

It is **not poisoned** (the content was never mutated; its hash matches the
transparency log), but it is misleading and must not be depended on:

```
github.com/scttfrdmn/substrate v0.67.0 h1:p8nwUALuzlN6XDCKfCIQImgY28V17zqgZLi4MKjlPJ8=
github.com/scttfrdmn/substrate v0.67.0/go.mod h1:0l7emfinozw6VVLToCq2EeEm1iuMJurSws+gHkfEu6Y=
```

### Resolution

- **Consumers:** do not use `v0.67.0`. Use `v0.66.1` or later. A consumer that
  accidentally pinned `v0.67.0` should `go get github.com/scttfrdmn/substrate@latest`.
- The tag is **left in place** (per the immutability rule — `sum.golang.org` has
  already recorded it and deleting it would break anyone who fetched it). It is
  **not** re-pointed.
- **The version number 0.67.0 is burned.** The next minor release skips it and
  is `v0.68.0`.

## Prevention

Published git tags are immutable by contract. The release process
(`CLAUDE.md` → Releasing) creates a signed tag and never moves it; any mistake
in a release is corrected by cutting a new patch version, never by re-tagging.

This is now enforced server-side by GitHub repository rulesets, not just by
convention:

- **Tag ruleset** on `refs/tags/v*`: blocks tag *deletion*, *update*, and
  *non-fast-forward* — a published version tag cannot be moved or removed by
  anyone (including admins and automation).
- **Branch ruleset** on `main`: requires changes to land via pull request and
  blocks direct pushes, force-pushes, and branch deletion, with no bypass
  actors. This prevents unreviewed direct commits and stray tags-on-merge of the
  kind that produced `v0.67.0`.
