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

### Prevention

Published git tags are immutable by contract. The release process
(`CLAUDE.md` → Releasing) creates a signed tag and never moves it; any mistake
in a release is corrected by cutting a new patch version, never by re-tagging.
