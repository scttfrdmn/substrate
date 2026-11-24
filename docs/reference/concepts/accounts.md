# Which account a request is attributed to

Most plugins scope a resource to the account of the request that created it, and
every ARN substrate mints names one, so "which account is this?" is a question with a
single answer. Substrate resolves it in one place and in this order:

| Source | When it applies |
|---|---|
| `123456789012` | Always, as the starting point. AWS's documented example account. |
| `account.default` in `substrate.yaml` | Whenever it is set and 12 digits. Set it to whatever your fixtures assert on. |
| A `CredentialRegistry` entry | When the request is signed with an access key the registry holds. Wire one with `credentials:` in `substrate.yaml` or `ServerOptions.Credentials`. |
| An STS session record | When the request is signed with credentials `AssumeRole` minted. |

Later rows win. The order is what it is because each row knows strictly more than the
one above it: a config file knows the deployment, a registry knows which key belongs
to which account, and only the session record knows the account a cross-account
`AssumeRole` landed in — a temporary credential's account is recorded when the session
is minted and appears nowhere on the wire.

An **unsigned** request is attributed to the resolved default. That is deliberate:
`VerifySigV4` passes a request carrying no `Authorization` header, so an unsigned
caller reaches its plugin as the default account even against a server with a
credential registry wired.

## There is no second account for free

Until [#734](https://github.com/scttfrdmn/substrate/issues/734), the account came
from the *shape of the access key*: a key beginning with `AKIA` was attributed to
`123456789012`, and everything else — substrate's own documented `test`/`test`, an
unsigned request, an `ASIA` session key — to `000000000000`. One server served two
accounts, chosen by which of two documented credentials the client happened to pick,
and nothing on the wire told a caller which one they had.

`000000000000` no longer exists anywhere in substrate. A test asserts it: every
non-test Go file is parsed and any string literal carrying an account-shaped run of
twelve digits fails the build, with one exemption for the declaration of the default
itself. A run bounded by anything other than `:`, `/` or the end of the string is not
an account — which is what keeps `shardId-000000000000` and the like out of it.

**If a fixture asserts on `000000000000`, it now sees `123456789012`.** Two things
follow. Every ARN returned to an unsigned or non-`AKIA` caller changes account. And
because several plugins prefix their state keys with the account —
`table:{account}/{region}/{name}`, `instance:{account}/{id}` — **persisted SQLite state written
under the old account is unreachable** after upgrading. Re-seed it, or set
`account.default: "000000000000"` to read it back.

## IAM entities belong to an account

Every IAM state key carries the account it belongs to —
`user:{account}/{name}`, `role_policies:{account}/{name}`,
`group_inline:{account}/{group}:{policy}` — the same `{kind}:{account}/{rest}` shape
DynamoDB and the other account-aware plugins already used. Before
[#737](https://github.com/scttfrdmn/substrate/issues/737) they did not, and an IAM
entity belonged to the emulator rather than to an account. Three things follow, all of
them now observable:

- **Two accounts can hold the same name.** `CreateRole deploy` in a second account
  answered `EntityAlreadyExists`; it now succeeds, and the two roles are distinct
  records with distinct ARNs.
- **A listing reports one account.** `ListUsers`, `ListRoles`, `ListGroups`,
  `ListInstanceProfiles` and `ListPolicies` with `Scope=Local` scan the caller's
  account only. `ListPolicies` in particular used to scan every account's policies.
- **A principal resolves against its own account.** `resolveIAMEntity` takes the
  account from the principal ARN, not from the request, so a cross-account principal
  is evaluated against its own account's policies rather than a same-named entity in
  the account the request resolved to. `AssumeRole` likewise reads the role named by
  `RoleArn` from *that* ARN's account, which is the case cross-account role assumption
  exists for.

One IAM key has no account and cannot have one: `accesskey:{id}`. An access key ID is
what *determines* an account, so a signed request looks the record up before any
account is known. The owning account is a field on the record instead. A record written
before this release has that field empty and falls back to the account the request
resolved to — which is what it always used, so nothing that worked stops working.

Because the keys changed shape, **IAM state written before this release is
unreachable**: a snapshot or exported fixture holding `user:alice` is not read by a
handler now looking for `user:123456789012/alice`. Re-seed it through the API. This is
the same class of break as the account-default change above, for the same reason.

## Configuring the registry from `substrate.yaml`

```yaml
account:
  default: "123456789012"
credentials:
  enabled: true              # build the registry; without it there is none
  verify_signatures: true    # the default — see below
  entries:
    - access_key_id: "AKIAEXAMPLE00000001"
      secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
      account_id: "111122223333"
```

`enabled: false` — the shipped default — wires no registry at all, so every caller
resolves to `account.default` and no signature is checked. Nothing about an existing
deployment changes.

`verify_signatures` defaults to **true**, because this section has documented `enabled`
as *"enable SigV4 signature verification"* since it was written and `enabled: true`
alone has to keep meaning that. Set it to `false` for the combination
[#630](https://github.com/scttfrdmn/substrate/issues/630) opened up: resolve an account
per access key without authenticating anyone. That is the setting a multi-account test
usually wants, since the registry is there to answer *which account* and no call needs a
real signature.

`account_id` is optional and falls back to `account.default`, so an entry can exist only
to make a key signable. `secret_access_key` is required while verification is on and
unused otherwise. A malformed entry — an empty `access_key_id`, an `account_id` that is
not twelve digits, a duplicated key — is a startup error rather than a silently dropped
row, and it is checked whether or not the section is enabled, so a typo does not stay
hidden until someone flips the flag.

Every registry — configured or built in-process — is seeded with the three credentials
substrate's own documentation tells a caller to use, all in `account.default`:
`AKIATEST12345678901`, `test`/`test` (README, endpoint configuration) and
`AKIAIOSFODNN7EXAMPLE` (testing guide, `test/e2e`). Without that, turning verification
on would make the quickstart wrong — a consumer would have followed the documentation
exactly and been answered `InvalidClientTokenId` 403. Reusing one of those access key
IDs in `entries:` replaces it, which is how a test moves the built-in key into another
account.

`SIGHUP` re-reads the file and adds any new `entries:` to the live registry. `enabled`
and `verify_signatures` are read once at startup; changing either logs a warning saying
so and takes effect on restart.

**Cross-service IAM authorization has no flag, and there is nothing to enable.**
`substrate server` always builds an `AuthController`. Enforcement is opt-in by
*existence* instead: a request is checked against attached policies, inline policies and
a permission boundary only once its access key resolves to an IAM entity substrate
actually holds, and a caller that never created a user or role is authorized against
nothing. An `auth.enabled` flag would overpromise in both directions — it could not turn
enforcement off for a principal that exists, because IAM's own authorization resolves
against state directly rather than through the controller.

## Attributing accounts and enforcing signatures are separate

`ServerOptions.Credentials` answers *which account does this access key belong to*;
`ServerOptions.VerifySignatures` answers *is this signature valid*. One field used to
mean both ([#630](https://github.com/scttfrdmn/substrate/issues/630)), so wiring a
registry in order to reach a second account also refused every credential substrate
documents — `test`/`test` and `AKIAIOSFODNN7EXAMPLE` were in no registry, and an
unregistered key is `InvalidClientTokenId` 403. All four combinations are expressible
now, and the one that did not exist before — a registry with verification off — is
what every test server uses. Both of those credentials are seeded into every registry
as of [#736](https://github.com/scttfrdmn/substrate/issues/736), so signing with one is
no longer refused either way.

One consequence worth knowing, because it decides what `GetCallerIdentity` reports. A
key the registry holds but IAM does not know names a principal only when its signature
was **verified**. A verified key has proven the caller holds the secret, so naming it
is a statement about someone substrate authenticated; a key merely present in a table
has proven nothing, and naming it would flip `GetCallerIdentity`'s ARN off `:root` and
turn `GetUser` from a validation error into a `NoSuchEntity` lookup for a server that
wired a registry only to attribute accounts. Either way the synthesized ARN names the
*key*, not a user, so it resolves to no policies and authorizes nothing.

`VerifySignatures` with no registry has no key material to check against. The server
logs a warning and runs with verification off rather than refusing every signed
request. Refusing at construction would be better, but `NewServer` returns no error and
a panic in an emulator library is worse than either.

In tests, `StartTestServer` wires a registry with verification off, so
`TestServer.RegisterAccount` works on any server it returns; ask for enforcement with
`StartTestServer(t, WithSignatureVerification())`.
