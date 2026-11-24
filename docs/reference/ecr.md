# ECR

**Endpoint:** `ecr.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AmazonEC2ContainerRegistry_V20150921.{Op}`)

`_V1_1_0` was documented here previously; `_V20150921` is what the ECR model
declares (#739). Both reduce to `ec2containerregistry`, so substrate routes either.

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateRepository | `tags` is [an array of `Tag` objects](#ecr-s-tags-is-an-array-with-capitalized-members); `imageTagMutability` defaults to `MUTABLE` and a value outside the published set is refused |
| DescribeRepositories | Reports [the nine published `Repository` members](#a-repository-response-carries-the-nine-published-members) and no others; a `repositoryNames` entry that names nothing is [refused, not skipped](#every-ecr-refusal-is-a-400); the registry-wide form [pages](#the-three-ecr-listings-page-three-different-ways), and the named form cannot |
| DeleteRepository | Reports the deleted repository in the same shape; a repository holding images needs [`force`](#every-ecr-refusal-is-a-400), and its images go with it |
| GetAuthorizationToken | Returns base64("AWS:password") |
| PutImage | The digest is [the SHA-256 of the manifest](#an-ecr-digest-is-the-sha-256-of-its-manifest); `imageManifest` is required; a repeat that changes nothing is `ImageAlreadyExistsException`, a supplied `imageDigest` that is not the manifest's is `ImageDigestDoesNotMatchException`, and a tag that would move on an [immutable repository](#an-immutable-tag-cannot-move-and-an-untagged-image-is-listed) is `ImageTagAlreadyExistsException`; an absent repository governed by a `CREATE_ON_PUSH` template is [created from it](#pull-through-cache-rules-and-repository-creation-templates) |
| BatchGetImage | Refuses an unknown repository, as do `BatchDeleteImage`, `DescribeImages` and `ListImages` — except one under a pull through cache rule, which it [creates](#pull-through-cache-rules-and-repository-creation-templates); answers the image under the tag it was asked by |
| BatchDeleteImage | [By tag removes that tag](#an-ecr-digest-is-the-sha-256-of-its-manifest), and the image only with its last; by digest removes the image and every tag, one `imageId` per tag; an entry that matches nothing is reported in `failures` |
| DescribeImages | [Pages](#the-three-ecr-listings-page-three-different-ways) unless `imageIds` is given, which excludes both members; an `imageIds` entry that names nothing is refused with `ImageNotFoundException`; describes [untagged images](#an-immutable-tag-cannot-move-and-an-untagged-image-is-listed) and honors `filter` |
| ListImages | Reports [one entry per digest-and-tag pair](#the-three-ecr-listings-page-three-different-ways), sorted, and pages with no exclusion on either member; an [untagged image](#an-immutable-tag-cannot-move-and-an-untagged-image-is-listed) is one entry with its digest alone, and `filter` is honored |
| TagResource | `tags` is [an array of `Tag` objects](#ecr-s-tags-is-an-array-with-capitalized-members); `resourceArn` is [parsed whole and must name the caller's registry](#a-repository-arn-is-resolved-where-it-points); a namespaced name keeps its `/` |
| UntagResource | `tagKeys` is an array of strings, as published; `resourceArn` as for `TagResource` |
| ListTagsForResource | Reports the published array, ordered by key; an untagged repository reports `[]`; `resourceArn` as for `TagResource` |
| CreatePullThroughCacheRule | [Pull through cache](#pull-through-cache-rules-and-repository-creation-templates): `upstreamRegistryUrl` must be on the published list (`UnsupportedUpstreamRegistryException`); an authenticated upstream needs a `credentialArn` and a cross-account ECR upstream a `customRoleArn` (`ValidationException`); a taken prefix is `PullThroughCacheRuleAlreadyExistsException` |
| DescribePullThroughCacheRules | All rules in prefix order, or those `ecrRepositoryPrefixes` names — an unknown one is `PullThroughCacheRuleNotFoundException`; both forms page |
| UpdatePullThroughCacheRule | Updates `credentialArn` and `customRoleArn`; an omitted one keeps its value; `PullThroughCacheRuleNotFoundException` |
| DeletePullThroughCacheRule | Echoes the rule; repositories it created stay; `PullThroughCacheRuleNotFoundException` |
| ValidatePullThroughCacheRule | `isValid: true` unless [seeded](#pull-through-cache-rules-and-repository-creation-templates) with `POST /v1/ecr/pull-through-cache-validation`; no upstream is contacted |
| CreateRepositoryCreationTemplate | `prefix` and `appliedFor` required; defaults `MUTABLE` and `AES256`; a taken prefix is `TemplateAlreadyExistsException` |
| DescribeRepositoryCreationTemplates | All templates in prefix order, or those `prefixes` names — an unknown one is omitted, since the page publishes no not-found code; pages |
| UpdateRepositoryCreationTemplate | Merges: an omitted member keeps its value; `TemplateNotFoundException` |
| DeleteRepositoryCreationTemplate | Echoes the template; `TemplateNotFoundException` |
| SetRepositoryPolicy | Stores `policyText` verbatim and echoes it with `registryId`; nothing parses it, so an unparseable document is accepted and no ECR request is ever authorized or refused by one. `force` is unread. `RepositoryNotFoundException` [at 400](#every-ecr-refusal-is-a-400) |
| GetRepositoryPolicy | `RepositoryPolicyNotFoundException` when the repository exists and no policy has been set — distinct from `RepositoryNotFoundException`, so a caller can tell the two apart |
| DeleteRepositoryPolicy | Echoes the `policyText` it removed, as published; `RepositoryPolicyNotFoundException` when there is none to delete, so deleting twice refuses the second time |
| PutLifecyclePolicy | Stores `lifecyclePolicyText` verbatim and echoes it. The rules are never evaluated — no image expires here, whatever the policy says, and `PreviewLifecyclePolicy` is unmodelled |
| GetLifecyclePolicy | `LifecyclePolicyNotFoundException` when none has been put, on the same split as the repository policy |

## ECR's `tags` is an array with capitalized members

`tags` is **an array of `Tag` objects** on `CreateRepository`'s and `TagResource`'s requests and on
`ListTagsForResource`'s response, and each entry's members are the capitalized `Key` and `Value`:

```json
{"tags": [{"Key": "env", "Value": "prod"}]}
```

That casing contradicts every other member in the service — `repositoryName`, `resourceArn`,
`tagKeys` are all lowerCamelCase — so it reads like a defect and is not one. `API_Tag` publishes
`Key` and `Value` with a capital, both `Required: Yes`, and the Request Syntax of
`API_CreateRepository` and `API_TagResource` and the Response Syntax and sample response of
`API_ListTagsForResource` all spell them that way. It is recorded here because a future reader will
otherwise "fix" it.

Substrate decoded and rendered a JSON *object* at all three sites until #1017. Since an array does
not unmarshal into a `map[string]string`, and each of these handlers refuses a body it cannot
unmarshal, the observable result was not a dropped tag: `aws ecr create-repository --tags
Key=env,Value=prod` answered `InvalidParameterException`/400, and ECR tagging was unusable from any
SDK. The round-trip rule #765 established could not catch it, because both halves of the round trip
shared the wrong shape — only the published Request and Response Syntax settles a shape question,
which is why the tests assert raw JSON.

Three readings are substrate's rather than AWS's:

- **The order** is lexicographic by key. `API_ListTagsForResource` publishes no order and its sample
  response carries one entry; sorting is what makes a recorded run replay byte-identically (#862),
  and ranging a Go map put map order on the wire.
- **An empty set is `[]`**, not `null` and not an absent member, per the rule #938 established for
  the tagging API: an SDK decoding `null` into a list cannot tell "no tags" from "the service did
  not answer".
- **An entry with no `Key` is refused** with `InvalidParameterException`/400, the code all three
  operations publish. An empty **value** is accepted, because `API_CreateRepository` describes a tag
  as "a key and an *optional* value" where `API_Tag` marks `Value` `Required: Yes` — the page
  contradicts itself, and the narrower reading refuses only what both sentences agree is required.

Storage is unchanged: the record holds a `map[string]string`, which is what the Resource Groups
Tagging API's `mergeResourceTags` arm and the CloudFormation tag stamp operate on, so those paths
needed no edit. A tag written through `TagResources` is reported by `ListTagsForResource` and one
written through ECR's own `TagResource` is reported by `GetResources` — #765 in both directions, which
is what proves the two halves now agree about the wire as well as about the record.

## A repository response carries the nine published members

`API_Repository` publishes nine members, all `Required: No`, and substrate answers eight of them —
`repositoryName`, `repositoryArn`, `registryId`, `repositoryUri`, `createdAt`, `imageTagMutability`,
`imageScanningConfiguration` and `encryptionConfiguration`. The ninth,
`imageTagMutabilityExclusionFilters`, and `encryptionConfiguration.kmsKey` are **absent** rather than
present and empty unless the repository has them, per #1013's rule; AWS's own `CreateRepository`
sample response omits both, so the absences are shapes the page publishes. `CreateRepository` reads
neither member; a repository created from a [creation template](#pull-through-cache-rules-and-repository-creation-templates)
carries the template's.

Until #1090 the persisted record was marshalled straight onto the wire at all three sites that answer
a repository, so each of them also reported fields that are substrate's own:

| Member answered | Published by ECR | Where AWS puts it |
|-----------------|------------------|-------------------|
| `AccountID`, `Region` | No — on every response, since neither was optional | `registryId` and the Region embedded in `repositoryArn` |
| `Tags` | No | `ListTagsForResource` |
| `LifecyclePolicy` | No | `GetLifecyclePolicy` |
| `RepositoryPolicy` | No | `GetRepositoryPolicy` |
| `ever_tagged` | No — substrate's #938 bookkeeping flag | nowhere; it is not an AWS concept |

The two policies were the worst of the set, because a `DescribeRepositories` response publishes no
policy member at all — a caller inspecting a repository saw a field name that matches no page. The
repository shape is now a projection (`ecr_wire.go`) rather than the record itself, which is the
pattern #529 established for API Gateway v1 and #1013 repeated for DynamoDB: a state record grows
fields for substrate's own bookkeeping, and a projection is what stops the next one from reaching a
response. The stored record is unchanged, so a recorded run still replays.

`createdAt` is a **JSON number**, not an RFC3339 string. ECR speaks
`application/x-amz-json-1.1`, whose timestamps are epoch seconds — AWS's sample response answers
`1.563223656E9` — and the SDK v2 decoder calls `ParseEpochSeconds` on a timestamp member, which a
quoted string does not satisfy. The persisted `time.Time` rendered as a string until #1090, so an SDK
caller could not decode the response at all.

`imageTagMutability` is now decoded on `CreateRepository`, stored, and reported from all three sites.
An omitted member takes `MUTABLE`, which `API_CreateRepository` publishes in as many words ("If this
parameter is omitted, the default setting of `MUTABLE` will be used"), and a record written before
#1090 is read the same way, because the member did not exist then. A value outside the published set
`MUTABLE | IMMUTABLE | IMMUTABLE_WITH_EXCLUSION | MUTABLE_WITH_EXCLUSION` is refused with
`InvalidParameterException`/400 — the code the page publishes — because the member is reported back,
so accepting one would put an unpublished string on the wire under a name whose Valid Values are
published. The two `_WITH_EXCLUSION` forms are accepted even though the filters they accompany are
unmodelled: those filters are `Required: No`, so a request naming one without them is a request AWS
accepts, and refusing it would be substrate inventing a bound.

Substrate acts on the setting since #1379: a push that would move a tag on an immutable repository
is refused; see [An immutable tag cannot move, and an untagged image is
listed](#an-immutable-tag-cannot-move-and-an-untagged-image-is-listed).

## Every ECR refusal is a 400

Every ECR operation page publishes exactly one status above 400 — `ServerException` at 500, which
substrate never answers — so every refusal an ECR handler can reach is a 400. That was read off the
pages one at a time rather than generalised from one of them: `API_CreateRepository`,
`API_DescribeRepositories`, `API_DeleteRepository`, `API_GetLifecyclePolicy`,
`API_GetRepositoryPolicy`, `API_ListImages`, `API_DescribeImages`, `API_BatchGetImage` and
`API_BatchDeleteImage` each publish their errors at 400 and nothing else below 500.

Until #1090 four of substrate's five ECR codes carried a status no page publishes, at twelve sites:

| Code | Was | Published | Sites |
|------|-----|-----------|-------|
| `RepositoryAlreadyExistsException` | 409 | 400 | 1 |
| `RepositoryNotFoundException` | 404 | 400 | 8 |
| `RepositoryPolicyNotFoundException` | 404 | 400 | 2 |
| `LifecyclePolicyNotFoundException` | 404 | 400 | 1 |

The codes were right, which is why no test noticed: `ecr_plugin_test.go` asserted that a refusal
happened and which code it carried, never its status. But the status is the part a consumer branches
on before it has parsed a body — an SDK's retry classifier reads it, and 409 is what
CloudFormation's own create-exists probe looks for — so every caller keying on the status was told
something the service never says. `InvalidParameterException`/400 was already right at the
twenty-seven sites that answer it.

Two published refusals also had no site they could fire from:

- **`RepositoryNotFoundException` on the four image operations.** `ListImages`, `DescribeImages`,
  `BatchGetImage` and `BatchDeleteImage` each read the repository's tag index and never the
  repository record, so a name that addresses nothing read as an empty index and each answered 200
  with an empty result — indistinguishable from a repository that exists and holds no images.
- **`RepositoryNotEmptyException` on `DeleteRepository`.** `force` was decoded into a field nothing
  read, so a repository full of images was deleted silently. A repository's contents are its image
  records, the set `ListImages` enumerates, so one holding only [untagged
  images](#an-immutable-tag-cannot-move-and-an-untagged-image-is-listed) is not empty and is refused
  without `force` (#1112). Until #1112 emptiness was measured by the tag index, so such a repository
  was deleted without `force` and its untagged images were left behind under their digests. A forced
  delete removes every image, tagged or not — the index used to outlive the repository, so a name
  re-created after a delete reported the previous repository's images.

`DescribeRepositories` is the third site where the refusal could not fire, and the reading there is
narrower: a name the **caller** supplied is answered for or refused, while a name read out of
substrate's own index is skipped, because a missing record there is an internal inconsistency rather
than a caller's mistake. Before #1090 every miss was dropped from the list, so a request naming one
real and one imaginary repository answered 200 with a single entry.

## An ECR digest is the SHA-256 of its manifest

`PutImage` answers `imageDigest` as `sha256:` and the hex SHA-256 of the `imageManifest` bytes,
exactly as pushed ([#1283](https://github.com/scttfrdmn/substrate/issues/1283)). A digest names
content, so a caller can verify a push by hashing the manifest it sent, and two pushes of
byte-identical manifests address **one** image. Substrate minted an unrelated value until #1283, so
every push stored a new image, whatever its manifest.

What follows from that, each from `API_PutImage` and `API_BatchDeleteImage`:

| Push or delete | Answer |
|---|---|
| No `imageManifest`, or an empty one (Required, minimum length 1) | `InvalidParameterException`/400. This is also why manifest-less pushes cannot collide: there is no empty manifest to hash. |
| A supplied `imageDigest` that is not the manifest's | `ImageDigestDoesNotMatchException`/400, and nothing is stored. A matching one is accepted. |
| The same manifest under a new tag | The one image gains the tag. `ListImages` reports it once per tag under one digest, and `DescribeImages` once, with both tags. |
| The same manifest under a tag that already names it, or with no tag | `ImageAlreadyExistsException`/400: "there were no changes to the manifest or image tag after the last push". |
| Another manifest under an existing tag | The tag moves to the new image, unless the repository is [immutable](#an-immutable-tag-cannot-move-and-an-untagged-image-is-listed), where it is `ImageTagAlreadyExistsException`/400. |
| `BatchDeleteImage` by tag | Removes that tag. The image is deleted with its last tag. |
| `BatchDeleteImage` by digest | Removes the image and every tag, answering one `imageId` per tag, as the published sample does. A digest naming no image is a `failures` entry. |

`BatchGetImage` answers an image under the tag it was asked by. Asked by digest, it answers the tag
the image was first pushed under.

A run recorded before #1283, replayed after it, diverges at its first `PutImage` that omitted
`imageDigest`: the replay answers the manifest's digest where the recording holds a minted one, so a
recorded read that names the minted digest finds nothing.

## The three ECR listings page three different ways

`DescribeRepositories`, `DescribeImages` and `ListImages` each publish `maxResults` (Valid Range 1 to
1000, and "If this parameter is not used, then … returns up to 100 results") and an opaque
`nextToken`. Substrate decoded neither on any of the three, so every request answered the whole
listing in one page. A caller written against the published contract — send `maxResults`, follow
`nextToken` until it is absent — got everything at once and could not tell, because one full page is
a well-formed answer.

The three pages were read one at a time rather than the rule being taken from the first, and they do
not agree about the mutual exclusion. That disagreement is why substrate declares the exclusion per
operation instead of sharing one guard:

| Operation | Published exclusion |
|-----------|---------------------|
| `DescribeRepositories` | Both members: "This option cannot be used when you specify repositories with `repositoryNames`." |
| `DescribeImages` | Both members: "This option cannot be used when you specify images with `imageIds`." |
| `ListImages` | None. Its `filter` member selects rather than enumerates, so a filtered listing still pages. |

`maxResults` outside 1 to 1000 is **refused**, not clamped: a page of 1000 does not tell a caller who
asked for 5000 that they misread the contract. The cursor is the base64 offset of
`offset_pagination_token.go`, so a `nextToken` substrate never issued is refused rather than silently
answering page one (#915), while an offset past the end of a listing that has since shrunk clamps to
a final empty page. `InvalidParameterException`/400 is the code for all three of those, because it is
the only parameter-fault code any of the three pages publishes — there is no ECR equivalent of KMS's
`InvalidMarkerException`.

Two further divergences were found and fixed in the same change, because an offset cursor is only
meaningful over a listing with settled membership and a stable order, and neither held:

- **`ListImages` answers one entry per image ID, not per image.** Substrate de-duplicated by digest
  and kept whichever tag Go's map iteration yielded first, so an image carrying two tags was
  reported under a randomly chosen one of them and two identical calls could answer differently.
  AWS's own published sample for the operation answers two entries with the same digest and
  different tags, and the page says a `TAGGED` filter lists "all of the tags in your repository".
- **All three listings now sort before they cut.** `DescribeRepositories` walked its names index in
  creation order, so a repository created mid-walk shifted every later one by a page position; both
  image operations built their result by ranging over the tag map. Repositories sort by name, image
  details by digest, image IDs by digest then tag.

`DescribeImages` also publishes `ImageNotFoundException`/400, which had no site: an `imageIds` entry
naming an unknown tag was dropped from the request and one naming an unknown digest was dropped from
the answer, so a caller naming one real and one imaginary image was answered 200 with a short list.
As with `DescribeRepositories`, only an image the **caller** named is refused; a digest derived from
substrate's own tag index with no record behind it is skipped as an internal inconsistency.

## An immutable tag cannot move, and an untagged image is listed

Until #1379 `imageTagMutability` was stored and never read, and every listing was built from the tag
index. Both are now what the pages publish.

**Tag immutability.** On a repository whose `imageTagMutability` is `IMMUTABLE`, a `PutImage` whose
`imageTag` already names a different image is refused with `ImageTagAlreadyExistsException`/400, which
`API_PutImage` publishes ("The specified image is tagged with a tag that already exists. The repository
is configured for tag immutability."). That covers a new manifest under an existing tag, and an existing
image re-pushed under another image's tag. A refused push stores nothing. A new tag is never a move, and
the same tag on the same manifest is `ImageAlreadyExistsException` whatever the setting.
`IMMUTABLE_WITH_EXCLUSION` reads as `IMMUTABLE` and `MUTABLE_WITH_EXCLUSION` as `MUTABLE`: the
exclusion filters that distinguish them are unmodelled, and with no filter no tag is excluded.
`PutImageTagMutability` is not routed, so the setting is fixed at create.

**Untagged images.** `ListImages`, and `DescribeImages` without `imageIds`, enumerate the repository's
image records rather than its tag index. So an image pushed without a tag, or left behind when its last
tag was removed or moved to another image, is listed: `ListImages` reports it as one entry with its
digest alone, and `DescribeImages` reports it with no `imageTags` member. That is the workflow
`API_ListImages` describes: list the `UNTAGGED` images, then pipe them to `BatchDeleteImage`.

**The filter.** Both operations read `filter`, whose `tagStatus` and `imageStatus` have the same Valid
Values on `ListImagesFilter` and `DescribeImagesFilter`:

| Member | Value | Selects |
|---|---|---|
| `tagStatus` | `TAGGED` | Images carrying a tag; `ListImages` reports every tag |
| `tagStatus` | `UNTAGGED` | Images no tag names |
| `tagStatus` | `ANY`, or absent | Every image. Neither page states a narrower default, and `ListImages`' sample lists all |
| `imageStatus` | `ACTIVE`, `ANY`, or absent | Every image: substrate models no archiving, so every image is `ACTIVE` |
| `imageStatus` | `ARCHIVED`, `ACTIVATING` | Nothing |

A value outside either Valid Values set is `InvalidParameterException`/400. On `DescribeImages` the
filter narrows the listing form; the enumerated form answers the images `imageIds` names, as the page
sets no rule combining the two.


## Every ECR date is epoch seconds

ECR speaks awsJson1_1, where a `Timestamp` is a JSON number of epoch seconds with a fraction, and an
SDK's timestamp deserializer refuses a string. Every date an ECR response renders answers that way,
to three decimals:

| Operation | Member | Page |
|-----------|--------|------|
| `CreateRepository`, `DescribeRepositories`, `DeleteRepository` | `createdAt` | `API_Repository` |
| `DescribeImages` | `imageDetails[].imagePushedAt` | `API_ImageDetail` |
| `GetAuthorizationToken` | `authorizationData[].expiresAt`, 12 hours after the request | `API_AuthorizationData` |

`imagePushedAt` and `expiresAt` rendered as RFC3339 strings until
[#1403](https://github.com/scttfrdmn/substrate/issues/1403), so `DescribeImages` and
`GetAuthorizationToken` failed to decode in a typed SDK. The stored image record keeps its encoding;
the response structs convert on the way out. `TestECRDates_EveryRenderedDateIsEpochSeconds` in
`emulator/ecr_dates_test.go` asserts all three on the raw bytes, at a sub-second instant.

Published dates substrate does not render, because nothing it models produces them:
`ImageDetail`'s `lastRecordedPullTime`, `lastArchivedAt` and `lastActivatedAt` (no pull is recorded
and no archive is modeled), and `GetLifecyclePolicy`'s `lastEvaluatedAt` (a lifecycle policy is
stored, never run).

Pull through cache rules and repository creation templates add four dates, all epoch seconds:
`createdAt` and `updatedAt` on a rule (`Create` answers no `updatedAt`, `Update` no `createdAt`,
`Validate` neither — each operation's Response Syntax, not one shared shape) and on a template.

## A repository ARN is resolved where it points

`TagResource`, `UntagResource` and `ListTagsForResource` take a `resourceArn`, and since
[#1465](https://github.com/scttfrdmn/substrate/issues/1465) and
[#1113](https://github.com/scttfrdmn/substrate/issues/1113) substrate takes it apart rather than
reading its last path segment:

- **Everything after `repository/` is the name.** A namespaced repository — `ns/sub/repo`, and every
  pull-through-cache repository, which is `<prefix>/<upstream-repo>` — is reached by its own ARN. The
  old parser answered `RepositoryNotFoundException: Repository not found: repo` for it.
- **The ARN must name the caller's registry.** Before, the ARN's account and Region were discarded
  and the caller's own repository of that name was tagged, untagged or read. Now an ARN naming another
  account, or the caller's account in another Region, is refused with
  `RepositoryNotFoundException`/400 — the code all three operations publish ("ensure that you are
  performing operations on the correct registry"), and the KMS tagging precedent. That mapping is
  substrate's reading. The reasons: ECR reaches across accounts only through a repository policy plus
  an identity policy (user guide, "How Amazon ECR works with IAM"), substrate stores repository
  policies without evaluating them, and no ECR page confirms the tagging operations work cross-account
  — so serving a foreign repository would grant access AWS might deny. An ECR endpoint is Regional,
  and no page says one serves another Region's repository ARN.
- **A string that is not an ECR repository ARN** — another service's ARN, a bare name, `repository/`
  with nothing after it, an account that is not twelve digits, no Region — is
  `InvalidParameterException`/400. `ListTagsForResource` publishes that "the only supported resource
  is an Amazon ECR repository".

The Resource Groups Tagging API's ECR arm parses through the same function, so the two cannot
disagree about which repository an ARN names. CloudFormation's ECR tagger keys by physical ID (the
repository name), not by ARN, so it was not affected.

## Pull through cache rules and repository creation templates

Substrate models the five rule operations and the four template operations as records in the
caller's registry ([#1461](https://github.com/scttfrdmn/substrate/issues/1461)), validated against
each page's own constraints:

- **Prefixes** — `ecrRepositoryPrefix` (2–30) and template `prefix` (1–256) — match the published
  pattern, `ROOT` included. "There is always an assumed `/` applied to the end of the prefix", so
  `quay` and `quay/` are one rule; the value is echoed as written.
- **`upstreamRegistryUrl`** must be on `API_CreatePullThroughCacheRule`'s list
  (`public.ecr.aws`, `registry.k8s.io`, `quay.io`, `registry-1.docker.io`, `ghcr.io`,
  `registry.gitlab.com`, `<name>.azurecr.io`, `<account>.dkr.ecr.<region>.amazonaws.com`), plus
  `cgr.dev` for Chainguard, which the page's Valid Values name and the user guide gives the URL of.
  Anything else is `UnsupportedUpstreamRegistryException`. `upstreamRegistry` is derived from the URL.
- **Authentication** is the user guide's: Docker Hub, GitHub, GitLab, Azure and Chainguard require a
  `credentialArn` (which must match the published `ecr-pullthroughcache/` pattern); an ECR upstream in
  another account requires a `customRoleArn`. A missing one is `ValidationException`. Which of
  `ValidationException` and `InvalidParameterException` answers what is substrate's reading, since
  every page publishes both and assigns neither to a case: a member outside its own constraint is
  `InvalidParameterException`, members that contradict each other are `ValidationException`. The
  secret itself is not looked up, so `SecretNotFoundException` and its two siblings are never
  answered.
- `upstreamRepositoryPrefix` is reported as `ROOT` when omitted for an ECR upstream ("When this field
  isn't specified, Amazon ECR will use the ROOT") and absent for the others.
- **`ROOT` and `upstreamRepositoryPrefix` are ECR-to-ECR only.** The user guide documents both on its
  "Customizing repository prefixes for ECR to ECR pull through cache" page and for no other upstream,
  and adds "You can set the upstream repository prefix only when the Amazon ECR repository prefix has
  a non-ROOT value". So a `ROOT` `ecrRepositoryPrefix` or any `upstreamRepositoryPrefix` on a non-ECR
  upstream, and a non-`ROOT` upstream prefix under a `ROOT` ecr prefix, are `ValidationException`.
  That the customization is ECR-only is substrate's reading of the page's scope.

**What a pull creates.** A cached repository is created by the first pull, and a pull's one ECR API
call is `BatchGetImage` — "When an image is pulled, the BatchGetImage API is called once" — which is
also the only operation that publishes `UnableToGetUpstreamImageException`. So `BatchGetImage` against
an absent repository that falls under a rule (the longest matching prefix, else a `ROOT` rule)
creates it, from the longest-prefix `PULL_THROUGH_CACHE` template (else a `ROOT` one), else with the
user guide's defaults: `MUTABLE`, `AES256`, no policies and no tags. A template contributes every
member a repository holds: `imageTagMutability`, `imageTagMutabilityExclusionFilters`, encryption type
and `kmsKey`, `repositoryPolicy`, `lifecyclePolicy` and `resourceTags`. For an **ECR upstream** the
upstream registry is one substrate holds, so the repository is created only when the upstream
repository the name maps onto exists — the rule's prefix removed, its `upstreamRepositoryPrefix`
prepended unless `ROOT`, per the user guide's mapping table. That is what keeps a `ROOT` rule, which
matches every name, from turning a mistyped `BatchGetImage` into a new repository. For other upstreams
the content is outside the emulator and the prefix is mandatory. The
image itself is **not** there: substrate reaches no upstream, so `BatchGetImage` reports each
requested image under `failures`, as for any repository that lacks it — push the manifest with
`PutImage` if a test needs one. An existing repository is never re-templated.

**Create on push.** `PutImage` is the call a push makes, so a push to an absent repository governed
by a `CREATE_ON_PUSH` template creates it from that template first. With no such template it is
still `RepositoryNotFoundException`: "Amazon ECR will not create a repository with default settings"
for a push. `PutImage` checks the manifest and digest before it reads the repository, so a push it
would refuse creates nothing — and a missing manifest is now `InvalidParameterException` even when
the repository is also missing.
`REPLICATION` is stored and echoed and never applied, because no replication is modeled.

**Not modelled.** The user guide says a template's role "must be provided when using repository tags
and/or KMS in the template, otherwise the repository creation will fail"; substrate applies
`resourceTags` without a `customRoleArn`, because it models no role assumption and no page publishes
the failure. `ValidatePullThroughCacheRule` contacts no upstream, so it answers `isValid: true` unless
seeded:

```bash
# One rule's outcome in one registry. accountId, region and ecrRepositoryPrefix are each optional;
# empty or "*" is a wildcard, and the most specific seed wins.
curl -X POST http://localhost:4566/v1/ecr/pull-through-cache-validation \
  -d '{"accountId":"123456789012","region":"us-east-1","ecrRepositoryPrefix":"docker-hub","isValid":false,"failure":"authentication failed"}'
curl -X DELETE "http://localhost:4566/v1/ecr/pull-through-cache-validation?accountId=123456789012&region=us-east-1&ecrRepositoryPrefix=docker-hub"
curl -X DELETE http://localhost:4566/v1/ecr/pull-through-cache-validation   # every seed
```

A seed with `isValid: true` and a `failure` is refused: `failure` is "the reason the validation
failed". `TestECR_PullThroughCacheRuleLifecycle` and `TestECR_RepositoryCreationTemplateLifecycle`
assert every response's raw members against its own page.
## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::ECR::Repository | RepositoryName | `LifecyclePolicy.LifecyclePolicyText` is applied through `PutLifecyclePolicy`. There is no `AWS::ECR::LifecyclePolicy` type: CloudFormation does not have one, so since #1605 a template declaring it is refused as unrecognized rather than deployed |

## Cost

ECR storage: $0.10 per GB-month. Data transfer is free within the same region.
