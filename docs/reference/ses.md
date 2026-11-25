# SES

**Endpoint:** `email.{region}.amazonaws.com`
**Protocol:** Query (`Version=2010-12-01`)

SES v1 is the API the AWS CLI's `aws ses`, boto3's `ses` client and Terraform's `aws_ses_*`
resources call. It shares its identities with [SES v2](/reference/sesv2): one account's domains and
addresses, whichever API created them.

## Supported operations

| Operation | Notes |
|-----------|-------|
| VerifyDomainIdentity | Creates the domain identity if the account does not hold it. The `VerificationToken` is derived from the account, Region and domain, so a repeat call answers the same token |
| VerifyEmailIdentity | Creates the address identity if the account does not hold it. Nothing is mailed |
| VerifyEmailAddress | The deprecated form of VerifyEmailIdentity. No result element, as its model declares no output |
| VerifyDomainDkim | Three 32-character Easy DKIM tokens, derived like the verification token, and DKIM signing enabled |
| SetIdentityDkimEnabled | Refused with `InvalidParameterValue` when enabling DKIM on a domain VerifyDomainDkim never set up, or on an address whose domain it never set up |
| GetIdentityVerificationAttributes | The seeded status (see below), and the token for a domain only |
| GetIdentityDkimAttributes | `NotStarted` with DKIM off and no tokens until VerifyDomainDkim, then the seeded status. An address reports its domain's tokens and status, and its own enabled setting |
| GetIdentityMailFromDomainAttributes | `BehaviorOnMXFailure` reads `UseDefaultValue` until one is set. The domain and its status are reported only while one is set |
| GetIdentityNotificationAttributes | Feedback forwarding is on by default. Topics are reported only while set |
| ListIdentities | Name order. `IdentityType` filters, `MaxItems` is 1–1000 (default 1000), and `NextToken` is a cursor |
| ListVerifiedEmailAddresses | The address identities whose status is `Success` |
| DeleteIdentity | Succeeds for an identity the account does not hold |
| DeleteVerifiedEmailAddress | The deprecated form of DeleteIdentity, for an address. No result element |
| SetIdentityMailFromDomain | The MAIL FROM domain must be a subdomain of the identity's domain. No `MailFromDomain` disables it |
| SetIdentityNotificationTopic | No `SnsTopic` clears that notification type's topic. The topic is recorded, not checked against SNS |
| SetIdentityFeedbackForwardingEnabled | `false` is refused unless bounce and complaint topics are both set |
| SetIdentityHeadersInNotificationsEnabled | |
| SendEmail | Recorded, not sent. Answers a deterministic `MessageId` |
| SendRawEmail | Recorded with the raw message as sent, not sent. Recipients are `Destinations`; the MIME headers are not parsed |
| GetSendQuota | The sandbox quota (200 per 24 hours, 1 per second), and the recipients recorded in the last 24 simulated hours |
| GetSendStatistics | One point per 15-minute period with recorded messages, over the last two simulated weeks. Delivery attempts count recipients, and bounces, complaints and rejects are 0 |

Every other SES v1 action answers `InvalidAction`.

## Routing: SES v1 and SES v2 share every signal

Both models declare the endpoint prefix `email` and the signing name `ses`, and neither sends
`X-Amz-Target`, so the host and the credential scope cannot tell them apart. The path does. Every
`requestUri` in the `sesv2` model starts with `/v2/`, and SES v1 posts its Query form to `/`. A
request on the `email` host or signed for `ses` goes to SES v2 when its path starts with `/v2/`,
and to SES v1 otherwise (#1614).

## Identities are shared with SES v2

SES v1 reads and writes the same identity records as SES v2. A domain verified through
`VerifyDomainIdentity` is SES v2's `GetEmailIdentity` answer. That answer includes the
verification status and, once SES v1 set them up, the DKIM tokens and MAIL FROM domain. An
identity SES v2's `CreateEmailIdentity` created is listed by `ListIdentities`, and a domain created
that way reports its token through `GetIdentityVerificationAttributes`. Deleting through either API
removes the identity from both.

Messages are shared the same way. `SendEmail` and `SendRawEmail` record into the store SES v2's
`SendEmail` uses, so `GET /v1/emails` lists messages from both APIs.

## Verification is seeded

AWS verifies an identity outside the API. It verifies a domain when it finds the TXT record or
DKIM CNAMEs in the domain's DNS, and an address when its owner follows the link SES mailed. That is
not observable to substrate. The status defaults to `Success`, so a wait loop such as
`aws_ses_domain_identity_verification` finishes on its first poll. A seed sets any other status:

```bash
# Report one identity, or every identity without its own seed ("*"), in every account and Region.
curl -X POST localhost:4566/v1/ses/identity-verification \
  -d '{"example.com": "Pending", "*": "Failed"}'
# The same, scoped to one account and Region; either may be omitted, meaning every one.
curl -X POST localhost:4566/v1/ses/identity-verification \
  -d '{"accountId": "123456789012", "region": "us-east-1", "identities": {"example.com": "Pending"}}'
# Clear one seed, named as it was posted (an omitted part is the wildcard), or every seed.
curl -X DELETE 'localhost:4566/v1/ses/identity-verification?identity=example.com&accountId=123456789012&region=us-east-1'
curl -X DELETE localhost:4566/v1/ses/identity-verification
```

A body with an `identities` member is the scoped form, and any other body is the bare form. The
scoped form takes no other members. Its `accountId` and `region` must not contain `/` or `*`, and
either of them without `identities` is refused with 400. When several seeds match, the most
specific one wins. A seed naming the identity outranks a `*` one, in any account. Among seeds that
name it the same way, an exact account outranks every account, and then an exact Region outranks
every Region. A body is validated in identity order before anything is written, so a refused body
writes no seed.

## Naming an identity by its ARN

Every operation that takes an identity also takes its ARN,
`arn:aws:ses:{region}:{account}:identity/{name}`, which SetIdentityNotificationTopic's page lists
among an identity's valid forms. The Get*Attributes maps are keyed by the identity as the caller
named it. An ARN naming another account or Region is treated as an identity the caller does not
hold. Get*Attributes leaves it out, DeleteIdentity does nothing, and the Set* operations refuse it
with `InvalidParameterValue`. No published source says what SES answers there, so this is
substrate's reading.

The statuses are SES v1's: `Pending`, `Success`, `Failed`, `TemporaryFailure` and `NotStarted`.
Any other value is refused with 400. One seed governs both APIs. SES v2's `GetEmailIdentity`
reports the same value in its own spelling (`PENDING`), with `VerifiedForSendingStatus` true
exactly when it is `SUCCESS`. The DKIM status reports the seeded value once VerifyDomainDkim has
run, and an address reports its domain's. The MAIL FROM status does too while a domain is set, except that `NotStarted`, which
`CustomMailFromStatus` lacks, reads as `Pending`. The seed is a control-plane write, so it is
recorded and replays in position.

## Errors

`MissingParameter` and `InvalidParameterValue`, both 400, are the codes SES's
[Common Error Types](https://docs.aws.amazon.com/ses/latest/APIReference/CommonErrors.html) page
publishes for a missing or unusable parameter. None of the identity operations publishes an error of
its own. The messages are substrate's own. A sender is not refused for being unverified: SES v2's
`SendEmail` does not model the sandbox either, and the two answer one account's sending the same way.

## Cost

SES outbound email: $0.10 per 1,000 emails, for `SendEmail` and `SendRawEmail`.
