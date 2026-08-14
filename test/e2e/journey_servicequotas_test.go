package e2e_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/servicequotas"
	sqtypes "github.com/aws/aws-sdk-go-v2/service/servicequotas/types"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_ServiceQuotasOrganizations is #620 at the SDK level: the
// Organizations account ceiling read through the real Service Quotas client.
//
// The unit tests cover the table and each refusal. What only this level catches is
// routing — the Service Quotas SigV4 name is `servicequotasv20190624` and its
// X-Amz-Target prefix is `ServiceQuotasV20190624`, neither of which is the plugin
// name, so a mapping that stops matching sends every SDK call to "service not
// emulated" while the unit tests, which drive the plugin through a hand-built
// target header, stay green. That is #610's failure mode exactly.
func TestJourney_ServiceQuotasOrganizations(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	// Retries off, so each assertion is about the first response rather than
	// whatever the retry loop settled on.
	quotas := servicequotas.NewFromConfig(cfg, func(o *servicequotas.Options) { o.RetryMaxAttempts = 1 })

	// --- the discovery path a tool actually walks ---
	services, err := quotas.ListServices(ctx, &servicequotas.ListServicesInput{})
	if err != nil {
		t.Fatalf("ListServices: %v", err)
	}
	found := false
	for _, svc := range services.Services {
		if aws.ToString(svc.ServiceCode) == "organizations" {
			found = true
			if got := aws.ToString(svc.ServiceName); got != "AWS Organizations" {
				t.Errorf("ListServices names organizations %q", got)
			}
		}
	}
	if !found {
		t.Fatal("ListServices did not report organizations, so nothing can discover its quotas")
	}

	listed, err := quotas.ListServiceQuotas(ctx, &servicequotas.ListServiceQuotasInput{
		ServiceCode: aws.String("organizations"),
	})
	if err != nil {
		t.Fatalf("ListServiceQuotas(organizations): %v", err)
	}
	if len(listed.Quotas) != 3 {
		t.Fatalf("ListServiceQuotas(organizations) returned %d quotas, want 3", len(listed.Quotas))
	}

	// --- #620's repro verbatim ---
	got, err := quotas.GetServiceQuota(ctx, &servicequotas.GetServiceQuotaInput{
		ServiceCode: aws.String("organizations"),
		QuotaCode:   aws.String("L-E619E033"),
	})
	if err != nil {
		t.Fatalf("GetServiceQuota(organizations, L-E619E033): %v", err)
	}
	if v := aws.ToFloat64(got.Quota.Value); v != 10 {
		t.Errorf("the accounts quota reads %v, want 10 — the value CreateAccount enforces", v)
	}
	if !got.Quota.Adjustable {
		t.Error("the accounts quota reports Adjustable=false; it is the adjustable one")
	}
	if !got.Quota.GlobalQuota {
		t.Error("the accounts quota reports GlobalQuota=false; Organizations is a global service")
	}

	// GetAWSDefaultServiceQuota agrees, because nothing here mutates a quota: the
	// applied value and the AWS default cannot diverge.
	def, err := quotas.GetAWSDefaultServiceQuota(ctx, &servicequotas.GetAWSDefaultServiceQuotaInput{
		ServiceCode: aws.String("organizations"),
		QuotaCode:   aws.String("L-E619E033"),
	})
	if err != nil {
		t.Fatalf("GetAWSDefaultServiceQuota: %v", err)
	}
	if aws.ToFloat64(def.Quota.Value) != aws.ToFloat64(got.Quota.Value) {
		t.Errorf("the applied value %v and the AWS default %v disagree",
			aws.ToFloat64(got.Quota.Value), aws.ToFloat64(def.Quota.Value))
	}

	// --- the refusals, through the typed exception a consumer branches on ---
	//
	// errors.As is the branch a caller's error handling actually takes. Both cases
	// are NoSuchResourceException, so the type alone cannot tell them apart — which
	// is why the plugin distinguishes them in the message.
	var noSuch *sqtypes.NoSuchResourceException
	if _, err := quotas.ListServiceQuotas(ctx, &servicequotas.ListServiceQuotasInput{
		ServiceCode: aws.String("nosuchsvc"),
	}); err == nil {
		t.Fatal("an unknown service code: expected NoSuchResourceException, not an empty list")
	} else if !errors.As(err, &noSuch) {
		t.Fatalf("expected *NoSuchResourceException, got %T: %v", err, err)
	}

	if _, err := quotas.GetServiceQuota(ctx, &servicequotas.GetServiceQuotaInput{
		ServiceCode: aws.String("organizations"),
		QuotaCode:   aws.String("L-NOTAQUOTA"),
	}); err == nil {
		t.Fatal("an unknown quota code: expected NoSuchResourceException")
	} else if !errors.As(err, &noSuch) {
		t.Fatalf("expected *NoSuchResourceException, got %T: %v", err, err)
	}

	// --- the increase a consumer files after reading the ceiling ---
	//
	// It records a request and grants nothing. A tool that read the quota back
	// expecting its DesiredValue would be asserting on a state real Service Quotas
	// never reaches on this call.
	requested, err := quotas.RequestServiceQuotaIncrease(ctx, &servicequotas.RequestServiceQuotaIncreaseInput{
		ServiceCode:  aws.String("organizations"),
		QuotaCode:    aws.String("L-E619E033"),
		DesiredValue: aws.Float64(50),
	})
	if err != nil {
		t.Fatalf("RequestServiceQuotaIncrease: %v", err)
	}
	if requested.RequestedQuota.Status != sqtypes.RequestStatusPending {
		t.Errorf("Status = %q, want PENDING", requested.RequestedQuota.Status)
	}
	after, err := quotas.GetServiceQuota(ctx, &servicequotas.GetServiceQuotaInput{
		ServiceCode: aws.String("organizations"),
		QuotaCode:   aws.String("L-E619E033"),
	})
	if err != nil {
		t.Fatalf("GetServiceQuota after the increase: %v", err)
	}
	if v := aws.ToFloat64(after.Quota.Value); v != 10 {
		t.Errorf("a pending increase moved the quota to %v; it must still read 10", v)
	}
}

// TestJourney_ServiceQuotasCallerIsolation is #624 at the SDK level: two accounts
// filing increases against one emulator, each seeing only its own.
//
// The plugin discarded its request context and filed every increase under the
// literal 000000000000, so this is the leak that fix closed. It needs two signing
// identities, which is why it lives here rather than in a single-caller journey: a
// consumer that reads its request history to decide whether it has already asked
// would otherwise see a sibling's request and skip filing its own.
func TestJourney_ServiceQuotasCallerIsolation(t *testing.T) {
	const otherAccount = "210987654321"
	ts := emulator.StartTestServerWithAccounts(t, journeyAccountID, otherAccount)

	ctx := context.Background()
	clients := make(map[string]*servicequotas.Client, 2)
	for _, account := range []string{journeyAccountID, otherAccount} {
		cfg, err := journeyConfigAs(ts, account)
		if err != nil {
			t.Fatalf("config for %s: %v", account, err)
		}
		clients[account] = servicequotas.NewFromConfig(cfg,
			func(o *servicequotas.Options) { o.RetryMaxAttempts = 1 })
	}

	// Each account asks for a different value, so the records are distinguishable by
	// content and not only by count.
	wanted := map[string]float64{journeyAccountID: 50, otherAccount: 75}
	for account, client := range clients {
		if _, err := client.RequestServiceQuotaIncrease(ctx, &servicequotas.RequestServiceQuotaIncreaseInput{
			ServiceCode:  aws.String("organizations"),
			QuotaCode:    aws.String("L-E619E033"),
			DesiredValue: aws.Float64(wanted[account]),
		}); err != nil {
			t.Fatalf("RequestServiceQuotaIncrease as %s: %v", account, err)
		}
	}

	for account, client := range clients {
		history, err := client.ListRequestedServiceQuotaChangeHistory(ctx,
			&servicequotas.ListRequestedServiceQuotaChangeHistoryInput{
				ServiceCode: aws.String("organizations"),
			})
		if err != nil {
			t.Fatalf("ListRequestedServiceQuotaChangeHistory as %s: %v", account, err)
		}
		if len(history.RequestedQuotas) != 1 {
			t.Fatalf("%s sees %d increase requests, want only its own 1", account, len(history.RequestedQuotas))
		}
		got := history.RequestedQuotas[0]
		if v := aws.ToFloat64(got.DesiredValue); v != wanted[account] {
			t.Errorf("%s sees a request for %v, want its own %v — the requests are crossed",
				account, v, wanted[account])
		}
		if got.Status != sqtypes.RequestStatusPending {
			t.Errorf("%s's request reads Status %q, want PENDING", account, got.Status)
		}
	}

	// The Status filter has to agree with the unfiltered read, or a consumer that
	// narrows its history query to PENDING would conclude it had never asked.
	pending, err := clients[journeyAccountID].ListRequestedServiceQuotaChangeHistory(ctx,
		&servicequotas.ListRequestedServiceQuotaChangeHistoryInput{
			Status: sqtypes.RequestStatusPending,
		})
	if err != nil {
		t.Fatalf("ListRequestedServiceQuotaChangeHistory(Status=PENDING): %v", err)
	}
	if len(pending.RequestedQuotas) != 1 {
		t.Errorf("filtering on PENDING returned %d requests, want the caller's 1",
			len(pending.RequestedQuotas))
	}
	// A status no record holds returns nothing rather than everything, which is the
	// difference between an honoured filter and an ignored one.
	denied, err := clients[journeyAccountID].ListRequestedServiceQuotaChangeHistory(ctx,
		&servicequotas.ListRequestedServiceQuotaChangeHistoryInput{
			Status: sqtypes.RequestStatusDenied,
		})
	if err != nil {
		t.Fatalf("ListRequestedServiceQuotaChangeHistory(Status=DENIED): %v", err)
	}
	if len(denied.RequestedQuotas) != 0 {
		t.Errorf("filtering on DENIED returned %d requests; every record is PENDING",
			len(denied.RequestedQuotas))
	}
}
