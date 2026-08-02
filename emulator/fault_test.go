package emulator_test

import (
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
)

func TestFaultController_NoRules(t *testing.T) {
	fc := emulator.NewFaultController(emulator.FaultConfig{Enabled: true}, 42)
	req := &emulator.AWSRequest{Service: "s3", Operation: "PutObject"}
	reqCtx := &emulator.RequestContext{}
	awsErr, delay := fc.InjectFault(reqCtx, req)
	if awsErr != nil {
		t.Errorf("expected no error, got %v", awsErr)
	}
	if delay != 0 {
		t.Errorf("expected zero delay, got %v", delay)
	}
}

func TestFaultController_Disabled(t *testing.T) {
	fc := emulator.NewFaultController(emulator.FaultConfig{
		Enabled: false,
		Rules: []emulator.FaultRule{
			{FaultType: "error", ErrorCode: "InternalError", Probability: 1.0},
		},
	}, 42)
	req := &emulator.AWSRequest{Service: "s3", Operation: "PutObject"}
	reqCtx := &emulator.RequestContext{}
	awsErr, delay := fc.InjectFault(reqCtx, req)
	if awsErr != nil {
		t.Errorf("disabled controller should not inject error, got %v", awsErr)
	}
	if delay != 0 {
		t.Errorf("disabled controller should not inject delay, got %v", delay)
	}
}

func TestFaultController_ErrorFault(t *testing.T) {
	fc := emulator.NewFaultController(emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{
			{
				Service:     "s3",
				FaultType:   "error",
				ErrorCode:   "ServiceUnavailable",
				HTTPStatus:  503,
				Probability: 1.0,
			},
		},
	}, 42)
	req := &emulator.AWSRequest{Service: "s3", Operation: "PutObject"}
	reqCtx := &emulator.RequestContext{}
	awsErr, delay := fc.InjectFault(reqCtx, req)
	if awsErr == nil {
		t.Fatal("expected error fault, got nil")
	}
	if awsErr.Code != "ServiceUnavailable" {
		t.Errorf("expected code ServiceUnavailable, got %q", awsErr.Code)
	}
	if awsErr.HTTPStatus != 503 {
		t.Errorf("expected HTTP 503, got %d", awsErr.HTTPStatus)
	}
	if delay != 0 {
		t.Errorf("expected zero delay for error fault, got %v", delay)
	}
}

func TestFaultController_LatencyFault(t *testing.T) {
	fc := emulator.NewFaultController(emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{
			{
				FaultType:   "latency",
				LatencyMs:   200,
				Probability: 1.0,
			},
		},
	}, 42)
	req := &emulator.AWSRequest{Service: "dynamodb", Operation: "PutItem"}
	reqCtx := &emulator.RequestContext{}
	awsErr, delay := fc.InjectFault(reqCtx, req)
	if awsErr != nil {
		t.Errorf("expected no error for latency fault, got %v", awsErr)
	}
	if delay != 200*time.Millisecond {
		t.Errorf("expected 200ms delay, got %v", delay)
	}
}

func TestFaultController_Probability_Zero(t *testing.T) {
	fc := emulator.NewFaultController(emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{
			{FaultType: "error", ErrorCode: "InternalError", Probability: 0.0},
		},
	}, 42)
	req := &emulator.AWSRequest{Service: "s3", Operation: "GetObject"}
	reqCtx := &emulator.RequestContext{}
	// With p=0, the roll is always >= p so it should never fire.
	// But per the implementation, p<=0 defaults to 1.0, so test p=0.001 approaches 0.
	// Actually looking at the code, p<=0 is treated as 1.0. So let's test by
	// using a very small value that won't fire with our seed.
	// Instead let's just test that 0.0 uses the 1.0 default and fires.
	awsErr, _ := fc.InjectFault(reqCtx, req)
	// p=0.0 defaults to 1.0 per spec, so it should fire
	if awsErr == nil {
		t.Error("expected fault to fire when probability defaults to 1.0")
	}
}

func TestFaultController_ServiceFilter(t *testing.T) {
	fc := emulator.NewFaultController(emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{
			{
				Service:     "s3",
				FaultType:   "error",
				ErrorCode:   "InternalError",
				Probability: 1.0,
			},
		},
	}, 42)
	// IAM request should not be affected by s3 rule
	iamReq := &emulator.AWSRequest{Service: "iam", Operation: "CreateUser"}
	reqCtx := &emulator.RequestContext{}
	awsErr, _ := fc.InjectFault(reqCtx, iamReq)
	if awsErr != nil {
		t.Errorf("s3 rule should not fire for iam request, got %v", awsErr)
	}
	// S3 request should be affected
	s3Req := &emulator.AWSRequest{Service: "s3", Operation: "PutObject"}
	awsErr, _ = fc.InjectFault(reqCtx, s3Req)
	if awsErr == nil {
		t.Error("s3 rule should fire for s3 request")
	}
}

func TestFaultController_OperationFilter(t *testing.T) {
	fc := emulator.NewFaultController(emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{
			{
				Service:     "s3",
				Operation:   "PutObject",
				FaultType:   "error",
				ErrorCode:   "InternalError",
				Probability: 1.0,
			},
		},
	}, 42)
	reqCtx := &emulator.RequestContext{}
	// PutObject should fire
	putReq := &emulator.AWSRequest{Service: "s3", Operation: "PutObject"}
	awsErr, _ := fc.InjectFault(reqCtx, putReq)
	if awsErr == nil {
		t.Error("rule should fire for matching operation")
	}
	// GetObject should not fire
	getReq := &emulator.AWSRequest{Service: "s3", Operation: "GetObject"}
	awsErr, _ = fc.InjectFault(reqCtx, getReq)
	if awsErr != nil {
		t.Errorf("rule should not fire for non-matching operation, got %v", awsErr)
	}
}

// errorRule builds a one-rule config that fires an error on every match, so a test
// only has to state the matchers it is about.
func errorRule(rule emulator.FaultRule) emulator.FaultConfig {
	rule.FaultType = "error"
	if rule.ErrorCode == "" {
		rule.ErrorCode = "InternalError"
	}
	if rule.Times == 0 {
		rule.Times = -1
	}
	return emulator.FaultConfig{Enabled: true, Rules: []emulator.FaultRule{rule}}
}

// TestFaultController_WireMatchers covers #480 finding 1 option 3: PathSuffix,
// QueryKey and HeaderPrefix select on the wire request rather than on its resolved
// operation name, and every non-empty matcher must hold. QueryKey is the one that
// separates the multipart sub-operations, which share a method and a path and differ
// only by a sub-resource parameter.
func TestFaultController_WireMatchers(t *testing.T) {
	t.Parallel()

	createUpload := &emulator.AWSRequest{
		Service:   "s3",
		Operation: "CreateMultipartUpload",
		Path:      "/b/big.bin",
		Params:    map[string]string{"uploads": "1"},
		Headers:   map[string]string{"X-Amz-Server-Side-Encryption": "AES256"},
	}
	uploadPart := &emulator.AWSRequest{
		Service:   "s3",
		Operation: "UploadPart",
		Path:      "/b/big.bin",
		Params:    map[string]string{"uploadId": "u", "partNumber": "1"},
		Headers:   map[string]string{},
	}
	// A sub-resource sent as "?tagging=" rather than bare "?tagging": the parser
	// records a bare key as the sentinel "1" and an explicitly empty one as "",
	// and QueryKey compares presence rather than the value, so both must match.
	emptyValued := &emulator.AWSRequest{
		Service:   "s3",
		Operation: "PutObjectTagging",
		Path:      "/b/k",
		Params:    map[string]string{"tagging": ""},
		Headers:   map[string]string{},
	}
	putParquet := &emulator.AWSRequest{
		Service:   "s3",
		Operation: "PutObject",
		Path:      "/b/data.parquet",
		Params:    map[string]string{},
		Headers:   map[string]string{},
	}

	tests := []struct {
		name string
		rule emulator.FaultRule
		req  *emulator.AWSRequest
		want bool
	}{
		// The finding-1 gate: naming the semantic operation hits that operation and
		// nothing else that shares its HTTP method.
		{"operation hits PutObject", emulator.FaultRule{Service: "s3", Operation: "PutObject"}, putParquet, true},
		{"operation misses UploadPart", emulator.FaultRule{Service: "s3", Operation: "PutObject"}, uploadPart, false},
		// A bare HTTP method no longer matches an S3 request at all, because the
		// parser resolves the name before faults are evaluated.
		{"bare method no longer matches s3", emulator.FaultRule{Service: "s3", Operation: "PUT"}, putParquet, false},

		{"query key hits create", emulator.FaultRule{QueryKey: "uploads"}, createUpload, true},
		{"query key misses part", emulator.FaultRule{QueryKey: "uploads"}, uploadPart, false},
		{"query key hits part", emulator.FaultRule{QueryKey: "partNumber"}, uploadPart, true},
		// Presence is what a sub-resource parameter signals, so the value is not
		// compared: ?uploads arrives as the bare-key sentinel, ?uploadId=u as a value.
		{"query key ignores the value", emulator.FaultRule{QueryKey: "uploadId"}, uploadPart, true},
		// …and an explicitly empty value is still present. Comparing the value
		// instead of testing for the key would silently miss this form.
		{"query key matches an empty value", emulator.FaultRule{QueryKey: "tagging"}, emptyValued, true},
		{"query key misses an absent key", emulator.FaultRule{QueryKey: "acl"}, emptyValued, false},

		{"path suffix selects an extension", emulator.FaultRule{PathSuffix: ".parquet"}, putParquet, true},
		{"path suffix misses another key", emulator.FaultRule{PathSuffix: ".parquet"}, uploadPart, false},
		{"path suffix selects a whole key", emulator.FaultRule{PathSuffix: "/big.bin"}, uploadPart, true},

		{"header prefix selects", emulator.FaultRule{HeaderPrefix: "x-amz-server-side-encryption"}, createUpload, true},
		{"header prefix misses", emulator.FaultRule{HeaderPrefix: "x-amz-server-side-encryption"}, uploadPart, false},
		// The comparison is case-insensitive both ways: a header name is
		// case-insensitive on the wire and the SDKs do not agree on a spelling.
		{"header prefix ignores case", emulator.FaultRule{HeaderPrefix: "X-Amz-Server-Side-"}, createUpload, true},

		// All matchers AND together, so a rule that gets one wrong fires on nothing.
		{
			name: "all matchers together",
			rule: emulator.FaultRule{
				Service: "s3", Operation: "CreateMultipartUpload",
				PathSuffix: ".bin", QueryKey: "uploads",
				HeaderPrefix: "x-amz-server-side-encryption",
			},
			req:  createUpload,
			want: true,
		},
		{
			name: "one wrong matcher blocks the rest",
			rule: emulator.FaultRule{
				Service: "s3", Operation: "CreateMultipartUpload",
				PathSuffix: ".parquet", QueryKey: "uploads",
			},
			req:  createUpload,
			want: false,
		},
		// The AND is asserted from both sides: a matching wire matcher must not
		// rescue a rule whose operation is wrong, which is what an OR would do and
		// what would put a PutObject rule back onto UploadPart by another route.
		{
			name: "a matching path suffix does not rescue a wrong operation",
			rule: emulator.FaultRule{Service: "s3", Operation: "PutObject", PathSuffix: "/big.bin"},
			req:  uploadPart,
			want: false,
		},
		{
			name: "a matching query key does not rescue a wrong operation",
			rule: emulator.FaultRule{Service: "s3", Operation: "PutObject", QueryKey: "uploadId"},
			req:  uploadPart,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fc := emulator.NewFaultController(errorRule(tt.rule), 42)
			awsErr, _ := fc.InjectFault(&emulator.RequestContext{}, tt.req)
			if tt.want && awsErr == nil {
				t.Error("expected the rule to fire")
			}
			if !tt.want && awsErr != nil {
				t.Errorf("expected the rule not to fire, got %v", awsErr)
			}
		})
	}
}

// TestFaultController_NonS3BareMethodStillFires is the regression guard for the other
// side of the semantic-name change: only S3 requests are renamed, so a rule on a bare
// method still fires for a service whose operation genuinely is its HTTP method.
func TestFaultController_NonS3BareMethodStillFires(t *testing.T) {
	t.Parallel()
	fc := emulator.NewFaultController(errorRule(emulator.FaultRule{Operation: http.MethodGet}), 42)
	req := &emulator.AWSRequest{Service: "execute-api", Operation: http.MethodGet, Path: "/prod/items"}
	awsErr, _ := fc.InjectFault(&emulator.RequestContext{}, req)
	if awsErr == nil {
		t.Error("a bare-method rule should still fire for a non-S3 service")
	}
}

// TestFaultController_ErrorDefaults pins what a rule naming only its fault type
// produces. Every field of an injected error is optional, and the defaults are the
// difference between a usable fault and one an SDK cannot see: a zero HTTPStatus
// serialized as HTTP 200 would arrive as a success carrying an error document, and an
// empty code leaves the SDK nothing to dispatch on.
func TestFaultController_ErrorDefaults(t *testing.T) {
	t.Parallel()
	fc := emulator.NewFaultController(emulator.FaultConfig{
		Enabled: true,
		Rules:   []emulator.FaultRule{{FaultType: "error"}},
	}, 42)

	awsErr, _ := fc.InjectFault(&emulator.RequestContext{},
		&emulator.AWSRequest{Service: "s3", Operation: "PutObject"})
	if awsErr == nil {
		t.Fatal("expected a fault with only FaultType set to fire")
	}
	if awsErr.Code != "InternalError" {
		t.Errorf("Code = %q, want InternalError", awsErr.Code)
	}
	if awsErr.HTTPStatus != http.StatusInternalServerError {
		t.Errorf("HTTPStatus = %d, want %d", awsErr.HTTPStatus, http.StatusInternalServerError)
	}
	if awsErr.Message != "injected fault" {
		t.Errorf("Message = %q, want \"injected fault\"", awsErr.Message)
	}
}

// TestFaultController_Times covers #480 finding 2. The bound is what makes retry
// assertable — fail twice then succeed is the outcome that distinguishes working
// retry from no retry, and an unbounded rule can only ever produce failure.
func TestFaultController_Times(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// times is the FaultRule.Times value under test, written out rather than
		// defaulted so the zero case is deliberate.
		times int
		// wantFailures is how many of six successive requests fail.
		wantFailures int
	}{
		// Zero means one, not unlimited: a rule that fires on nothing is never what a
		// caller meant, and reading zero as unlimited turns a mistyped field into a
		// test that consumes the whole retry budget. Asserted deliberately because it
		// is a choice, not an inevitability.
		{"zero means one", 0, 1},
		{"one", 1, 1},
		{"two failures then success", 2, 2},
		{"negative is unlimited", -1, 6},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fc := emulator.NewFaultController(emulator.FaultConfig{
				Enabled: true,
				Rules: []emulator.FaultRule{{
					Service:   "s3",
					Operation: "PutObject",
					FaultType: "error",
					ErrorCode: "SlowDown",
					Times:     tt.times,
				}},
			}, 42)

			req := &emulator.AWSRequest{Service: "s3", Operation: "PutObject"}
			failures := 0
			for i := 0; i < 6; i++ {
				if awsErr, _ := fc.InjectFault(&emulator.RequestContext{}, req); awsErr != nil {
					failures++
				}
			}
			if failures != tt.wantFailures {
				t.Errorf("failures = %d, want %d", failures, tt.wantFailures)
			}
			if fired := fc.FaultsFired(); fired != tt.wantFailures {
				t.Errorf("FaultsFired() = %d, want %d", fired, tt.wantFailures)
			}
		})
	}
}

// TestFaultController_TimesBoundsLatencyToo asserts the bound applies to a latency
// fault as well as an error one. A latency rule is the one a caller is most likely to
// arm unbounded by accident, since it produces no visible failure to notice.
func TestFaultController_TimesBoundsLatencyToo(t *testing.T) {
	t.Parallel()
	fc := emulator.NewFaultController(emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{{
			Service:   "s3",
			FaultType: "latency",
			LatencyMs: 5,
			Times:     2,
		}},
	}, 42)

	req := &emulator.AWSRequest{Service: "s3", Operation: "GetObject"}
	delays := 0
	for i := 0; i < 5; i++ {
		if _, delay := fc.InjectFault(&emulator.RequestContext{}, req); delay > 0 {
			delays++
		}
	}
	if delays != 2 {
		t.Errorf("delays = %d, want 2", delays)
	}
	if fired := fc.FaultsFired(); fired != 2 {
		t.Errorf("FaultsFired() = %d, want 2", fired)
	}
}

// TestFaultController_SpentRuleFallsThroughToTheNext asserts a rule that has reached
// its bound is skipped rather than stopping evaluation, so a later rule still gets its
// turn. Returning early on a spent rule would silently disarm every rule after it.
func TestFaultController_SpentRuleFallsThroughToTheNext(t *testing.T) {
	t.Parallel()
	fc := emulator.NewFaultController(emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{
			{Service: "s3", FaultType: "error", ErrorCode: "SlowDown", Times: 1},
			{Service: "s3", FaultType: "error", ErrorCode: "InternalError", Times: -1},
		},
	}, 42)

	req := &emulator.AWSRequest{Service: "s3", Operation: "PutObject"}
	first, _ := fc.InjectFault(&emulator.RequestContext{}, req)
	if first == nil || first.Code != "SlowDown" {
		t.Fatalf("first request: got %v, want SlowDown", first)
	}
	second, _ := fc.InjectFault(&emulator.RequestContext{}, req)
	if second == nil || second.Code != "InternalError" {
		t.Fatalf("second request: got %v, want InternalError", second)
	}
}

// TestFaultController_TimesIsAtomic is the race gate for #480 finding 2, and must run
// under -race. With Times set to 1, N concurrent requests have to produce exactly one
// failure; that atomicity is the property the mechanism exists for, and it is not
// something a counter on the client side can arrange.
func TestFaultController_TimesIsAtomic(t *testing.T) {
	t.Parallel()
	fc := emulator.NewFaultController(emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{{
			Service:   "s3",
			Operation: "PutObject",
			FaultType: "error",
			ErrorCode: "SlowDown",
			Times:     1,
		}},
	}, 42)

	const workers = 10
	var wg sync.WaitGroup
	var failures atomic.Int64
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := &emulator.AWSRequest{Service: "s3", Operation: "PutObject"}
			if awsErr, _ := fc.InjectFault(&emulator.RequestContext{}, req); awsErr != nil {
				failures.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := failures.Load(); got != 1 {
		t.Errorf("failures across %d concurrent requests = %d, want 1", workers, got)
	}
	if fired := fc.FaultsFired(); fired != 1 {
		t.Errorf("FaultsFired() = %d, want 1", fired)
	}
}

// TestFaultController_GetConfigCopiesRules asserts the snapshot is a copy: a caller
// reading a fired count must not share the slice InjectFault increments, or the read
// races the write and -race reports it.
func TestFaultController_GetConfigCopiesRules(t *testing.T) {
	t.Parallel()
	fc := emulator.NewFaultController(emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{
			{Service: "s3", FaultType: "error", ErrorCode: "SlowDown", Times: -1},
		},
	}, 42)

	req := &emulator.AWSRequest{Service: "s3", Operation: "PutObject"}
	_, _ = fc.InjectFault(&emulator.RequestContext{}, req)
	snapshot := fc.GetConfig()
	if snapshot.Rules[0].Fired != 1 {
		t.Fatalf("snapshot fired = %d, want 1", snapshot.Rules[0].Fired)
	}

	_, _ = fc.InjectFault(&emulator.RequestContext{}, req)
	if snapshot.Rules[0].Fired != 1 {
		t.Errorf("snapshot fired = %d after a second fault, want the copy to be unchanged", snapshot.Rules[0].Fired)
	}
	if live := fc.GetConfig().Rules[0].Fired; live != 2 {
		t.Errorf("live fired = %d, want 2", live)
	}
}

// TestFaultController_FaultsFiredIsZeroForARuleThatMatchedNothing is the assertion
// that gives the count its point: a rule matching no request produces exactly the same
// passing test as a consumer's retry working, and only the count tells them apart.
func TestFaultController_FaultsFiredIsZeroForARuleThatMatchedNothing(t *testing.T) {
	t.Parallel()
	fc := emulator.NewFaultController(errorRule(emulator.FaultRule{
		Service:   "s3",
		Operation: "PutObject",
	}), 42)

	req := &emulator.AWSRequest{Service: "s3", Operation: "GetObject"}
	if awsErr, _ := fc.InjectFault(&emulator.RequestContext{}, req); awsErr != nil {
		t.Fatalf("rule should not have fired, got %v", awsErr)
	}
	if fired := fc.FaultsFired(); fired != 0 {
		t.Errorf("FaultsFired() = %d, want 0", fired)
	}
	if fired := fc.GetConfig().Rules[0].Fired; fired != 0 {
		t.Errorf("rule fired = %d, want 0", fired)
	}
}

// TestFaultController_UpdateConfigResetsFiredCounts pins the control-plane contract:
// arming rules replaces the configuration, so a fixture that re-arms the same rule
// between test phases gets its full Times budget rather than a spent one.
func TestFaultController_UpdateConfigResetsFiredCounts(t *testing.T) {
	t.Parallel()
	cfg := emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{
			{Service: "s3", FaultType: "error", ErrorCode: "SlowDown", Times: 1},
		},
	}
	fc := emulator.NewFaultController(cfg, 42)
	req := &emulator.AWSRequest{Service: "s3", Operation: "PutObject"}

	if awsErr, _ := fc.InjectFault(&emulator.RequestContext{}, req); awsErr == nil {
		t.Fatal("expected the first request to fail")
	}
	if awsErr, _ := fc.InjectFault(&emulator.RequestContext{}, req); awsErr != nil {
		t.Fatal("expected the rule to be spent")
	}

	fc.UpdateConfig(cfg)
	if fired := fc.FaultsFired(); fired != 0 {
		t.Errorf("FaultsFired() = %d after re-arming, want 0", fired)
	}
	if awsErr, _ := fc.InjectFault(&emulator.RequestContext{}, req); awsErr == nil {
		t.Error("expected a re-armed rule to fire again")
	}
}

// TestFaultController_UpdateConfigDoesNotMutateTheCallersRules is the second half of
// the private-copy guarantee. [TestFaultController_UpdateConfigResetsFiredCounts] only
// exercises the copy NewFaultController makes: the value it re-arms with is still
// pristine because the first arming copied it. Arming twice through UpdateConfig is
// what reaches the second copy — without it, the fired count is written into the
// caller's own slice and the second arming starts spent.
func TestFaultController_UpdateConfigDoesNotMutateTheCallersRules(t *testing.T) {
	t.Parallel()
	cfg := emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{
			{Service: "s3", FaultType: "error", ErrorCode: "SlowDown", Times: 1},
		},
	}
	fc := emulator.NewFaultController(emulator.FaultConfig{Enabled: false}, 42)
	req := &emulator.AWSRequest{Service: "s3", Operation: "PutObject"}

	fc.UpdateConfig(cfg)
	if awsErr, _ := fc.InjectFault(&emulator.RequestContext{}, req); awsErr == nil {
		t.Fatal("expected the first request to fail")
	}

	// The caller's literal must be untouched, so a fixture holding one value and
	// arming it between phases gets its full budget every time.
	if cfg.Rules[0].Fired != 0 {
		t.Errorf("the caller's rule was mutated: Fired = %d, want 0", cfg.Rules[0].Fired)
	}

	fc.UpdateConfig(cfg)
	if awsErr, _ := fc.InjectFault(&emulator.RequestContext{}, req); awsErr == nil {
		t.Error("expected the same config, armed again, to fire again")
	}
}

func TestFaultController_UpdateConfig(t *testing.T) {
	fc := emulator.NewFaultController(emulator.FaultConfig{Enabled: false}, 42)
	req := &emulator.AWSRequest{Service: "s3", Operation: "PutObject"}
	reqCtx := &emulator.RequestContext{}
	awsErr, _ := fc.InjectFault(reqCtx, req)
	if awsErr != nil {
		t.Error("disabled controller should not fire")
	}

	// Enable and add a rule
	fc.UpdateConfig(emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{
			{FaultType: "error", ErrorCode: "InternalError", Probability: 1.0},
		},
	})
	awsErr, _ = fc.InjectFault(reqCtx, req)
	if awsErr == nil {
		t.Error("expected fault after UpdateConfig enabled it")
	}
}
