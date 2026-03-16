package substrate_test

import (
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate"
)

func TestGenerateTestFixture_Empty(t *testing.T) {
	src, err := substrate.GenerateTestFixture(nil, "mytestpkg", "TestEmpty")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(src, "package mytestpkg") {
		t.Errorf("missing package declaration in:\n%s", src)
	}
	if !strings.Contains(src, "func TestEmpty") {
		t.Errorf("missing function declaration in:\n%s", src)
	}
	// Must be valid Go.
	if _, parseErr := parser.ParseFile(token.NewFileSet(), "", src, 0); parseErr != nil {
		t.Fatalf("generated source is not valid Go: %v\nsource:\n%s", parseErr, src)
	}
}

func TestGenerateTestFixture_WithEvents(t *testing.T) {
	req := &substrate.AWSRequest{
		Service:   "sqs",
		Operation: "SendMessage",
		Path:      "/",
		Headers:   map[string]string{"Host": "sqs.us-east-1.amazonaws.com"},
		Body:      []byte(`{"QueueUrl":"https://sqs.us-east-1.amazonaws.com/123/q","MessageBody":"hello"}`),
	}
	resp := &substrate.AWSResponse{
		StatusCode: 200,
	}
	events := []*substrate.Event{
		{
			Sequence:  0,
			Service:   "sqs",
			Operation: "SendMessage",
			Request:   req,
			Response:  resp,
		},
		{
			Sequence:  1,
			Service:   "sqs",
			Operation: "ReceiveMessage",
			// No request body — should be skipped.
		},
	}

	src, err := substrate.GenerateTestFixture(events, "substrate_test", "TestSQSReplay")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Must be valid Go.
	if _, parseErr := parser.ParseFile(token.NewFileSet(), "", src, 0); parseErr != nil {
		t.Fatalf("generated source is not valid Go: %v\nsource:\n%s", parseErr, src)
	}

	if !strings.Contains(src, "http.NewRequest") {
		t.Errorf("expected http.NewRequest in generated source")
	}
	if !strings.Contains(src, `"/"`) {
		t.Errorf("expected path %q in generated source", "/")
	}
	if !strings.Contains(src, "sqs.us-east-1.amazonaws.com") {
		t.Errorf("expected Host header in generated source")
	}
	// ReceiveMessage (no request) should NOT appear.
	if strings.Contains(src, "ReceiveMessage") {
		t.Errorf("expected skipped event to be absent from generated source")
	}
}

func TestGenerateTestFixture_BacktickBody(t *testing.T) {
	req := &substrate.AWSRequest{
		Service:   "s3",
		Operation: "PutObject",
		Path:      "/bucket/key",
		Body:      []byte("data with a ` backtick"),
	}
	events := []*substrate.Event{
		{Sequence: 0, Service: "s3", Operation: "PutObject", Request: req},
	}

	src, err := substrate.GenerateTestFixture(events, "substrate_test", "TestBacktick")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Must be valid Go even with backtick in the body.
	if _, parseErr := parser.ParseFile(token.NewFileSet(), "", src, 0); parseErr != nil {
		t.Fatalf("generated source is not valid Go: %v\nsource:\n%s", parseErr, src)
	}
}

func TestGenerateTestFixture_NoBodyEvents(t *testing.T) {
	events := []*substrate.Event{
		{Sequence: 0, Service: "iam", Operation: "ListUsers"},
		{Sequence: 1, Service: "iam", Operation: "GetUser"},
	}
	src, err := substrate.GenerateTestFixture(events, "substrate_test", "TestNoBody")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// All events have no request; the function body should still be valid Go.
	if _, parseErr := parser.ParseFile(token.NewFileSet(), "", src, 0); parseErr != nil {
		t.Fatalf("generated source is not valid Go: %v\nsource:\n%s", parseErr, src)
	}
}
