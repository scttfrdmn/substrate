//go:build ignore
// Run: go run examples/betty_workflow/main.go

// Package main demonstrates the full Betty.codes validation workflow using
// Substrate's in-process API — no HTTP server required.
//
// The example:
//  1. Wires up IAM and S3 plugins.
//  2. Deploys a CloudFormation template (role + S3 bucket) via BettyClient.Deploy.
//  3. Opens a recording session and runs manual S3 PutObject operations.
//  4. Stops the session and validates it with BettyClient.StopRecording.
//  5. Opens a DebugSession and inspects state at event 0.
//  6. Prints the ValidationReport as formatted JSON.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/spf13/afero"

	substrate "github.com/scttfrdmn/substrate"
)

func main() {
	ctx := context.Background()

	// --- 1. Wire up dependencies -------------------------------------------

	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	store := substrate.NewEventStore(substrate.EventStoreConfig{
		Enabled:       true,
		Backend:       "memory",
		IncludeBodies: true,
	})

	registry := substrate.NewPluginRegistry()

	iamPlugin := &substrate.IAMPlugin{}
	if err := iamPlugin.Initialize(ctx, substrate.PluginConfig{
		State:  state,
		Logger: logger,
	}); err != nil {
		log.Fatalf("IAMPlugin.Initialize: %v", err)
	}
	registry.Register(iamPlugin)

	s3Plugin := &substrate.S3Plugin{}
	if err := s3Plugin.Initialize(ctx, substrate.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"filesystem":      afero.NewMemMapFs(),
		},
	}); err != nil {
		log.Fatalf("S3Plugin.Initialize: %v", err)
	}
	registry.Register(s3Plugin)

	betty := substrate.NewBettyClient(registry, store, state, tc, logger)

	// --- 2. Deploy a CloudFormation stack ----------------------------------

	cfnTemplate := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Description": "Betty workflow example stack",
		"Resources": {
			"AppRole": {
				"Type": "AWS::IAM::Role",
				"Properties": {
					"RoleName": "betty-example-role",
					"AssumeRolePolicyDocument": {
						"Version": "2012-10-17",
						"Statement": [{
							"Effect": "Allow",
							"Principal": {"Service": "lambda.amazonaws.com"},
							"Action": "sts:AssumeRole"
						}]
					}
				}
			},
			"DataBucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": {
					"BucketName": "betty-example-bucket"
				}
			}
		}
	}`

	deployResult, err := betty.Deploy(ctx, cfnTemplate, substrate.Intent{
		MaxCost: 1.0, // warn if deploy cost exceeds $1
	})
	if err != nil {
		log.Fatalf("Deploy: %v", err)
	}
	fmt.Printf("Deployed stack %q: %d resources, cost=$%.6f, duration=%s\n",
		deployResult.StackName,
		len(deployResult.Resources),
		deployResult.TotalCost,
		deployResult.Duration.Round(time.Millisecond),
	)
	for _, r := range deployResult.Resources {
		if r.Error != "" {
			fmt.Printf("  [WARN] %s (%s): %s\n", r.LogicalID, r.Type, r.Error)
		} else {
			fmt.Printf("  [OK]   %s (%s) → %s\n", r.LogicalID, r.Type, r.PhysicalID)
		}
	}

	// --- 3. Record S3 operations -------------------------------------------

	session, err := betty.StartRecording(ctx, "s3-workload")
	if err != nil {
		log.Fatalf("StartRecording: %v", err)
	}
	fmt.Printf("\nRecording session started (stream=%s)\n", session.StreamID)

	// Simulate S3 operations via in-process plugin dispatch.
	reqCtx := &substrate.RequestContext{
		RequestID: "ex-1",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Timestamp: tc.Now(),
		Metadata:  map[string]any{"stream_id": session.StreamID},
	}
	for i := range 5 {
		req := &substrate.AWSRequest{
			Service:   "s3",
			Operation: "PutObject",
			Path:      fmt.Sprintf("/betty-example-bucket/item-%d", i),
			Headers:   map[string]string{"Content-Type": "application/octet-stream"},
			Params:    map[string]string{},
			Body:      []byte(fmt.Sprintf(`{"index":%d}`, i)),
		}
		resp, dispatchErr := registry.RouteRequest(reqCtx, req)
		if dispatchErr != nil {
			fmt.Printf("  [WARN] PutObject %d: %v\n", i, dispatchErr)
			continue
		}
		if storeErr := store.RecordRequest(ctx, reqCtx, req, resp, time.Millisecond, 0.000005, nil); storeErr != nil {
			log.Fatalf("RecordRequest: %v", storeErr)
		}
	}

	// --- 4. Stop recording and validate ------------------------------------

	report, err := betty.StopRecording(ctx, session)
	if err != nil {
		log.Fatalf("StopRecording: %v", err)
	}

	// --- 5. Debug session — inspect state at event 0 ----------------------

	dbg := betty.NewDebugSession(session.StreamID)
	if jumpErr := dbg.JumpToEvent(ctx, 0); jumpErr != nil {
		fmt.Printf("[WARN] JumpToEvent(0): %v\n", jumpErr)
	} else {
		stateSnap, inspectErr := dbg.InspectState(ctx)
		if inspectErr != nil {
			fmt.Printf("[WARN] InspectState: %v\n", inspectErr)
		} else {
			fmt.Printf("\nState snapshot at event 0: %d bytes\n", len(stateSnap))
		}
	}

	// --- 6. Print ValidationReport as JSON ---------------------------------

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	fmt.Println("\n--- ValidationReport ---")
	if marshalErr := enc.Encode(report); marshalErr != nil {
		log.Fatalf("encode report: %v", marshalErr)
	}
}
