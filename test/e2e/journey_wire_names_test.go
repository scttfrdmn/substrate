package e2e_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/wafv2"
	wafv2types "github.com/aws/aws-sdk-go-v2/service/wafv2/types"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_DynamoDBTableArnDecodes is #738 at the only tier that could have caught it.
//
// Substrate published the table's ARN as TableARN, the stream's as LatestStreamARN and an
// index's as IndexARN. DynamoDB publishes TableArn, LatestStreamArn and IndexArn, so a real
// SDK decoded all three as nil and a consumer reading TableArn got an empty string with no
// error — the failure mode this project exists to prevent, and one no hand-built parameter
// map can expose, because the unit tier asserted on substrate's own spelling.
//
// The e2e module had no DynamoDB client at all before this, which is precisely why every
// release up to v0.108.0 shipped the defect green.
func TestJourney_DynamoDBTableArnDecodes(t *testing.T) {
	ts := emulator.StartTestServer(t)
	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.RetryMaxAttempts = 1 })

	const tableName = "wire-names"
	create, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("gsi"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
		GlobalSecondaryIndexes: []ddbtypes.GlobalSecondaryIndex{{
			IndexName: aws.String("by-gsi"),
			KeySchema: []ddbtypes.KeySchemaElement{
				{AttributeName: aws.String("gsi"), KeyType: ddbtypes.KeyTypeHash},
			},
			Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
		}},
		StreamSpecification: &ddbtypes.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: ddbtypes.StreamViewTypeNewAndOldImages,
		},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// CreateTable's own TableDescription, decoded by the SDK rather than read from a map.
	assertTableARNs(t, "CreateTable", create.TableDescription)

	describe, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		t.Fatalf("DescribeTable: %v", err)
	}
	assertTableARNs(t, "DescribeTable", describe.Table)
}

// assertTableARNs checks the three ARN members a real SDK reads off a TableDescription.
// aws.ToString turning a nil pointer into "" is exactly how the defect stayed invisible,
// so each assertion tests the pointer as well as the value.
func assertTableARNs(t *testing.T, op string, desc *ddbtypes.TableDescription) {
	t.Helper()
	if desc == nil {
		t.Fatalf("%s: TableDescription is nil", op)
	}

	if desc.TableArn == nil {
		t.Errorf("%s: TableArn is nil — substrate is publishing the ARN under another name", op)
	} else if !strings.HasPrefix(*desc.TableArn, "arn:aws:dynamodb:") {
		t.Errorf("%s: TableArn = %q, want an arn:aws:dynamodb: prefix", op, *desc.TableArn)
	}

	if desc.LatestStreamArn == nil {
		t.Errorf("%s: LatestStreamArn is nil although the table has a stream enabled", op)
	} else if !strings.Contains(*desc.LatestStreamArn, "/stream/") {
		t.Errorf("%s: LatestStreamArn = %q, want it to name a stream", op, *desc.LatestStreamArn)
	}

	if len(desc.GlobalSecondaryIndexes) != 1 {
		t.Fatalf("%s: want 1 GSI, got %d", op, len(desc.GlobalSecondaryIndexes))
	}
	gsi := desc.GlobalSecondaryIndexes[0]
	if gsi.IndexArn == nil {
		t.Errorf("%s: GSI IndexArn is nil", op)
	} else if !strings.HasSuffix(*gsi.IndexArn, "/index/by-gsi") {
		t.Errorf("%s: GSI IndexArn = %q, want it to name the index", op, *gsi.IndexArn)
	}
}

// TestJourney_WAFv2IPSetAddressVersion is the half of #738 that was a behaviour bug rather
// than a rendering one. Substrate read the request's IP version from IPVersion, so a typed
// SDK — which can only send IPAddressVersion — had its IPV6 request silently replaced by
// substrate's IPV4 default. The set came back successfully, holding the wrong family, and a
// consumer's error branch never ran.
func TestJourney_WAFv2IPSetAddressVersion(t *testing.T) {
	ts := emulator.StartTestServer(t)
	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	client := wafv2.NewFromConfig(cfg, func(o *wafv2.Options) { o.RetryMaxAttempts = 1 })

	create, err := client.CreateIPSet(ctx, &wafv2.CreateIPSetInput{
		Name:             aws.String("v6-only"),
		Scope:            wafv2types.ScopeRegional,
		IPAddressVersion: wafv2types.IPAddressVersionIpv6,
		Addresses:        []string{"2001:db8::/32"},
	})
	if err != nil {
		t.Fatalf("CreateIPSet: %v", err)
	}
	if create.Summary == nil || create.Summary.Id == nil {
		t.Fatal("CreateIPSet returned no summary Id")
	}

	got, err := client.GetIPSet(ctx, &wafv2.GetIPSetInput{
		Id:    create.Summary.Id,
		Name:  aws.String("v6-only"),
		Scope: wafv2types.ScopeRegional,
	})
	if err != nil {
		t.Fatalf("GetIPSet: %v", err)
	}
	if got.IPSet == nil {
		t.Fatal("GetIPSet returned no IPSet")
	}
	if got.IPSet.IPAddressVersion != wafv2types.IPAddressVersionIpv6 {
		t.Errorf("IPAddressVersion = %q, want IPV6 — the requested family was not read",
			got.IPSet.IPAddressVersion)
	}
}
