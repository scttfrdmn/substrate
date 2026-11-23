package emulator

import (
	"fmt"
	"net/http"
	"strings"
)

// Warm throughput and server-side encryption on a table description (#1608).
//
// Terraform's aws_dynamodb_table waits on both after every create and update, and each wait spun
// forever while the member was absent: waitTableWarmThroughputActive polls for WarmThroughput.Status
// ACTIVE on the table and on each global secondary index, and the SSE waiter polls for
// SSEDescription.Status ENABLED on a table created with server_side_encryption.
//
// # Warm throughput
//
// "Warm throughput values are available by default for all tables and global secondary indexes", and
// the developer guide's scenarios page publishes the on-demand default: "A new on-demand table starts
// with a warm throughput of 12,000 read units per second and 4,000 write units per second." Its
// describe-table example reports the same values, with Status ACTIVE, on the table and on its GSI. The
// guide publishes no separate default for a provisioned table, only that "As you increase the
// provisioned throughput beyond these values, the warm throughput will automatically adjust to the new
// higher values". So substrate reports, for a table and for each GSI, the larger of that default, the
// value CreateTable or UpdateTable requested, and the provisioned capacity; using the on-demand
// default as the provisioned baseline is substrate's reading. A requested value lower than the current
// one leaves it unchanged, since "After warm throughput is increased, the values can't be decreased".
// Substrate settles every change in the request that makes it, so Status is always ACTIVE.
//
// # Server-side encryption
//
// API_SSESpecification: "If enabled (true), server-side encryption type is set to KMS and an AWS managed
// key is used ... If disabled (false) or not specified, server-side encryption is set to AWS owned key."
// A table on an AWS owned key reports no SSEDescription, and one using KMS reports Status ENABLED,
// SSEType KMS and the KMSMasterKeyArn. A KMSMasterKeyId given as a key ID is reported as that key's ARN
// in the caller's account and Region, and a key ARN or alias ARN verbatim. An alias name is reported as
// its alias ARN rather than resolved to the key behind it, which AWS would do. No key is looked up, and
// substrate mints no AWS managed key, so a table using KMS with no KMSMasterKeyId reports no
// KMSMasterKeyArn rather than an invented one.

// dynamodbDefaultWarmRead and dynamodbDefaultWarmWrite are the published on-demand defaults.
const (
	dynamodbDefaultWarmRead  = 12000
	dynamodbDefaultWarmWrite = 4000
)

// DynamoDBWarmThroughput is a WarmThroughput: the request member on CreateTable, UpdateTable and a GSI,
// and, with Status, the TableWarmThroughputDescription and GlobalSecondaryIndexWarmThroughputDescription
// a description reports. Stored, it holds what a caller requested.
type DynamoDBWarmThroughput struct {
	// ReadUnitsPerSecond is the warm read throughput.
	ReadUnitsPerSecond int64 `json:"ReadUnitsPerSecond,omitempty"`

	// WriteUnitsPerSecond is the warm write throughput.
	WriteUnitsPerSecond int64 `json:"WriteUnitsPerSecond,omitempty"`

	// Status is ACTIVE on the wire; it is not stored.
	Status string `json:"Status,omitempty"`
}

// DynamoDBSSEDescription is an SSEDescription.
type DynamoDBSSEDescription struct {
	// Status is ENABLED.
	Status string `json:"Status"`

	// SSEType is KMS.
	SSEType string `json:"SSEType"`

	// KMSMasterKeyArn is the key's ARN, when the request named one.
	KMSMasterKeyArn string `json:"KMSMasterKeyArn,omitempty"`
}

// dynamodbSSESpecification is an SSESpecification request member.
type dynamodbSSESpecification struct {
	Enabled        *bool  `json:"Enabled"`
	KMSMasterKeyID string `json:"KMSMasterKeyId"`
	SSEType        string `json:"SSEType"`
}

// dynamodbSSEDescriptionFor returns the description a specification asks for: nil for an AWS owned
// key. The messages of its refusals are substrate's own.
func dynamodbSSEDescriptionFor(spec *dynamodbSSESpecification, region, accountID string) (*DynamoDBSSEDescription, error) {
	if spec == nil {
		return nil, nil
	}
	if spec.SSEType != "" && spec.SSEType != "KMS" && spec.SSEType != "AES256" {
		return nil, &AWSError{Code: "ValidationException",
			Message:    fmt.Sprintf("SSEType %q is not one of AES256, KMS", spec.SSEType),
			HTTPStatus: http.StatusBadRequest}
	}
	if spec.Enabled == nil || !*spec.Enabled {
		return nil, nil
	}
	return &DynamoDBSSEDescription{
		Status:          "ENABLED",
		SSEType:         "KMS",
		KMSMasterKeyArn: dynamodbKMSKeyARN(spec.KMSMasterKeyID, region, accountID),
	}, nil
}

// dynamodbKMSKeyARN renders a KMSMasterKeyId as an ARN, "" for none: an ARN verbatim, an alias name as
// its alias ARN, and anything else as a key ID.
func dynamodbKMSKeyARN(keyID, region, accountID string) string {
	switch {
	case keyID == "":
		return ""
	case strings.HasPrefix(keyID, "arn:"):
		return keyID
	case strings.HasPrefix(keyID, "alias/"):
		return "arn:aws:kms:" + region + ":" + accountID + ":" + keyID
	default:
		return "arn:aws:kms:" + region + ":" + accountID + ":key/" + keyID
	}
}

// dynamodbValidateWarmThroughput refuses a requested value outside "Valid Range: Minimum value of 1".
// An absent member is 0 and is not a request. The message is substrate's own.
func dynamodbValidateWarmThroughput(wt *DynamoDBWarmThroughput) error {
	if wt != nil && (wt.ReadUnitsPerSecond < 0 || wt.WriteUnitsPerSecond < 0) {
		return &AWSError{Code: "ValidationException",
			Message:    "WarmThroughput ReadUnitsPerSecond and WriteUnitsPerSecond must be at least 1",
			HTTPStatus: http.StatusBadRequest}
	}
	return nil
}

// dynamodbMergeWarmThroughput raises a stored request by a new one. Warm throughput is never lowered.
func dynamodbMergeWarmThroughput(stored, requested *DynamoDBWarmThroughput) *DynamoDBWarmThroughput {
	if requested == nil {
		return stored
	}
	out := DynamoDBWarmThroughput{ReadUnitsPerSecond: requested.ReadUnitsPerSecond, WriteUnitsPerSecond: requested.WriteUnitsPerSecond}
	if stored != nil {
		out.ReadUnitsPerSecond = max(out.ReadUnitsPerSecond, stored.ReadUnitsPerSecond)
		out.WriteUnitsPerSecond = max(out.WriteUnitsPerSecond, stored.WriteUnitsPerSecond)
	}
	return &out
}

// dynamodbWarmThroughputWire renders the warm throughput a table or index reports.
func dynamodbWarmThroughputWire(requested *DynamoDBWarmThroughput, pt DynamoDBProvisionedThroughputDesc) *DynamoDBWarmThroughput {
	out := &DynamoDBWarmThroughput{
		ReadUnitsPerSecond:  max(int64(dynamodbDefaultWarmRead), pt.ReadCapacityUnits),
		WriteUnitsPerSecond: max(int64(dynamodbDefaultWarmWrite), pt.WriteCapacityUnits),
		Status:              "ACTIVE",
	}
	if requested != nil {
		out.ReadUnitsPerSecond = max(out.ReadUnitsPerSecond, requested.ReadUnitsPerSecond)
		out.WriteUnitsPerSecond = max(out.WriteUnitsPerSecond, requested.WriteUnitsPerSecond)
	}
	return out
}
