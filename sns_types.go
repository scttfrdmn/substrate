// Package substrate contains SNS type definitions for the AWS SNS emulator.
package substrate

import "fmt"

const snsNamespace = "sns"

// SNSTopic represents an Amazon SNS topic.
type SNSTopic struct {
	// ARN is the Amazon Resource Name of the topic.
	ARN string `json:"TopicArn"`

	// Name is the topic name.
	Name string `json:"Name"`

	// Attributes holds optional topic attributes.
	Attributes map[string]string `json:"Attributes,omitempty"`

	// Tags holds optional resource tags.
	Tags []SNSTag `json:"Tags,omitempty"`

	// AccountID is the AWS account that owns this topic.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where the topic resides.
	Region string `json:"Region"`
}

// SNSTag is a key-value tag for SNS resources.
type SNSTag struct {
	// Key is the tag key.
	Key string `json:"Key"`

	// Value is the tag value.
	Value string `json:"Value"`
}

// SNSSubscription represents an SNS subscription.
type SNSSubscription struct {
	// ARN is the subscription ARN.
	ARN string `json:"SubscriptionArn"`

	// TopicARN is the ARN of the subscribed topic.
	TopicARN string `json:"TopicArn"`

	// Protocol is the subscription protocol (e.g., "sqs", "lambda", "email").
	Protocol string `json:"Protocol"`

	// Endpoint is the protocol-specific destination endpoint.
	Endpoint string `json:"Endpoint"`

	// AccountID is the AWS account that owns this subscription.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where the subscription resides.
	Region string `json:"Region"`

	// FilterPolicy holds an optional message filtering policy. When set,
	// only messages whose attributes match the policy are delivered.
	FilterPolicy map[string]interface{} `json:"FilterPolicy,omitempty"`
}

// snsTopicARN constructs an SNS topic ARN from region, account, and name.
func snsTopicARN(region, accountID, name string) string {
	return fmt.Sprintf("arn:aws:sns:%s:%s:%s", region, accountID, name)
}

// snsSubscriptionARN constructs an SNS subscription ARN.
func snsSubscriptionARN(region, accountID, topicName, subID string) string {
	return fmt.Sprintf("arn:aws:sns:%s:%s:%s:%s", region, accountID, topicName, subID)
}

// generateSNSSubID returns a short random ID for SNS subscriptions.
func generateSNSSubID() string {
	return randomHex(8)
}
