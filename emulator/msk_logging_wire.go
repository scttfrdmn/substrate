package emulator

// The wire shapes of the four members #1386 added: loggingInfo, openMonitoring and rebalancing on a
// provisioned cluster, and connectivityInfo on its broker node group. They are kept apart from the
// state types in msk_types.go for the reason that file's wire preamble gives: the stored record is
// PascalCase and persisted, and the kafka API's wire members are lowerCamel.
//
// Each is answered only when the create sent it. Neither create page states a default for any of
// the four, so a cluster created without one answers without it rather than with an invented value
// (#1013's rule). A boolean a page marks Required is rendered from the pointer the create validated
// present, so it is never absent from a member that is answered.

// mskLoggingInfoOut is a cluster's loggingInfo.
type mskLoggingInfoOut struct {
	BrokerLogs     *mskLogDestinationsOut `json:"brokerLogs,omitempty"`
	AuthorizerLogs *mskLogDestinationsOut `json:"authorizerLogs,omitempty"`
}

// mskLogDestinationsOut is brokerLogs or authorizerLogs.
type mskLogDestinationsOut struct {
	CloudWatchLogs *mskCloudWatchLogsOut `json:"cloudWatchLogs,omitempty"`
	Firehose       *mskFirehoseLogsOut   `json:"firehose,omitempty"`
	S3             *mskS3LogsOut         `json:"s3,omitempty"`
}

// mskCloudWatchLogsOut is a CloudWatch Logs delivery target.
type mskCloudWatchLogsOut struct {
	Enabled  bool   `json:"enabled"`
	LogGroup string `json:"logGroup,omitempty"`
}

// mskFirehoseLogsOut is a Firehose delivery target.
type mskFirehoseLogsOut struct {
	DeliveryStream string `json:"deliveryStream,omitempty"`
	Enabled        bool   `json:"enabled"`
}

// mskS3LogsOut is an S3 delivery target.
type mskS3LogsOut struct {
	Bucket  string `json:"bucket,omitempty"`
	Enabled bool   `json:"enabled"`
	Prefix  string `json:"prefix,omitempty"`
}

// mskOpenMonitoringOut is a cluster's openMonitoring.
type mskOpenMonitoringOut struct {
	Prometheus mskPrometheusOut `json:"prometheus"`
}

// mskPrometheusOut is the Prometheus settings.
type mskPrometheusOut struct {
	JmxExporter  *mskExporterOut `json:"jmxExporter,omitempty"`
	NodeExporter *mskExporterOut `json:"nodeExporter,omitempty"`
}

// mskExporterOut is one Prometheus exporter.
type mskExporterOut struct {
	EnabledInBroker bool `json:"enabledInBroker"`
}

// mskRebalancingOut is a cluster's rebalancing.
type mskRebalancingOut struct {
	Status string `json:"status,omitempty"`
}

// mskConnectivityInfoOut is a broker node group's connectivityInfo.
type mskConnectivityInfoOut struct {
	PublicAccess    *mskPublicAccessOut    `json:"publicAccess,omitempty"`
	VpcConnectivity *mskVpcConnectivityOut `json:"vpcConnectivity,omitempty"`
	NetworkType     string                 `json:"networkType,omitempty"`
}

// mskPublicAccessOut is the publicAccess setting.
type mskPublicAccessOut struct {
	Type string `json:"type,omitempty"`
}

// mskVpcConnectivityOut is the vpcConnectivity setting.
type mskVpcConnectivityOut struct {
	ClientAuthentication *mskVpcClientAuthenticationOut `json:"clientAuthentication,omitempty"`
}

// mskVpcClientAuthenticationOut is vpcConnectivity's clientAuthentication.
type mskVpcClientAuthenticationOut struct {
	Sasl *mskVpcSaslOut `json:"sasl,omitempty"`
	TLS  *mskEnabledOut `json:"tls,omitempty"`
}

// mskVpcSaslOut is vpcConnectivity's SASL settings.
type mskVpcSaslOut struct {
	Scram *mskEnabledOut `json:"scram,omitempty"`
	IAM   *mskEnabledOut `json:"iam,omitempty"`
}

// mskBool answers a Required boolean the create validated present. A nil one cannot reach a
// projection, and would read as false.
func mskBool(b *bool) bool { return b != nil && *b }

// mskLoggingInfoWire projects loggingInfo, or nil when the create sent none.
func mskLoggingInfoWire(l *MSKLoggingInfo) *mskLoggingInfoOut {
	if l == nil {
		return nil
	}
	return &mskLoggingInfoOut{
		BrokerLogs:     mskLogDestinationsWire(l.BrokerLogs),
		AuthorizerLogs: mskLogDestinationsWire(l.AuthorizerLogs),
	}
}

// mskLogDestinationsWire projects brokerLogs or authorizerLogs, or nil when absent.
func mskLogDestinationsWire(d *MSKLogDestinations) *mskLogDestinationsOut {
	if d == nil {
		return nil
	}
	out := &mskLogDestinationsOut{}
	if c := d.CloudWatchLogs; c != nil {
		out.CloudWatchLogs = &mskCloudWatchLogsOut{Enabled: mskBool(c.Enabled), LogGroup: c.LogGroup}
	}
	if f := d.Firehose; f != nil {
		out.Firehose = &mskFirehoseLogsOut{DeliveryStream: f.DeliveryStream, Enabled: mskBool(f.Enabled)}
	}
	if s := d.S3; s != nil {
		out.S3 = &mskS3LogsOut{Bucket: s.Bucket, Enabled: mskBool(s.Enabled), Prefix: s.Prefix}
	}
	return out
}

// mskOpenMonitoringWire projects openMonitoring, or nil when the create sent none.
func mskOpenMonitoringWire(o *MSKOpenMonitoring) *mskOpenMonitoringOut {
	if o == nil || o.Prometheus == nil {
		return nil
	}
	out := &mskOpenMonitoringOut{}
	if j := o.Prometheus.JmxExporter; j != nil {
		out.Prometheus.JmxExporter = &mskExporterOut{EnabledInBroker: mskBool(j.EnabledInBroker)}
	}
	if n := o.Prometheus.NodeExporter; n != nil {
		out.Prometheus.NodeExporter = &mskExporterOut{EnabledInBroker: mskBool(n.EnabledInBroker)}
	}
	return out
}

// mskRebalancingWire projects rebalancing, or nil when the create sent none.
func mskRebalancingWire(r *MSKRebalancing) *mskRebalancingOut {
	if r == nil {
		return nil
	}
	return &mskRebalancingOut{Status: r.Status}
}

// mskConnectivityInfoWire projects connectivityInfo, or nil when the create sent none.
func mskConnectivityInfoWire(c *MSKConnectivityInfo) *mskConnectivityInfoOut {
	if c == nil {
		return nil
	}
	out := &mskConnectivityInfoOut{NetworkType: c.NetworkType}
	if c.PublicAccess != nil {
		out.PublicAccess = &mskPublicAccessOut{Type: c.PublicAccess.Type}
	}
	if v := c.VpcConnectivity; v != nil {
		out.VpcConnectivity = &mskVpcConnectivityOut{}
		if a := v.ClientAuthentication; a != nil {
			ca := &mskVpcClientAuthenticationOut{TLS: mskEnabledWire(a.TLS)}
			if a.Sasl != nil {
				ca.Sasl = &mskVpcSaslOut{Scram: mskEnabledWire(a.Sasl.Scram), IAM: mskEnabledWire(a.Sasl.IAM)}
			}
			out.VpcConnectivity.ClientAuthentication = ca
		}
	}
	return out
}
