package emulator

import (
	"fmt"
	"net/http"
	"strings"
)

// The OpenSearch Service control plane is not emulated (#1212, #1564).
//
// Substrate routes the OpenSearch data plane — the domain's own REST API — and nothing else. The
// control plane (CreateDomain, DescribeDomain, ListDomainNames and the rest) arrives on a host and a
// SigV4 signing name this plugin owns, so it reaches HandleRequest, and before #1212 its path was split
// as `{index}/{rest…}`: `POST /2021-01-01/opensearch/domain` was read as an index named `2021-01-01`
// and answered the engine's `route_not_found` at 404, a body with no AWS `Code` an SDK could model.
//
// The path shape is the reason no general rule covered it. Both control-plane APIs publish their API
// version date as the first path segment — `/2021-01-01/…` for `opensearch`, `/2015-01-01/…` for the
// legacy `es` API — which no other REST service in the tree does on a host of its own. Every one of the
// 155 operations botocore's two models publish carries one of those two prefixes, so the first segment
// alone tells a control-plane call from a data-plane one. The cost is that an index literally named
// `2021-01-01` or `2015-01-01` is unreachable; an index named for a date normally carries a prefix
// (`logs-2021-01-01`), which is unaffected.
//
// A control-plane call is refused as an AWSError, so the REST-JSON serializer writes it with the
// `X-Amzn-ErrorType` an SDK reads. The code and status are the ones substrate answers for every
// unrouted operation of a REST-JSON service, UnknownOperationException at 404 (see
// [unknownActionError]); the message is substrate's own, and names the operation the method and path
// resolve to under the published models.

// openSearchControlPlaneVersions are the API version dates the two control-plane models put first in
// every request URI: `opensearch` 2021-01-01 and `es` 2015-01-01.
var openSearchControlPlaneVersions = map[string]bool{"2021-01-01": true, "2015-01-01": true}

// openSearchControlPlaneOp is one published control-plane operation: its method, its request-URI
// template and its name.
type openSearchControlPlaneOp struct {
	method, uri, name string
}

// isOpenSearchControlPlanePath reports whether path is addressed to the control plane: its first
// segment is one of the two API version dates.
func isOpenSearchControlPlanePath(path string) bool {
	first, _, _ := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	return openSearchControlPlaneVersions[first]
}

// openSearchControlPlaneOperation names the published operation method and path resolve to, or ""
// when no template matches. Where two templates match, the one with more literal segments wins, which
// is how a REST router prefers a literal segment over a label in the same position.
func openSearchControlPlaneOperation(method, path string) string {
	segs := strings.Split(strings.Trim(path, "/"), "/")
	best, bestLiterals := "", -1
	for _, op := range openSearchControlPlaneOps {
		if op.method != method {
			continue
		}
		tmpl := strings.Split(strings.Trim(op.uri, "/"), "/")
		if len(tmpl) != len(segs) {
			continue
		}
		literals, ok := 0, true
		for i, t := range tmpl {
			if strings.HasPrefix(t, "{") && strings.HasSuffix(t, "}") {
				if segs[i] == "" {
					ok = false
					break
				}
				continue
			}
			if t != segs[i] {
				ok = false
				break
			}
			literals++
		}
		if ok && literals > bestLiterals {
			best, bestLiterals = op.name, literals
		}
	}
	return best
}

// openSearchControlPlaneRefusal is the error a control-plane call answers.
//
// The message is substrate's own: no AWS page publishes wording for an operation an endpoint does
// not implement. It names the operation when the path resolves to one, and the method and path when
// it does not, and points at #1564, where the control plane is tracked.
func openSearchControlPlaneRefusal(method, path string) *AWSError {
	what := method + " " + path
	if name := openSearchControlPlaneOperation(method, path); name != "" {
		what = name + " (" + what + ")"
	}
	return &AWSError{
		Code: "UnknownOperationException",
		Message: fmt.Sprintf("substrate does not emulate the OpenSearch Service control plane: %s is not supported; "+
			"see https://github.com/scttfrdmn/substrate/issues/1564", what),
		HTTPStatus: http.StatusNotFound,
	}
}

// openSearchControlPlaneOps is every operation in botocore's `opensearch` (2021-01-01) and `es`
// (2015-01-01) service-2.json models, as method, requestUri and name. It is used only to name the
// operation a refusal declines; none of these is routed.
var openSearchControlPlaneOps = []openSearchControlPlaneOp{
	{"GET", "/2021-01-01/domain", "ListDomainNames"},
	{"GET", "/2021-01-01/domain/{DomainName}/packages", "ListPackagesForDomain"},
	{"GET", "/2021-01-01/opensearch/app-migrations", "ListMigrations"},
	{"POST", "/2021-01-01/opensearch/app-migrations", "StartMigration"},
	{"GET", "/2021-01-01/opensearch/app-migrations/{migrationId}", "GetMigration"},
	{"POST", "/2021-01-01/opensearch/application", "CreateApplication"},
	{"GET", "/2021-01-01/opensearch/application/{ApplicationId}/capability/{CapabilityName}", "GetCapability"},
	{"DELETE", "/2021-01-01/opensearch/application/{ApplicationId}/capability/deregister/{CapabilityName}", "DeregisterCapability"},
	{"POST", "/2021-01-01/opensearch/application/{ApplicationId}/capability/register", "RegisterCapability"},
	{"DELETE", "/2021-01-01/opensearch/application/{id}", "DeleteApplication"},
	{"GET", "/2021-01-01/opensearch/application/{id}", "GetApplication"},
	{"PUT", "/2021-01-01/opensearch/application/{id}", "UpdateApplication"},
	{"POST", "/2021-01-01/opensearch/application/{id}/attachDataSource", "AttachDataSource"},
	{"POST", "/2021-01-01/opensearch/application/{id}/describeDataSourceAttachment", "DescribeDataSourceAttachment"},
	{"POST", "/2021-01-01/opensearch/application/{id}/detachDataSource", "DetachDataSource"},
	{"POST", "/2021-01-01/opensearch/application/{id}/listDataSourceAttachments", "ListDataSourceAttachments"},
	{"DELETE", "/2021-01-01/opensearch/cc/inboundConnection/{ConnectionId}", "DeleteInboundConnection"},
	{"PUT", "/2021-01-01/opensearch/cc/inboundConnection/{ConnectionId}/accept", "AcceptInboundConnection"},
	{"PUT", "/2021-01-01/opensearch/cc/inboundConnection/{ConnectionId}/reject", "RejectInboundConnection"},
	{"POST", "/2021-01-01/opensearch/cc/inboundConnection/search", "DescribeInboundConnections"},
	{"POST", "/2021-01-01/opensearch/cc/outboundConnection", "CreateOutboundConnection"},
	{"DELETE", "/2021-01-01/opensearch/cc/outboundConnection/{ConnectionId}", "DeleteOutboundConnection"},
	{"POST", "/2021-01-01/opensearch/cc/outboundConnection/search", "DescribeOutboundConnections"},
	{"POST", "/2021-01-01/opensearch/compareDomainConfiguration", "CompareDomainConfiguration"},
	{"GET", "/2021-01-01/opensearch/compatibleVersions", "GetCompatibleVersions"},
	{"GET", "/2021-01-01/opensearch/defaultApplicationSetting", "GetDefaultApplicationSetting"},
	{"PUT", "/2021-01-01/opensearch/defaultApplicationSetting", "PutDefaultApplicationSetting"},
	{"POST", "/2021-01-01/opensearch/directQueryDataSource", "AddDirectQueryDataSource"},
	{"GET", "/2021-01-01/opensearch/directQueryDataSource", "ListDirectQueryDataSources"},
	{"DELETE", "/2021-01-01/opensearch/directQueryDataSource/{DataSourceName}", "DeleteDirectQueryDataSource"},
	{"GET", "/2021-01-01/opensearch/directQueryDataSource/{DataSourceName}", "GetDirectQueryDataSource"},
	{"PUT", "/2021-01-01/opensearch/directQueryDataSource/{DataSourceName}", "UpdateDirectQueryDataSource"},
	{"POST", "/2021-01-01/opensearch/domain-info", "DescribeDomains"},
	{"POST", "/2021-01-01/opensearch/domain", "CreateDomain"},
	{"DELETE", "/2021-01-01/opensearch/domain/{DomainName}", "DeleteDomain"},
	{"GET", "/2021-01-01/opensearch/domain/{DomainName}", "DescribeDomain"},
	{"POST", "/2021-01-01/opensearch/domain/{DomainName}/authorizeVpcEndpointAccess", "AuthorizeVpcEndpointAccess"},
	{"GET", "/2021-01-01/opensearch/domain/{DomainName}/autoTunes", "DescribeDomainAutoTunes"},
	{"POST", "/2021-01-01/opensearch/domain/{DomainName}/clone", "UpdateClone"},
	{"GET", "/2021-01-01/opensearch/domain/{DomainName}/config", "DescribeDomainConfig"},
	{"POST", "/2021-01-01/opensearch/domain/{DomainName}/config", "UpdateDomainConfig"},
	{"POST", "/2021-01-01/opensearch/domain/{DomainName}/config/cancel", "CancelDomainConfigChange"},
	{"POST", "/2021-01-01/opensearch/domain/{DomainName}/dataSource", "AddDataSource"},
	{"GET", "/2021-01-01/opensearch/domain/{DomainName}/dataSource", "ListDataSources"},
	{"DELETE", "/2021-01-01/opensearch/domain/{DomainName}/dataSource/{DataSourceName}", "DeleteDataSource"},
	{"GET", "/2021-01-01/opensearch/domain/{DomainName}/dataSource/{DataSourceName}", "GetDataSource"},
	{"PUT", "/2021-01-01/opensearch/domain/{DomainName}/dataSource/{DataSourceName}", "UpdateDataSource"},
	{"GET", "/2021-01-01/opensearch/domain/{DomainName}/domainMaintenance", "GetDomainMaintenanceStatus"},
	{"POST", "/2021-01-01/opensearch/domain/{DomainName}/domainMaintenance", "StartDomainMaintenance"},
	{"GET", "/2021-01-01/opensearch/domain/{DomainName}/domainMaintenances", "ListDomainMaintenances"},
	{"GET", "/2021-01-01/opensearch/domain/{DomainName}/dryRun", "DescribeDryRunProgress"},
	{"GET", "/2021-01-01/opensearch/domain/{DomainName}/health", "DescribeDomainHealth"},
	{"POST", "/2021-01-01/opensearch/domain/{DomainName}/index", "CreateIndex"},
	{"DELETE", "/2021-01-01/opensearch/domain/{DomainName}/index/{IndexName}", "DeleteIndex"},
	{"GET", "/2021-01-01/opensearch/domain/{DomainName}/index/{IndexName}", "GetIndex"},
	{"PUT", "/2021-01-01/opensearch/domain/{DomainName}/index/{IndexName}", "UpdateIndex"},
	{"GET", "/2021-01-01/opensearch/domain/{DomainName}/listVpcEndpointAccess", "ListVpcEndpointAccess"},
	{"GET", "/2021-01-01/opensearch/domain/{DomainName}/nodes", "DescribeDomainNodes"},
	{"GET", "/2021-01-01/opensearch/domain/{DomainName}/progress", "DescribeDomainChangeProgress"},
	{"GET", "/2021-01-01/opensearch/domain/{DomainName}/replication", "DescribeDomainReplication"},
	{"POST", "/2021-01-01/opensearch/domain/{DomainName}/replication/addPeer", "AddReplicationPeer"},
	{"POST", "/2021-01-01/opensearch/domain/{DomainName}/replication/failover", "FailoverReplication"},
	{"POST", "/2021-01-01/opensearch/domain/{DomainName}/replication/removePeer", "RemoveReplicationPeer"},
	{"POST", "/2021-01-01/opensearch/domain/{DomainName}/replication/switchover", "SwitchoverReplication"},
	{"POST", "/2021-01-01/opensearch/domain/{DomainName}/revokeVpcEndpointAccess", "RevokeVpcEndpointAccess"},
	{"PUT", "/2021-01-01/opensearch/domain/{DomainName}/scheduledAction/update", "UpdateScheduledAction"},
	{"GET", "/2021-01-01/opensearch/domain/{DomainName}/scheduledActions", "ListScheduledActions"},
	{"GET", "/2021-01-01/opensearch/domain/{DomainName}/vpcEndpoints", "ListVpcEndpointsForDomain"},
	{"POST", "/2021-01-01/opensearch/insight-details", "DescribeInsightDetails"},
	{"POST", "/2021-01-01/opensearch/insight-feedback", "InsightFeedback"},
	{"POST", "/2021-01-01/opensearch/insights", "ListInsights"},
	{"GET", "/2021-01-01/opensearch/instanceTypeDetails/{EngineVersion}", "ListInstanceTypeDetails"},
	{"GET", "/2021-01-01/opensearch/instanceTypeLimits/{EngineVersion}/{InstanceType}", "DescribeInstanceTypeLimits"},
	{"GET", "/2021-01-01/opensearch/list-applications", "ListApplications"},
	{"POST", "/2021-01-01/opensearch/purchaseReservedInstanceOffering", "PurchaseReservedInstanceOffering"},
	{"GET", "/2021-01-01/opensearch/reservedInstanceOfferings", "DescribeReservedInstanceOfferings"},
	{"GET", "/2021-01-01/opensearch/reservedInstances", "DescribeReservedInstances"},
	{"POST", "/2021-01-01/opensearch/serviceSoftwareUpdate/cancel", "CancelServiceSoftwareUpdate"},
	{"POST", "/2021-01-01/opensearch/serviceSoftwareUpdate/rollback", "RollbackServiceSoftwareUpdate"},
	{"POST", "/2021-01-01/opensearch/serviceSoftwareUpdate/start", "StartServiceSoftwareUpdate"},
	{"POST", "/2021-01-01/opensearch/upgradeDomain", "UpgradeDomain"},
	{"GET", "/2021-01-01/opensearch/upgradeDomain/{DomainName}/history", "GetUpgradeHistory"},
	{"GET", "/2021-01-01/opensearch/upgradeDomain/{DomainName}/status", "GetUpgradeStatus"},
	{"GET", "/2021-01-01/opensearch/versions", "ListVersions"},
	{"POST", "/2021-01-01/opensearch/vpcEndpoints", "CreateVpcEndpoint"},
	{"GET", "/2021-01-01/opensearch/vpcEndpoints", "ListVpcEndpoints"},
	{"DELETE", "/2021-01-01/opensearch/vpcEndpoints/{VpcEndpointId}", "DeleteVpcEndpoint"},
	{"POST", "/2021-01-01/opensearch/vpcEndpoints/describe", "DescribeVpcEndpoints"},
	{"POST", "/2021-01-01/opensearch/vpcEndpoints/update", "UpdateVpcEndpoint"},
	{"POST", "/2021-01-01/packages", "CreatePackage"},
	{"DELETE", "/2021-01-01/packages/{PackageID}", "DeletePackage"},
	{"GET", "/2021-01-01/packages/{PackageID}/domains", "ListDomainsForPackage"},
	{"GET", "/2021-01-01/packages/{PackageID}/history", "GetPackageVersionHistory"},
	{"POST", "/2021-01-01/packages/associate/{PackageID}/{DomainName}", "AssociatePackage"},
	{"POST", "/2021-01-01/packages/associateMultiple", "AssociatePackages"},
	{"POST", "/2021-01-01/packages/describe", "DescribePackages"},
	{"POST", "/2021-01-01/packages/dissociate/{PackageID}/{DomainName}", "DissociatePackage"},
	{"POST", "/2021-01-01/packages/dissociateMultiple", "DissociatePackages"},
	{"POST", "/2021-01-01/packages/update", "UpdatePackage"},
	{"POST", "/2021-01-01/packages/updateScope", "UpdatePackageScope"},
	{"POST", "/2021-01-01/tags-removal", "RemoveTags"},
	{"POST", "/2021-01-01/tags", "AddTags"},
	{"GET", "/2021-01-01/tags/", "ListTags"},
	{"GET", "/2015-01-01/domain", "ListDomainNames"},
	{"GET", "/2015-01-01/domain/{DomainName}/packages", "ListPackagesForDomain"},
	{"DELETE", "/2015-01-01/es/ccs/inboundConnection/{ConnectionId}", "DeleteInboundCrossClusterSearchConnection"},
	{"PUT", "/2015-01-01/es/ccs/inboundConnection/{ConnectionId}/accept", "AcceptInboundCrossClusterSearchConnection"},
	{"PUT", "/2015-01-01/es/ccs/inboundConnection/{ConnectionId}/reject", "RejectInboundCrossClusterSearchConnection"},
	{"POST", "/2015-01-01/es/ccs/inboundConnection/search", "DescribeInboundCrossClusterSearchConnections"},
	{"POST", "/2015-01-01/es/ccs/outboundConnection", "CreateOutboundCrossClusterSearchConnection"},
	{"DELETE", "/2015-01-01/es/ccs/outboundConnection/{ConnectionId}", "DeleteOutboundCrossClusterSearchConnection"},
	{"POST", "/2015-01-01/es/ccs/outboundConnection/search", "DescribeOutboundCrossClusterSearchConnections"},
	{"GET", "/2015-01-01/es/compatibleVersions", "GetCompatibleElasticsearchVersions"},
	{"POST", "/2015-01-01/es/domain-info", "DescribeElasticsearchDomains"},
	{"POST", "/2015-01-01/es/domain", "CreateElasticsearchDomain"},
	{"DELETE", "/2015-01-01/es/domain/{DomainName}", "DeleteElasticsearchDomain"},
	{"GET", "/2015-01-01/es/domain/{DomainName}", "DescribeElasticsearchDomain"},
	{"POST", "/2015-01-01/es/domain/{DomainName}/authorizeVpcEndpointAccess", "AuthorizeVpcEndpointAccess"},
	{"GET", "/2015-01-01/es/domain/{DomainName}/autoTunes", "DescribeDomainAutoTunes"},
	{"POST", "/2015-01-01/es/domain/{DomainName}/clone", "UpdateClone"},
	{"GET", "/2015-01-01/es/domain/{DomainName}/config", "DescribeElasticsearchDomainConfig"},
	{"POST", "/2015-01-01/es/domain/{DomainName}/config", "UpdateElasticsearchDomainConfig"},
	{"POST", "/2015-01-01/es/domain/{DomainName}/config/cancel", "CancelDomainConfigChange"},
	{"GET", "/2015-01-01/es/domain/{DomainName}/listVpcEndpointAccess", "ListVpcEndpointAccess"},
	{"GET", "/2015-01-01/es/domain/{DomainName}/progress", "DescribeDomainChangeProgress"},
	{"POST", "/2015-01-01/es/domain/{DomainName}/revokeVpcEndpointAccess", "RevokeVpcEndpointAccess"},
	{"GET", "/2015-01-01/es/domain/{DomainName}/vpcEndpoints", "ListVpcEndpointsForDomain"},
	{"GET", "/2015-01-01/es/instanceTypeLimits/{ElasticsearchVersion}/{InstanceType}", "DescribeElasticsearchInstanceTypeLimits"},
	{"GET", "/2015-01-01/es/instanceTypes/{ElasticsearchVersion}", "ListElasticsearchInstanceTypes"},
	{"POST", "/2015-01-01/es/purchaseReservedInstanceOffering", "PurchaseReservedElasticsearchInstanceOffering"},
	{"GET", "/2015-01-01/es/reservedInstanceOfferings", "DescribeReservedElasticsearchInstanceOfferings"},
	{"GET", "/2015-01-01/es/reservedInstances", "DescribeReservedElasticsearchInstances"},
	{"DELETE", "/2015-01-01/es/role", "DeleteElasticsearchServiceRole"},
	{"POST", "/2015-01-01/es/serviceSoftwareUpdate/cancel", "CancelElasticsearchServiceSoftwareUpdate"},
	{"POST", "/2015-01-01/es/serviceSoftwareUpdate/start", "StartElasticsearchServiceSoftwareUpdate"},
	{"POST", "/2015-01-01/es/upgradeDomain", "UpgradeElasticsearchDomain"},
	{"GET", "/2015-01-01/es/upgradeDomain/{DomainName}/history", "GetUpgradeHistory"},
	{"GET", "/2015-01-01/es/upgradeDomain/{DomainName}/status", "GetUpgradeStatus"},
	{"GET", "/2015-01-01/es/versions", "ListElasticsearchVersions"},
	{"POST", "/2015-01-01/es/vpcEndpoints", "CreateVpcEndpoint"},
	{"GET", "/2015-01-01/es/vpcEndpoints", "ListVpcEndpoints"},
	{"DELETE", "/2015-01-01/es/vpcEndpoints/{VpcEndpointId}", "DeleteVpcEndpoint"},
	{"POST", "/2015-01-01/es/vpcEndpoints/describe", "DescribeVpcEndpoints"},
	{"POST", "/2015-01-01/es/vpcEndpoints/update", "UpdateVpcEndpoint"},
	{"POST", "/2015-01-01/packages", "CreatePackage"},
	{"DELETE", "/2015-01-01/packages/{PackageID}", "DeletePackage"},
	{"GET", "/2015-01-01/packages/{PackageID}/domains", "ListDomainsForPackage"},
	{"GET", "/2015-01-01/packages/{PackageID}/history", "GetPackageVersionHistory"},
	{"POST", "/2015-01-01/packages/associate/{PackageID}/{DomainName}", "AssociatePackage"},
	{"POST", "/2015-01-01/packages/describe", "DescribePackages"},
	{"POST", "/2015-01-01/packages/dissociate/{PackageID}/{DomainName}", "DissociatePackage"},
	{"POST", "/2015-01-01/packages/update", "UpdatePackage"},
	{"POST", "/2015-01-01/tags-removal", "RemoveTags"},
	{"POST", "/2015-01-01/tags", "AddTags"},
	{"GET", "/2015-01-01/tags/", "ListTags"},
}
