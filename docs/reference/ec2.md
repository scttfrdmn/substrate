# EC2

**Endpoint:** `ec2.{region}.amazonaws.com`
**Protocol:** AWS Query (form-encoded, `Action=` parameter)

## Supported operations

**All 114 operations the EC2 plugin routes have a row below.** The table and the plugin's dispatch
switch agree in both directions — no row names an action the switch does not handle, and no action
the switch handles is missing a row. Thirty-six of the rows were added by
[#1094](https://github.com/scttfrdmn/substrate/issues/1094), which is also where the count came
from: it is stated here so that the next reader of this section corrects a number rather than
re-deriving one. The table is hand-maintained — only the [coverage matrix](/services#coverage-matrix) is
generated — so a new `case` in the switch has to be given a row by hand.

| Operation | Notes |
|-----------|-------|
| RunInstances | A launch naming no subnet lands in the [default VPC](#the-default-vpc)'s default subnet for its `Placement.AvailabilityZone` (zone a when it names none), and is refused with `VPCIdNotSpecified` when the account has none or `InvalidInput` when the zone has no default subnet; a `SubnetId` naming no subnet is `InvalidSubnetID.NotFound` ([#1551](https://github.com/scttfrdmn/substrate/issues/1551)); `InstanceMarketOptions.MarketType=spot` [launches a Spot Instance with its own request](#spot-instance-requests); [requires an AMI that resolves](#runinstances-requires-a-resolvable-ami), from the caller's own images or the [bundled catalog](#which-amis-resolve), and [resolves an `ImageId` of `resolve:ssm:<parameter>`](#an-imageid-may-name-a-systems-manager-parameter); [refuses an AMI whose architecture the instance type cannot run](#an-ami-the-instance-type-cannot-run-is-refused); refuses a launch template identifier [of the wrong shape](#a-missing-launch-template-is-refused-by-the-identifier-named); [merges a named launch template field by field](#a-launch-template-merges-with-the-request-field-by-field); [validates MinCount/MaxCount](#mincount-and-maxcount); [refuses an invalid block device mapping](#a-mapping-aws-refuses-is-refused-with-invalidblockdevicemapping); reports [`groupSet`](#security-groups-on-an-instance), [`blockDeviceMapping`](#an-instance-reports-its-own-block-devices), [`placement`](#an-instance-s-architecture-and-placement-group) (with the placement group named by `Placement.GroupName` or `Placement.GroupId`, which must exist — naming both is `InvalidParameterCombination`), [`architecture`](#an-instance-s-architecture-and-placement-group) and [`metadataOptions`](#instance-metadata-options); the launched instance is born `pending` — see [Seeding an instance-state progression](#seeding-an-instance-state-progression) |
| DescribeInstances | [Explicit resource IDs](#explicit-resource-ids); reports [`groupSet`](#security-groups-on-an-instance), [`blockDeviceMapping`](#an-instance-reports-its-own-block-devices), [`placement`](#an-instance-s-architecture-and-placement-group), [`architecture`](#an-instance-s-architecture-and-placement-group) and [`metadataOptions`](#instance-metadata-options); fifteen filters, `instance-lifecycle`, `spot-instance-request-id`, `architecture` and `placement-group-name` among them, and [filter names are checked](#one-rule-for-an-unrecognized-filter-name). Paginates on `MaxResults`/`NextToken`, counting instances rather than reservations — see [One offset paginator, shared](#one-offset-paginator-shared) |
| TerminateInstances | [Explicit resource IDs](#explicit-resource-ids); [honours termination protection, per Availability Zone](#termination-protection-is-honored-one-availability-zone-at-a-time); answers `shutting-down` as its own `currentState` — see [Seeding an instance-state progression](#seeding-an-instance-state-progression) |
| StopInstances | [Explicit resource IDs](#explicit-resource-ids); answers `stopping` as its own `currentState`, and refuses a `terminated` instance — see [Seeding an instance-state progression](#seeding-an-instance-state-progression) |
| StartInstances | [Explicit resource IDs](#explicit-resource-ids); answers `pending` as its own `currentState`, and refuses a `terminated` instance — see [Seeding an instance-state progression](#seeding-an-instance-state-progression) |
| RebootInstances | `InstanceId.N` is `Required: Yes` and **nothing reads it**: the operation answers `<return>true</return>` without resolving an ID or touching state. An unchanged `instanceState` is the nominal answer — AWS documents the reboot as asynchronous (*"it only queues a request to reboot"*) and as ignoring a terminated instance — so what diverges is only the refusal: an absent or unknown `InstanceId.N` is answered identically, where every other instance operation [is decided against every ID it names](#an-operation-naming-resources-by-id-is-decided-against-every-one-of-them) |
| DescribeInstanceStatus | [Explicit resource IDs](#explicit-resource-ids); three of eighteen filters, and [filter names are checked](#one-rule-for-an-unrecognized-filter-name); reports `availabilityZone`, and a [seeded transient state](#seeding-an-instance-state-progression) both in `instanceState` and to its own `instance-state-name` filter. Paginates on `MaxResults`/`NextToken`, with no published range — see [One offset paginator, shared](#one-offset-paginator-shared) |
| DescribeInstanceAttribute | Five attributes, scalars `<value>`-wrapped — see [Instance attributes](#instance-attributes) |
| ModifyInstanceAttribute | `InstanceType.Value`, `UserData.Value`, `DisableApiTermination.Value`; the first two [require a stopped instance](#instance-attributes) |
| ModifyInstanceMetadataOptions | Each of the five options named replaces the instance's and the rest are kept, validated against the published values (`InvalidParameterValue`). The response's `instanceMetadataOptions` reports `pending` and every later `DescribeInstances` `applied`. A `terminated` instance is `IncorrectInstanceState`. See [Instance metadata options](#instance-metadata-options) |
| CreateVpc | Stores `TagSpecification.N` scoped to `vpc` and `InstanceTenancy` (`default` or `dedicated`; anything else, `host` included, is `InvalidParameterValue`, since API_CreateVpc says host "cannot be used with this parameter"), both dropped before #1519. Renders the same VPC `DescribeVpcs` does, `ownerId`, `instanceTenancy` and `tagSet` included — see [Twelve describes gained filters](#twelve-describes-gained-filters). Creates the VPC's main route table and, as of [#1492](https://github.com/scttfrdmn/substrate/issues/1492), its security group named `default` — the self-referencing all-traffic ingress rule and the all-traffic egress rule to `0.0.0.0/0` — see [A VPC's dependents](#a-vpc-s-dependents). As of [#1614](https://github.com/scttfrdmn/substrate/issues/1614) it also creates the VPC's default network ACL and associates the VPC with the Region's default DHCP option set — see [Network ACLs, DHCP option sets and flow logs](#network-acls-dhcp-option-sets-and-flow-logs) |
| CreateDefaultVpc | Creates the [default VPC](#the-default-vpc) and everything AWS creates with one; `DefaultVpcAlreadyExists` when the Region has one. Clears the tombstone a deleted default VPC leaves |
| CreateDefaultSubnet | A `/20` default subnet in the named zone of the [default VPC](#the-default-vpc), by `AvailabilityZone` or `AvailabilityZoneId`; refuses with `DefaultVpcDoesNotExist`, `DefaultSubnetAlreadyExistsInAvailabilityZone` or `InvalidAvailabilityZone`. `Ipv6Native` is unread |
| DescribeNetworkInterfaces | The interfaces live instances recorded from `NetworkInterface.N` at launch; substrate mints none for an instance launched without one, or for a Lambda function or load balancer. [Explicit resource IDs](#explicit-resource-ids) (`InvalidNetworkInterfaceID.NotFound`/`.Malformed`); sixteen filters evaluated and the rest inert, with [filter names checked](#one-rule-for-an-unrecognized-filter-name); paginates on `MaxResults`/`NextToken` over 5–1000 (#1519) |
| DescribeVpcs | [Explicit resource IDs](#explicit-resource-ids); seven of fifteen filters, and [filter names are checked](#one-rule-for-an-unrecognized-filter-name); reports `state`, `ownerId`, `dhcpOptionsId` and `tagSet`. Paginates on `MaxResults`/`NextToken`, over the published 5–1000 range — see [One offset paginator, shared](#one-offset-paginator-shared) |
| ModifyVpcAttribute | `VpcId` is required and checked ([Explicit resource IDs](#explicit-resource-ids)). `EnableDnsSupport.Value`, `EnableDnsHostnames.Value` and `EnableNetworkAddressUsageMetrics.Value` are read under the spellings the page publishes, and each is readable back through `DescribeVpcAttribute` ([#1151](https://github.com/scttfrdmn/substrate/issues/1151); until then the first two were read as `EnableDNSSupport.Value`/`EnableDNSHostnames.Value`, which no SDK sends, and every modify was discarded). One attribute per request, and hostnames only where DNS support is enabled ([#1554](https://github.com/scttfrdmn/substrate/issues/1554)) — see [VPC attributes and internet gateways](#vpc-attributes-and-internet-gateways) |
| DescribeVpcAttribute | `VpcId` and `Attribute` are required; `Attribute` is one of `enableDnsSupport`, `enableDnsHostnames` and `enableNetworkAddressUsageMetrics` (`InvalidParameterValue` otherwise), and the response carries `vpcId` and that one attribute as `value` — the shape of the page's samples. A new VPC answers DNS support on and hostnames off; the [default VPC](#the-default-vpc) has both on. Network Address Usage metrics is a recorded flag: no metric is produced |
| DeleteVpc | [Explicit resource IDs](#explicit-resource-ids). Refused with `DependencyViolation` while the VPC holds a subnet, an attached internet gateway, a security group other than `default`, a route table other than its main one, a network ACL other than its default one, a NAT gateway not yet `deleted`, or an instance not yet `terminated`, and nothing is written; otherwise its `default` security group, main route table and default network ACL go with it — see [A VPC's dependents](#a-vpc-s-dependents). Deleting the [default VPC](#the-default-vpc) leaves a tombstone, so it is not seeded again |
| CreateSubnet | `TagSpecification.N` scoped to `subnet`, and it renders the same subnet `DescribeSubnets` does — see [A subnet reports its tags, and filters on them](#a-subnet-reports-its-tags-and-filters-on-them). The subnet is associated with its VPC's default network ACL ([#1614](https://github.com/scttfrdmn/substrate/issues/1614)) |
| DescribeSubnets | [Explicit resource IDs](#explicit-resource-ids); fourteen filters, and [filter names are checked](#one-rule-for-an-unrecognized-filter-name); [reports `tagSet`, `subnetArn`, `ownerId` and `defaultForAz`](#a-subnet-reports-its-tags-and-filters-on-them). Paginates on `MaxResults`/`NextToken`, over the published 5–1000 range — see [One offset paginator, shared](#one-offset-paginator-shared) |
| ModifySubnetAttribute | `SubnetId` is required and checked ([Explicit resource IDs](#explicit-resource-ids)). `MapPublicIpOnLaunch` is the one modelled attribute — it decides whether a launch into the subnet gets a public IPv4 address, and it is readable back through [`DescribeSubnets`](#a-subnet-reports-its-tags-and-filters-on-them) — and the other ten published parameters are unread. It is read as `MapPublicIpOnLaunch.Value`, the page's own spelling ([#1151](https://github.com/scttfrdmn/substrate/issues/1151); until then it was read as `MapPublicIPOnLaunch.Value` and every SDK's modify was discarded). AWS's *"You can only modify one attribute at a time"* is not enforced, which is inert while one attribute is modelled |
| DeleteSubnet | [Explicit resource IDs](#explicit-resource-ids). A subnet still holding an instance that is not terminated, or a NAT gateway not yet `deleted`, is `DependencyViolation` ([#1551](https://github.com/scttfrdmn/substrate/issues/1551)) — see [What a delete refuses on](#what-a-delete-refuses-on). A deleted subnet's network ACL association goes with it |
| CreateSecurityGroup | `TagSpecification.N` scoped to `security-group`, answered in `tagSet`; an absent `VpcId` places the group in the [default VPC](#the-default-vpc), or is `VPCIdNotSpecified` without one; the name `default` is reserved (`InvalidParameterValue`), and a name another group in the same VPC holds, compared case-insensitively, is `InvalidGroup.Duplicate` ([#1491](https://github.com/scttfrdmn/substrate/issues/1491)); a `VpcId` naming no VPC is `InvalidVpcID.NotFound` — see [Security group tags and rules](#security-group-tags-and-rules) |
| DescribeSecurityGroups | [Explicit resource IDs](#explicit-resource-ids); `GroupName.N` selects among the [default VPC](#the-default-vpc)'s groups only, as the page scopes it, case-insensitively ([#1491](https://github.com/scttfrdmn/substrate/issues/1491)); reports `ownerId` and `tagSet`, and evaluates every filter it documents, `tag:<key>` and the rule filters included — see [Security group tags and rules](#security-group-tags-and-rules); [filter names are checked](#one-rule-for-an-unrecognized-filter-name). Paginates on `MaxResults`/`NextToken`, over the published 5–1000 range, and `GroupName.N` does not conflict with `MaxResults` — see [One offset paginator, shared](#one-offset-paginator-shared) |
| DeleteSecurityGroup | [Explicit resource IDs](#explicit-resource-ids). `GroupName` names a group in the [default VPC](#the-default-vpc), case-insensitively, and one that resolves to nothing there is `InvalidGroup.NotFound`; `GroupId` wins when both are sent ([#1491](https://github.com/scttfrdmn/substrate/issues/1491)). A VPC's `default` group is `CannotDelete`, and a group an instance carries or another group's rule references is `DependencyViolation` ([#1551](https://github.com/scttfrdmn/substrate/issues/1551)) — see [What a delete refuses on](#what-a-delete-refuses-on) |
| AuthorizeSecurityGroupIngress | Supports source security groups (`IpPermissions.N.Groups.M.GroupId`), including self-referencing rules. A source the group already allows for the same protocol and ports is `InvalidPermission.Duplicate`, checked per source, and nothing is written ([#1491](https://github.com/scttfrdmn/substrate/issues/1491)). IPv4 and IPv6 CIDRs are [canonicalised](#security-group-tags-and-rules) before they are compared or stored, an unparseable one is `InvalidParameterValue`, and `GroupName` names a group in the [default VPC](#the-default-vpc) ([#1553](https://github.com/scttfrdmn/substrate/issues/1553)) |
| AuthorizeSecurityGroupEgress | Supports destination security groups. A duplicate is refused as for `AuthorizeSecurityGroupIngress` — including the all-traffic `0.0.0.0/0` rule every new group carries — and CIDRs and `GroupName` are read as there |
| RevokeSecurityGroupIngress | Matches on protocol, ports, **and** source, and removes [each named source on its own](#security-group-tags-and-rules), not the permission it was authorized in; a rule the group does not hold is `InvalidPermission.NotFound` outside the default VPC and `unknownIpPermissionSet` inside it. `SecurityGroupRuleId.N` is unread. CIDRs are canonicalised and `GroupName` is read as for `AuthorizeSecurityGroupIngress`, and a rule stored before canonicalisation still matches |
| RevokeSecurityGroupEgress | As `RevokeSecurityGroupIngress` |
| CreateInternetGateway | Renders the same gateway `DescribeInternetGateways` does — see [Twelve describes gained filters](#twelve-describes-gained-filters) |
| AttachInternetGateway | `InternetGatewayId` and `VpcId` are both required and resolved ([Explicit resource IDs](#explicit-resource-ids)). A gateway already attached — to this VPC or another — is `Resource.AlreadyAssociated`, whose client-error-table entry reads *"You also cannot attach an internet gateway to more than one VPC at a time"* ([#1194](https://github.com/scttfrdmn/substrate/issues/1194)). A VPC that already has a gateway attached is `InvalidParameterValue` ([#1554](https://github.com/scttfrdmn/substrate/issues/1554)) — see [VPC attributes and internet gateways](#vpc-attributes-and-internet-gateways) |
| DetachInternetGateway | `InternetGatewayId` and `VpcId` are both required ([#1194](https://github.com/scttfrdmn/substrate/issues/1194)), and the named VPC is removed from the gateway's `attachmentSet`, so the detach is visible through `DescribeInternetGateways`. A VPC the gateway is not attached to — including a gateway attached to nothing — is `Gateway.NotAttached`, the code the client-error table gives for *"If you are trying to detach an internet gateway, ensure that you specify the correct VPC"*; the page itself publishes none. AWS's precondition — *"The VPC must not contain any running instances with Elastic IP addresses or public IPv4 addresses"* — is **not enforced**, because the page states it with no code |
| DescribeInternetGateways | [Explicit resource IDs](#explicit-resource-ids); **all six** filters, and [filter names are checked](#one-rule-for-an-unrecognized-filter-name); reports `ownerId`, `attachmentSet` and `tagSet`. Paginates on `MaxResults`/`NextToken`, over the published 5–1000 range — see [One offset paginator, shared](#one-offset-paginator-shared) |
| DeleteInternetGateway | [Explicit resource IDs](#explicit-resource-ids). A gateway still attached is `DependencyViolation` — *"You must detach the internet gateway from the VPC before you can delete it"* ([#1492](https://github.com/scttfrdmn/substrate/issues/1492)) |
| DescribeAvailabilityZones | Three zones per region, from the same list the offerings and spot-price operations use — see [Instance types are a seeded catalog](#instance-types-are-a-seeded-catalog). `ZoneName.N`, `ZoneId.N`, and four of eleven filters. `zoneId` takes AWS's published shape (`use1-az1`) and always maps zone `a` to `-az1` — see [Zone IDs](#zone-ids-take-aws-s-published-shape-and-always-map-zone-a-to-az1) |
| DescribeRegions | **All three** filters, and [filter names are checked](#one-rule-for-an-unrecognized-filter-name). `AllRegions` is accepted and inert — every seeded region is `opt-in-not-required`, so it is already in the answer |
| DescribeInstanceTypes | Answers from a [seeded catalog](#instance-types-are-a-seeded-catalog). `InstanceType.N` is an assertion: a type outside the catalog is refused with `InvalidInstanceType`. Six of fifty-six filters, and [filter names are checked](#one-rule-for-an-unrecognized-filter-name). Paginates on `MaxResults`/`NextToken`, over the published 5–**100** range, and `InstanceType.N` does not conflict with `MaxResults` — see [One offset paginator, shared](#one-offset-paginator-shared) |
| DescribeInstanceTypeOfferings | `instance-type` and `location` filters (both with [wildcards](#wildcards-in-filter-values)) and the `LocationType` parameter; an unmatched filter is an empty answer, not an error. Paginates on `MaxResults`/`NextToken`, over the published 5–1000 range and counting offerings rather than types — see [One offset paginator, shared](#one-offset-paginator-shared) |
| DescribeSpotPriceHistory | One stub price per catalog type per zone. `InstanceType.N` here is a *filter*, so an unknown type is an empty history — [see below](#instance-types-are-a-seeded-catalog). `ProductDescription.N` is read at every index, and five of six filters. Paginates on `MaxResults`/`NextToken`, with no published range and no `InvalidParameterCombination` — one of the three converted describes with no ID-list parameter, see [One offset paginator, shared](#one-offset-paginator-shared) |
| GetSpotPlacementScores | Scores the three seeded regions, or their nine zones under `SingleAvailabilityZone=true`, by AZ **ID**. `TargetCapacity` is required and range-checked; `InstanceType.N` and `RegionName.N` are **singular**; `MaxResults` floors at **10**, which is this operation's own published range. The score itself is [seeded, not computed](#seeding-a-spot-placement-score) |
| RequestSpotInstances | Launches `InstanceCount` Spot Instances from `LaunchSpecification`, one request per instance, active/`fulfilled` at once — see [Spot Instance requests](#spot-instance-requests). `TagSpecification.N` must be `spot-instances-request` |
| DescribeSpotInstanceRequests | `SpotInstanceRequestId.N` asserts existence (`InvalidSpotInstanceRequestID.NotFound`); twenty-two of forty filters, `state`, `instance-id` and `tag:<key>` among them; paginates on `MaxResults`/`NextToken` with no published range; fulfills an open request a cleared seed no longer blocks — see [Spot Instance requests](#spot-instance-requests) |
| CancelSpotInstanceRequests | Moves each request to `cancelled` without terminating its instance; an unknown ID cancels nothing — see [Spot Instance requests](#spot-instance-requests) |
| CreateRouteTable | |
| AssociateRouteTable | |
| ReplaceRouteTableAssociation | Both IDs resolve before anything is written: an unknown `RouteTableId` is refused through [the route-table kind](#explicit-resource-ids) and an `AssociationId` naming nothing answers `InvalidAssociationID.NotFound`/400 — the ordering #713 fixed, since the refusals used to sit after the source association had already been removed, so a bogus target destroyed the association it was asked to move. A fresh `rtbassoc-` is minted on replacement, matching AWS, and the subnet and main-table flag move with it |
| DisassociateRouteTable | `AssociationId` is resolved by scanning the region's route tables, and the matching association is removed, so the disassociation is visible through `DescribeRouteTables`. Unlike `ReplaceRouteTableAssociation` it answers `return=true` for an `AssociationId` naming nothing rather than `InvalidAssociationID.NotFound`, and the main-table association — which AWS refuses to disassociate — is removed like any other |
| DescribeRouteTables | [Explicit resource IDs](#explicit-resource-ids); [filter names are checked](#one-rule-for-an-unrecognized-filter-name). Paginates on `MaxResults`/`NextToken`, over the published 5–**100** range — the ceiling two of its siblings do not share, see [One offset paginator, shared](#one-offset-paginator-shared) |
| DeleteRouteTable | [Explicit resource IDs](#explicit-resource-ids) |
| CreateRoute | `RouteTableId` is required and checked ([Explicit resource IDs](#explicit-resource-ids)); the route is appended with `state` `active`. All twelve published targets are read and each renders under its own `Route` element — see [Route targets](#route-targets) ([#1187](https://github.com/scttfrdmn/substrate/issues/1187)). No destination, or no target, is `MissingParameter`; two destinations, or two targets, is `InvalidParameterCombination`. A `DestinationCidrBlock` is canonicalised (`100.68.0.18/18` is stored as `100.68.0.0/18`), a destination the table already routes is `RouteAlreadyExists`, and an internet gateway, NAT gateway or instance target that does not exist is `InvalidGatewayID.NotFound`, `InvalidNatGatewayID.NotFound` or `InvalidInstanceID.NotFound`. A NAT gateway that is `deleted` or `failed`, or an instance that is `shutting-down` or `terminated`, is stored `blackhole` |
| ReplaceRoute | `RouteTableId` is required and checked; the route matching the destination exactly has its target and `state` rewritten, and a destination no route carries answers `InvalidRoute.NotFound`/400. The destination and target are read as `CreateRoute` reads them, plus `LocalTarget=true` for `local`, and the previous target is cleared — see [Route targets](#route-targets) |
| DeleteRoute | `RouteTableId` is required and checked; the route matching `DestinationCidrBlock` (canonicalised as `CreateRoute` stores it), `DestinationIpv6CidrBlock` or `DestinationPrefixListId` is dropped, and a request naming none is `MissingParameter`. A destination no route carries is `InvalidRoute.NotFound`, as `ReplaceRoute` answers it — the client-error table's gloss reads *"Ensure that you indicate the exact CIDR range for the route in the request"*. Until [#1187](https://github.com/scttfrdmn/substrate/issues/1187) it answered `return=true` |
| CreateNetworkAcl | A new ACL in `VpcId` (`InvalidVpcID.NotFound` when it names nothing) holding only the two catch-all denies, rule 32767 in each direction; `TagSpecification.N` scoped to `network-acl`. A `ClientToken` is echoed, not used to deduplicate — see [Network ACLs, DHCP option sets and flow logs](#network-acls-dhcp-option-sets-and-flow-logs) |
| DeleteNetworkAcl | `InvalidNetworkAclID.NotFound` for an unknown ACL; a default ACL is `InvalidParameterValue` / "cannot delete default network ACL acl-…", and one still associated with a subnet is `DependencyViolation` |
| DescribeNetworkAcls | [Explicit resource IDs](#explicit-resource-ids); **all nineteen** filters, the `association.*` and `entry.*` families included, and [filter names are checked](#one-rule-for-an-unrecognized-filter-name). Paginates on `MaxResults`/`NextToken`, over the published 5–1000 range. A VPC or subnet recorded before #1614 reads its default ACL and association under IDs derived from its own — see [Network ACLs, DHCP option sets and flow logs](#network-acls-dhcp-option-sets-and-flow-logs) |
| CreateNetworkAclEntry | Rule numbers 1–32766; a rule number the direction already holds is `NetworkAclEntryAlreadyExists` unless the stored rule is identical, which succeeds unchanged; more than 20 rules in one direction is `NetworkAclEntryLimitExceeded`. Protocol names are stored as numbers, the CIDR is canonicalised, and a port range or ICMP type and code is kept only for the protocols that take one |
| ReplaceNetworkAclEntry | The rule with the number and direction is replaced whole; one that does not exist is `InvalidNetworkAclEntry.NotFound` |
| DeleteNetworkAclEntry | A rule that does not exist is `InvalidNetworkAclEntry.NotFound`; the catch-all deny cannot be named |
| ReplaceNetworkAclAssociation | `AssociationId` names the subnet's current association (`InvalidAssociationID.NotFound`) and `NetworkAclId` the new ACL, which must be in the subnet's VPC; a fresh `aclassoc-` ID is answered as `newAssociationId` |
| CreateDhcpOptions | `DhcpConfiguration.N` with the six documented keys, each at most once and within its documented value count; `TagSpecification.N` scoped to `dhcp-options` |
| DeleteDhcpOptions | `InvalidDhcpOptionsID.NotFound` for an unknown set; a set any VPC uses is `DependencyViolation` / "The dhcpOptions 'dopt-…' has dependencies and cannot be deleted." Deleting the Region's default set means later VPCs are created with none |
| DescribeDhcpOptions | The Region's default set and the caller's; `DhcpOptionsId.N` naming nothing is `InvalidDhcpOptionID.NotFound` — the reference's other spelling; **all six** filters. Paginates over the published 5–1000 range |
| AssociateDhcpOptions | `default` associates the VPC with no set, and DescribeVpcs then renders `dhcpOptionsId` as `default`; an unknown VPC or set is `InvalidVpcID.NotFound` or `InvalidDhcpOptionsID.NotFound` |
| CreateFlowLogs | `ResourceType` `VPC`, `Subnet` or `NetworkInterface`; each resource must exist, or the whole request is its kind's `NotFound`. Validates the destination parameters as the page states them. A resource already holding a flow log with the same traffic type and destination is an `Unsuccessful` item carrying `FlowLogAlreadyExists`. Nothing is delivered — see [Network ACLs, DHCP option sets and flow logs](#network-acls-dhcp-option-sets-and-flow-logs) |
| DescribeFlowLogs | `FlowLogId.N` narrows rather than asserts; **all eight** filters; `deliverLogsStatus` is `SUCCESS` and `flowLogStatus` `ACTIVE`. Paginates with no published range |
| DeleteFlowLogs | An ID naming no flow log is a top-level `InvalidFlowLogId.NotFound`, and nothing is deleted |
| CreateVolume | `TagSpecification.N` scoped to `volume`, validated [before the volume is written](#a-tagged-create-is-authorized-twice) — see [A volume carries tags](#a-volume-carries-tags). Exactly one of `AvailabilityZone`/`AvailabilityZoneId` and at least one of `Size`/`SnapshotId` are required, both refused with `InvalidParameterCombination`; a named snapshot is resolved, must be observed `completed` (`IncorrectState`), and sets the size floor — see [`CreateVolume` invents neither a size nor a zone](#createvolume-invents-neither-a-size-nor-a-zone). `VolumeType` defaults to `gp2`, and `Iops`/`Throughput` are stored without the per-type rules AWS publishes for them |
| DescribeVolumes | [Explicit resource IDs](#explicit-resource-ids) — `VolumeId.N` [asserts existence](#which-selectors-assert-existence) as of [#731](https://github.com/scttfrdmn/substrate/issues/731), where it used to answer a superset; eleven of twenty filters, and [filter names are checked](#one-rule-for-an-unrecognized-filter-name); reports `tagSet` and the volume's `attachmentSet`. Paginates on `MaxResults`/`NextToken`, with **no published range** — so a floor of 1 and no ceiling, and `VolumeId.N` with `MaxResults` is `InvalidParameterCombination`, see [One offset paginator, shared](#one-offset-paginator-shared) |
| DeleteVolume | `VolumeId` is required and checked ([Explicit resource IDs](#explicit-resource-ids)); an `in-use` volume is refused with `VolumeInUse`/400, and the record is deleted rather than marked, so the delete is **not** idempotent |
| AttachVolume | `VolumeId`, `InstanceId` and `Device` are all `Required: Yes`; the first two are resolved and a missing one answers `MissingParameter`, while `Device` defaults to `/dev/xvdf` rather than being required. A volume not `available` is refused with `IncorrectState`. The attachment is recorded on the volume, with `deleteOnTermination` `false` — but **not** on the instance, so [an instance reports its own block devices](#an-instance-reports-its-own-block-devices) from its launch mapping alone and an attached volume does not appear there |
| DetachVolume | `VolumeId` is required and checked; the response reports the attachment as it was before the detach — the previous `instanceId` and `device` — with `status` `detached`, and the volume's `attachmentSet` is then cleared and its state set to `available`. `InstanceId`, `Device` and `Force` are accepted and inert, so a detach naming the wrong instance still succeeds, and a volume that carried no attachment is detached rather than refused |
| CreateSnapshot | `VolumeId` is required and checked; `volumeSize` and `encrypted` come from the source volume, and `status` is `completed` at once — see [A snapshot has a real size](#a-snapshot-has-a-real-size) |
| CreateSnapshots | Snapshots every volume attached to `InstanceSpecification.InstanceId`, in one atomic call: a request refused for any one volume writes nothing. `CopyTagsFromSource=volume` copies each volume's tags, with the request's `TagSpecification.N` winning a collision. The response element is `snapshotSet` of `SnapshotInfo`, which names the state member **`state`** where `CreateSnapshot` and `DescribeSnapshots` both name it `status`, and each snapshot in the set carries its own [seeded countdown](#seeding-a-snapshot-progression) — see [The rest of the snapshot family](#the-rest-of-the-snapshot-family) |
| DescribeSnapshots | [Explicit resource IDs](#explicit-resource-ids); ten filters, and [filter names are checked](#one-rule-for-an-unrecognized-filter-name); `Owner.N` and `RestorableBy.N` — see [A snapshot filters on its own members](#a-snapshot-filters-on-its-own-members-and-scopes-by-account). Paginates on `MaxResults`/`NextToken`, as do `DescribeVolumes` and `DescribeImages` — see [One offset paginator, shared](#one-offset-paginator-shared) |
| DeleteSnapshot | `SnapshotId` is required and checked; refuses one a registered AMI still references with `InvalidSnapshot.InUse`, and is **not** idempotent — see [Deleting a snapshot](#deleting-a-snapshot-refuses-what-aws-refuses) |
| CopySnapshot | A same-region copy, which is what AWS's own text makes of a single-region endpoint — `DestinationRegion` is a `PresignedUrl` artifact, not routing. `SourceRegion` and `SourceSnapshotId` are both required and the source is resolved; a `SourceRegion` that is not the request's answers `SnapshotCopyUnsupported.InterRegion`. `Encrypted=false` and a `KmsKeyId` without encryption are both refused; `TagSpecification.N` is the copy's only source of tags, and `volumeId` is a fresh ID naming no volume, as AWS's own does. The response carries `snapshotId` and `tagSet` and nothing else — see [The rest of the snapshot family](#the-rest-of-the-snapshot-family) |
| DescribeSnapshotAttribute | `createVolumePermission` and `productCodes`, one at a time; the name is validated **before** the snapshot is resolved, and both answer as a present-but-empty element rather than an omitted one — see [The rest of the snapshot family](#the-rest-of-the-snapshot-family) |
| ModifySnapshotAttribute | Both wire forms of a permission change — structured `CreateVolumePermission.Add.N`/`.Remove.N` and flat `OperationType` with `UserId.N`/`UserGroup.N`. `Group` accepts only `all`; sharing an encrypted snapshot publicly, adding *and* removing the same account ID, `productCodes`, and more than 500 modifications in one call are each refused, while adding a group while removing an account is allowed because that is AWS's own Example 2. The recorded permission grants nothing — substrate is single-account — so it is [readable intent](#the-rest-of-the-snapshot-family) |
| ResetSnapshotAttribute | `createVolumePermission` only; `productCodes` is refused. Clears the list rather than restoring a remembered default, since an empty list is what a snapshot is created with — see [The rest of the snapshot family](#the-rest-of-the-snapshot-family) |
| CreateImage | `InstanceId` and `Name` are both required. `TagSpecification.N` is honored **per `ResourceType`**, `image` and `snapshot` scoped separately — see [Tag scoping on `CreateImage`](#tag-scoping-on-createimage). A backing snapshot is always materialised, at the instance's own root volume size — see [A snapshot has a real size](#a-snapshot-has-a-real-size) — and the AMI inherits [only what the operating system decides](#an-ami-reports-its-architecture-platform-and-root-device) from its parent: architecture, platform, virtualization type and root device name, but not `ownerAlias`, `publicSsmParameterName` or public launch permissions. `imageState` is `available` at once, and the response carries `imageId` alone |
| RegisterImage | `Name` is required; `TagSpecification.N` must be scoped to `image` and any other type is refused before anything is written. [The whole block device mapping is recorded](#registerimage-records-the-whole-block-device-mapping), and each mapping naming a snapshot is resolved, so a dangling reference cannot be stored — but only that rule of the launch path's set, since the rest is not published for this operation. `Architecture` defaults to **`i386`** and `VirtualizationType` to **`paravirtual`**, which are AWS's published defaults rather than the `x86_64`/`hvm` a modern caller expects; `rootDeviceType` is derived from whether `ImageLocation` is present. Response is `imageId` alone |
| DescribeImages | [Explicit resource IDs](#explicit-resource-ids) — `ImageId.N` [asserts existence](#which-selectors-assert-existence) as of [#731](https://github.com/scttfrdmn/substrate/issues/731), where before it was not read at all; eighteen of forty-three filters, and [filter names are checked](#one-rule-for-an-unrecognized-filter-name) — see [`DescribeImages` filters](#describeimages-filters). Reports [`architecture`, `platform`, the root device and the block device mapping](#an-ami-reports-its-architecture-platform-and-root-device). `Owner.N` (#1432) and `ExecutableBy.N` (#1494) narrow the answer — see [Which AMIs resolve](#which-amis-resolve). A [bundled AMI](#which-amis-resolve) resolves by explicit ID but is not listed by an unqualified describe. Paginates on `MaxResults`/`NextToken`, with no published range, and `ImageId.N` with `MaxResults` is `InvalidParameterCombination` — see [One offset paginator, shared](#one-offset-paginator-shared) |
| DeregisterImage | `ImageId` is required; the record is deleted, so the AMI stops resolving for `RunInstances` and stops being reported by `DescribeImages` — and a [snapshot it protected](#deleting-a-snapshot-refuses-what-aws-refuses) becomes deletable. Silently idempotent: an `ImageId` naming nothing answers `return=true`, and the ID is not checked for shape, so it is the one AMI operation that does not resolve what it names |
| AllocateAddress | `Domain` defaults to `vpc` and must be `vpc` or `standard` (`InvalidParameterValue` otherwise); the `publicIp` is derived deterministically from the minted `allocationId`, so the same event log replays the same address. `TagSpecification.N` scoped to `elastic-ip` is stored (another `ResourceType` is `InvalidParameterValue`), so `DescribeAddresses` reports it and filters on it, and the Resource Groups Tagging API finds the address ([#1191](https://github.com/scttfrdmn/substrate/issues/1191)). `NetworkBorderGroup` is echoed as sent — an unknown group is not refused — and is the Region when absent. `Address` is refused, because `ReleaseAddress` keeps no record of what it released: `InvalidAddress.Malformed` for a value that is not an IPv4 address, `InvalidIPAddress.InUse` for one the account holds, `InvalidAddress.NotFound` for any other ([#1555](https://github.com/scttfrdmn/substrate/issues/1555)). `PublicIpv4Pool`, `CustomerOwnedIpv4Pool` and `IpamPoolId` are `InvalidParameterValue`: no address pools are modelled. Four of the eight published response elements are rendered — `publicIp`, `allocationId`, `domain` and `networkBorderGroup` |
| AssociateAddress | `AllocationId` is required and checked ([Explicit resource IDs](#explicit-resource-ids)), and a fresh `associationId` is minted. When `InstanceId` names an instance the instance's `publicIp` and `publicDnsName` are set, so the association is visible through `DescribeInstances` as well as `DescribeAddresses` — but `InstanceId` and `NetworkInterfaceId` are **not resolved**, so an association naming neither, or naming an instance that does not exist, is recorded and answered `return=true`. `AllowReassociation` is inert, and the deprecated EC2-Classic `PublicIp` form is not read |
| DescribeAddresses | [Explicit resource IDs](#explicit-resource-ids); `AllocationId.N` and `PublicIp.N` [union](#twelve-describes-gained-filters); eight of ten filters, and [filter names are checked](#one-rule-for-an-unrecognized-filter-name); reports `tagSet` |
| DisassociateAddress | `AssociationId` is resolved by scanning the region's addresses; the association is cleared and, where it named an instance, the instance's `publicIp` and `publicDnsName` are cleared with it. An `AssociationId` naming nothing answers `return=true` — which is **not** a divergence here: AWS publishes *"This is an idempotent operation. If you perform the operation more than once, Amazon EC2 doesn't return an error."* The page marks `AssociationId` `Required: No` while its own prose says *"This parameter is required"*, and substrate follows the prose only to the extent of reading it |
| ReleaseAddress | `AllocationId` is required and checked; an address still associated is refused with `InvalidIPAddress.InUse`/400, which is why a release is preceded by a disassociate. The record is deleted rather than marked, so the release is **not** idempotent — a second one answers `InvalidAllocationID.NotFound` |
| CreateNatGateway | `SubnetId` is required and checked ([Explicit resource IDs](#explicit-resource-ids)), and the VPC is taken from the subnet rather than the request. `TagSpecification.N` scoped to `natgateway`, validated [before the gateway is written](#a-tagged-create-is-authorized-twice). `ConnectivityType` defaults to `public`. A named `AllocationId` that resolves to no Elastic IP is `InvalidAllocationID.NotFound` (#1188). `PrivateIpAddress` is honored and must be an IPv4 address inside the subnet's CIDR block (`InvalidParameterValue` otherwise); without it `privateIp` is derived from the minted gateway ID, so it replays identically. `ClientToken` is echoed as `clientToken` and makes the create idempotent — see [Seeding a NAT gateway progression](#seeding-a-nat-gateway-progression). `state` is `available` unless a seed is in place, in which case the gateway is born `pending`. `networkInterfaceId` is not answered: no network interface is modeled |
| DescribeNatGateways | [Explicit resource IDs](#explicit-resource-ids); [filter names are checked](#one-rule-for-an-unrecognized-filter-name). Paginates on `MaxResults`/`NextToken`, over the published 5–1000 range — see [One offset paginator, shared](#one-offset-paginator-shared). `state` is what the [seeded progression](#seeding-a-nat-gateway-progression) reports, taken before the filters run, and a `failed` gateway carries `failureCode` and `failureMessage` |
| DeleteNatGateway | `NatGatewayId` is required and checked ([Explicit resource IDs](#explicit-resource-ids)). The answer is `natGatewayId` alone, as published; the `state` element substrate used to add is on no page. The record settles `deleted` and is kept, so `DescribeNatGateways` keeps reporting it — as AWS does, though AWS stops after about an hour and substrate reports it for the life of the store. A seed's count reports `deleting` first ([Seeding a NAT gateway progression](#seeding-a-nat-gateway-progression)). A second delete answers again rather than refusing |
| CreateKeyPair | `KeyName` is required (`MissingParameter`), and a name already held answers `InvalidKeyPair.Duplicate`/400. `TagSpecification.N` scoped to `key-pair`; `tagSet` is [omitted rather than empty](#an-untagged-resource-omits-tagset) for an untagged key. `KeyType` is `rsa` (the default) or `ed25519`, anything else `InvalidParameterValue`, and the key is that type (#1296): a 2048-bit RSA key as the PKCS#1 `RSA PRIVATE KEY` PEM the page publishes, fingerprinted as the colon-separated SHA-1 digest of its DER (PKCS#8) private key; or an ed25519 key in OpenSSH format, fingerprinted as the base64 SHA-256 digest of its public key. The material is [derived from the request ID](/reference/concepts/seeds-and-replay#an-identifier-a-replay-mints-is-the-one-it-recorded), so a replay answers the same key. `KeyName` is up to 255 ASCII characters, as published, and anything longer or non-ASCII is `InvalidParameterValue`. `KeyFormat` is read: `pem` (the default) is answered, and `ppk` is `InvalidParameterValue` — substrate does not generate PuTTY keys — as is any other value. The response does not render `keyType`, which this operation's Response Elements do not publish ([#1190](https://github.com/scttfrdmn/substrate/issues/1190)); `DescribeKeyPairs` reports it. The published 5,000-per-Region quota is not enforced |
| ImportKeyPair | `KeyName` and `PublicKeyMaterial` are both required, a duplicate name answers `InvalidKeyPair.Duplicate`, and `KeyName` is held to CreateKeyPair's 255-ASCII-character bound. The material is base64-decoded, falling back to the raw text when it is not base64, and **parsed** ([#1190](https://github.com/scttfrdmn/substrate/issues/1190)): an RSA or ED25519 key in the OpenSSH public key format, or an RSA key as base64 DER or an RFC 4716 file — the User Guide's supported formats. Anything else, ECDSA and DSA keys included, is `InvalidKeyPair.Format`, as is an RSA key that is not one of the "Supported lengths: 1024, 2048, and 4096". An ED25519 key's fingerprint is the base64 SHA-256 digest of its public key, CreateKeyPair's rule; an RSA key's is the colon-separated MD5 of its DER public key — see [Key pair fingerprints](#key-pair-fingerprints). `TagSpecification.N` scoped to `key-pair`; no `keyMaterial` and no `keyType` are returned, matching the published Response Elements |
| DescribeKeyPairs | `KeyName.N` and `KeyPairId.N` [union](#twelve-describes-gained-filters) and **narrow rather than assert**: an unknown name or ID is an empty answer where AWS publishes `InvalidKeyPair.NotFound` — one of the five selector families whose assertion is [unimplemented rather than declined](#which-selectors-assert-existence). **All five** filters are evaluated, and [filter names are checked](#one-rule-for-an-unrecognized-filter-name). Reports `keyPairId`, `keyName`, `keyFingerprint`, `keyType`, `createTime` and `tagSet`. No pagination, matching AWS, which publishes none |
| DeleteKeyPair | Either `KeyName` or `KeyPairId`; both absent answers `MissingParameter`, which is substrate's reading — AWS marks both `Required: No` and publishes no error for the empty request. A `key-`-prefixed value is resolved by scanning the region's key pairs, and anything else is taken as a name. Silently idempotent: a key pair naming nothing answers `return=true`, matching AWS, whose page publishes no error for a nonexistent key pair. The published `keyPairId` response element is not rendered — the answer is `return` alone |
| CreatePlacementGroup | `GroupName` is required here, where AWS marks it `Required: No`; a name already held answers `InvalidPlacementGroup.Duplicate`/400. `Strategy` defaults to `cluster` and is stored unvalidated, so `partition` is accepted without a `PartitionCount` and `SpreadLevel` is not read. `TagSpecification.N` scoped to `placement-group` — which until [#708](https://github.com/scttfrdmn/substrate/issues/708) was the only way a placement group's tags were settable at all. Renders `groupName`, `groupId`, `groupArn`, `strategy`, `state` (`available` at once) and `tagSet`; `partitionCount` is not rendered |
| DescribePlacementGroups | `GroupName.N` and `GroupId.N` [union](#twelve-describes-gained-filters) and narrow rather than assert, where AWS publishes `InvalidPlacementGroup.Unknown` — see [Which selectors assert existence](#which-selectors-assert-existence). Six of seven filters, `spread-level` inert, and [filter names are checked](#one-rule-for-an-unrecognized-filter-name); the page publishes **no `group-id` filter**, so `group-arn` is the ID-shaped one. Reports `tagSet`, [omitted rather than empty](#an-untagged-resource-omits-tagset) for an untagged group. No pagination, matching AWS |
| DeletePlacementGroup | `GroupName` is required and **resolved**: a name naming nothing answers `InvalidPlacementGroup.Unknown`/400, so the delete is not idempotent. A group holding an instance that is not terminated is `InvalidPlacementGroup.InUse` ([#1193](https://github.com/scttfrdmn/substrate/issues/1193)), per *"You must terminate all instances in the placement group before you can delete the placement group"*; the page's second precondition, about a parent of a cluster group, is declined, since substrate models no such relation — see [An instance's architecture and placement group](#an-instance-s-architecture-and-placement-group) |
| CreateLaunchTemplate | Creates version 1. A name outside the published rule is [`InvalidLaunchTemplateName.MalformedException`](#a-missing-launch-template-is-refused-by-the-identifier-named). [Does not validate the AMI](#runinstances-requires-a-resolvable-ami), matching AWS, which reports mapping problems here through `warning`. Networking is read from every `NetworkInterface.N.*` — see [Launch template networking](#launch-template-networking). `MetadataOptions` is stored and validated, and `DisableApiTermination` and every `SecurityGroupId.N` are read — see [Instance metadata options](#instance-metadata-options) and [What a launch template stores](#what-a-launch-template-stores). The top-level `TagSpecification.N` scoped to `launch-template` tags the template itself, separately from `LaunchTemplateData`'s, which tags what a launch creates |
| DescribeLaunchTemplates | Summary only — no `launchTemplateData`, matching AWS. Use `DescribeLaunchTemplateVersions` to read a template's parameters. **All four** filters, and [filter names are checked](#one-rule-for-an-unrecognized-filter-name); `LaunchTemplateId.N` and `LaunchTemplateName.N` are read at every index and [union](#twelve-describes-gained-filters), and a template either names that does not exist is [refused](#which-selectors-assert-existence) — `InvalidLaunchTemplateId.NotFound` or `InvalidLaunchTemplateName.NotFoundException`. [Paginates](#one-offset-paginator-shared) on `MaxResults` (1–200) and `NextToken`; `LaunchTemplateId.N` with `MaxResults` is `InvalidParameterCombination`, `LaunchTemplateName.N` is not. `IncludeManagedResources` is inert |
| DeleteLaunchTemplate | A template that does not exist is refused [by the identifier the request named it with](#a-missing-launch-template-is-refused-by-the-identifier-named) |
| CreateLaunchTemplateVersion | `SourceVersion` inheritance — see [Launch template versions](#launch-template-versions) |
| ModifyLaunchTemplate | `SetDefaultVersion` only, which is AWS's only modifiable attribute |
| DescribeLaunchTemplateVersions | Reports [only the members a version carries](#what-a-launch-template-stores) — no empty list elements. A `resolve:ssm:` `ImageId` reads back as stored unless `ResolveAlias=true`, which reports the AMI it [resolves to](#an-imageid-may-name-a-systems-manager-parameter). Numbers, `$Latest`, `$Default`, `MinVersion`/`MaxVersion`, `MaxResults`/`NextToken` (1–200, through the [shared paginator](#one-offset-paginator-shared)), and the account-wide form. Four of fourteen filters, applied **before** pagination, and [filter names are checked](#one-rule-for-an-unrecognized-filter-name) before the template is resolved — as is the token, so a malformed one is refused whether or not the template exists |
| DeleteLaunchTemplateVersions | Reports per version at HTTP 200; the default version cannot be deleted. A template identifier of the wrong shape fails every version with `launchTemplateIdMalformed` or `launchTemplateNameMalformed` — see [below](#a-missing-launch-template-is-refused-by-the-identifier-named) |
| CreateFleet | Instances launch through the `RunInstances` path, so they are visible to `DescribeInstances`, [need an AMI that resolves](#runinstances-requires-a-resolvable-ami) on a type that [can run it](#an-ami-the-instance-type-cannot-run-is-refused), and carry the reserved `aws:ec2:fleet-id` tag. Partial fulfillment is seedable — see below |
| DescribeFleets | An `instant` fleet is returned only when its ID is named explicitly, matching AWS; [filter names are checked](#one-rule-for-an-unrecognized-filter-name), and it documents **no tag filter**. Paginates on `MaxResults`/`NextToken`, with no published range — and an `instant` fleet therefore never lands on a paginated page, see [One offset paginator, shared](#one-offset-paginator-shared) |
| DeleteFleets | `TerminateInstances=true` (and any `instant` fleet) terminates the fleet's instances, [subject to termination protection](#termination-protection-is-honored-one-availability-zone-at-a-time) |
| CreateCapacityReservation | Reserves capacity **immediately**, in `active` state, and returns the whole `capacityReservation` structure. `InstanceCount`, `InstancePlatform` and `InstanceType` are the only required parameters — `AvailabilityZone` is not one — and `InstanceCount` is range-checked 1–1000. Honours `TagSpecification.N`. `EndDateType` is inferred from `EndDate` rather than defaulted; a **future-dated** reservation is refused rather than answered falsely, and the outcome is [seedable](#seeding-a-capacity-reservation-outcome) — see [A Capacity Reservation is never consumed](#a-capacity-reservation-is-never-consumed) |
| DescribeCapacityReservations | `CapacityReservationId.N` is **singular** and narrows rather than asserting, while a malformed ID is refused; **all twelve** filters, and [filter names are checked](#one-rule-for-an-unrecognized-filter-name) — the page documents **no tag filter**, so use `DescribeTags`. `MaxResults` 1–1000 and `NextToken` through the [shared paginator](#one-offset-paginator-shared). A reservation past its `EndDate` reports `expired`, derived from the simulated clock |
| CancelCapacityReservation | Sets the state to `cancelled` and releases the capacity, so `availableInstanceCount` becomes zero while `totalInstanceCount` keeps reporting what was reserved. A well-formed ID naming nothing answers `InvalidCapacityReservationId.NotFound`; a second cancel answers `IncorrectState`, which is [substrate's reading](#a-capacity-reservation-is-never-consumed) |
| CreateTags | Rejects [reserved `aws:` keys](#reserved-tag-keys), [over-long keys and values](#tag-key-and-value-length-limits), and more than [50 tags per resource](#the-50-tag-per-resource-limit); reaches [all seventeen taggable ID prefixes](#every-taggable-id-prefix-is-reachable) and refuses anything else with `InvalidID`; [authorized against every resource named](#tagging-is-authorized-against-every-resource-it-names) |
| DeleteTags | Rejects [reserved `aws:` keys](#reserved-tag-keys) and [over-long keys](#tag-key-and-value-length-limits); resolves [the same sixteen prefixes](#every-taggable-id-prefix-is-reachable); [authorized against every resource named](#tagging-is-authorized-against-every-resource-it-names) |
| DescribeTags | Every tag in the region, across [the same sixteen resource types `CreateTags` writes](#every-taggable-id-prefix-is-reachable). Five filters with **wildcards**, `MaxResults` 5–1000 and `NextToken` (through the [shared paginator](#one-offset-paginator-shared)), and a deterministic order — see [Finding a resource by tag](#finding-a-resource-by-tag) |

## One rule for an unrecognized filter name

**A `Filter.N.Name` the operation's own API reference does not list is refused**, with
`InvalidParameterValue` / `The filter "<name>" is not valid for this request`, HTTP 400.
The check runs **before** the state scan, so the refusal never depends on whether a
resource happened to match: an empty account and a populated one answer a typo the same
way.

This replaced three different answers across substrate's filter-parsing EC2 operations —
dropped on volumes, snapshots, security groups, route tables, NAT gateways, fleets and
images; matched nothing on instances; refused on instance-type offerings — with the one
real EC2 gives. `DescribeSubnets` and `DescribeTags` were the tenth and eleventh operations
the rule covered, and the only two that arrived with both halves at once: the first parsed no
`Filter.N` at all before it gained the filters below, and the second did not exist.

[#695](https://github.com/scttfrdmn/substrate/issues/695) then brought **twelve more the same
way** — every EC2 describe that had never parsed `Filter.N` at all — so the rule now covers
**twenty-three operations**. That is every EC2 describe substrate serves but one:
`DescribeInstanceAttribute`, whose reference page documents no `Filters` parameter, so there is
no set to check a name against. See
[Twelve describes gained filters](#twelve-describes-gained-filters) for what changed besides
the filters.

**This is a behaviour change, and the loudest one in the release that brought it.** A
misspelled filter name previously came back as a successful response: dropped, so the query
returned *everything*, or match-nothing, so it returned *nothing*. Either way a consumer's
test could pass on a filter that was never applied. It now fails, which is what real EC2
does.

**Provenance.** Refusal is real EC2's *observed* behaviour, not its documented behaviour.
Neither the
[filtering guide](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Using_Filtering.html)
nor the [`Filter` type](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_Filter.html)
says what happens to a name outside the documented set — both are silent. The code
`InvalidParameterValue` is documented; the message text is substrate's own, so dispatch on
the code.

### What is refused, and what is merely inert

The refusal reproduces AWS's set rather than substrate's coverage, so a filter splits three
ways:

| The name | Answer |
|---|---|
| Documented **and** evaluated | Applied |
| Documented, **not** evaluated — substrate keeps no state to answer it | **Inert**: it constrains nothing, so the operation returns every resource it would have returned without it |
| Not documented for that operation | **Refused**, 400 |

Refusing the middle row would deny filters real EC2 accepts —
`ipv6-cidr-block-association.state` is a filter, not a typo — so it stays accepted, and it
is listed by name below rather than left for a caller to discover. Inert is *also* a change
for `DescribeInstances`, which used to match nothing for such a name: an empty answer is
indistinguishable from "the resource does not exist", so a wait loop polls forever, where
over-matching fails once and visibly.

The lists live in `emulator/ec2_filters.go`, one per operation, each transcribed from that
operation's own reference page. AWS documents different filters for each, so a name valid on
one is refused on its neighbour — `tag:<key>` most conspicuously (see below).

| Operation | Evaluated | Documented but inert |
|---|---|---|
| DescribeInstances | `architecture`, `availability-zone`, `image-id`, `instance-id`, `instance-lifecycle`, `instance-state-code`, `instance-state-name`, `instance-type`, `key-name`, `placement-group-name`, `spot-instance-request-id`, `subnet-id`, `tag-key`, `tag:<key>`, `vpc-id` | the other 121 — the `network-interface.*`, `block-device-mapping.*`, `metadata-options.*`, `capacity-reservation*`, `private-dns-name-options.*`, `iam-instance-profile.*` and `operator.*` families, plus `platform`, `tenancy`, `root-device-type`, `owner-id` and the rest |
| DescribeImages | `architecture`, `block-device-mapping.snapshot-id`, `description`, `hypervisor`, `image-id`, `image-type`, `is-public`, `name`, `owner-alias`, `owner-id`, `platform`, `public-ssm-parameter-name`, `root-device-name`, `root-device-type`, `state`, `tag-key`, `tag:<key>`, `virtualization-type` | the other 25 — the `block-device-mapping.*` (bar `snapshot-id`), `image-watermark.*`, `product-code*`, `source-image*` and `state-reason-*` families, plus `creation-date`, the ENA/sriov and kernel/ramdisk names, the Allowed-AMIs and Free-Tier markers and the rest |
| DescribeVolumes | `attachment.delete-on-termination`, `attachment.device`, `attachment.instance-id`, `availability-zone`, `size`, `snapshot-id`, `status`, `tag-key`, `tag:<key>`, `volume-id`, `volume-type` | `attachment.attach-time`, `attachment.status`, `availability-zone-id`, `create-time`, `encrypted`, `fast-restored`, `multi-attach-enabled`, `operator.managed`, `operator.principal` |
| DescribeSnapshots | `description`, `encrypted`, `owner-id`, `progress`, `snapshot-id`, `start-time`, `status`, `tag-key`, `tag:<key>`, `volume-id`, `volume-size` | `owner-alias`, `storage-tier`, `transfer-type` |
| DescribeSubnets | `availability-zone`, `cidr-block`, `default-for-az`, `map-public-ip-on-launch`, `owner-id`, `state`, `subnet-arn`, `subnet-id`, `tag-key`, `tag:<key>`, `vpc-id`, plus AWS's four alias spellings `availabilityZone`, `cidr`, `cidrBlock` and `defaultForAz` | `availability-zone-id`/`availabilityZoneId`, `available-ip-address-count`, `customer-owned-ipv4-pool`, `enable-dns64`, `enable-lni-at-device-index`, `ipv6-native`, `map-customer-owned-ip-on-launch`, `outpost-arn`, and the three `ipv6-cidr-block-association.*` and three `private-dns-name-options-on-launch.*` filters |
| DescribeSecurityGroups | `description`, `group-id`, `group-name`, `owner-id`, `tag-key`, `tag:<key>`, `vpc-id`, and the twenty `ip-permission.*`/`egress.ip-permission.*` rule filters — **all of them** (#1440) | — |
| DescribeSpotInstanceRequests | `availability-zone-group`, `create-time`, `fault-code`, `fault-message`, `instance-id`, `launch-group`, `launch.group-id`, `launch.image-id`, `launch.instance-type`, `launch.key-name`, `launched-availability-zone`, `network-interface.subnet-id`, `product-description`, `spot-instance-request-id`, `spot-price`, `state`, `status-code`, `status-message`, `tag-key`, `tag:<key>`, `type`, `valid-until` | the five `launch.block-device-mapping.*` filters, `launch.group-name`, `launch.kernel-id`, `launch.monitoring-enabled`, `launch.ramdisk-id`, `launched-availability-zone-id`, the other seven `network-interface.*` filters, and `valid-from` |
| DescribeRouteTables | `association.route-table-id`, `association.subnet-id`, `vpc-id` | `association.gateway-id`, `association.main`, `association.route-table-association-id`, `owner-id`, `route-table-id`, `tag-key`, `tag:<key>`, and the eleven `route.*` filters |
| DescribeNatGateways | `state`, `vpc-id` | `nat-gateway-id`, `subnet-id`, `tag-key`, `tag:<key>` |
| DescribeFleets | `activity-status`, `fleet-state`, `type` | `excess-capacity-termination-policy`, `replace-unhealthy-instances` |
| DescribeCapacityReservations | `availability-zone`, `end-date`, `end-date-type`, `instance-match-criteria`, `instance-platform`, `instance-type`, `outpost-arn`, `owner-id`, `placement-group-arn`, `start-date`, `state`, `tenancy` — **all twelve** | — |
| DescribeInstanceTypeOfferings | `instance-type`, `location` (both with [wildcards](#wildcards-in-filter-values)) | — |
| DescribeTags | `key`, `resource-id`, `resource-type`, `value`, `tag:<key>` — all five AWS documents, all with [wildcards](#wildcards-in-filter-values) | — |
| DescribeInstanceStatus | `availability-zone`, `instance-state-code`, `instance-state-name` | the other fifteen — the `event.*`, `system-status.*`, `instance-status.*` and `operator.*` families, plus `application-status.status`, `attached-ebs-status.status` and `availability-zone-id` |
| DescribeVpcs | `cidr`, `dhcp-options-id`, `is-default`, `owner-id`, `state`, `tag-key`, `tag:<key>` | the eight `cidr-block-association.*`/`ipv6-cidr-block-association.*` filters |
| DescribeNetworkAcls | `association.association-id`, `association.network-acl-id`, `association.subnet-id`, `default`, the ten `entry.*` filters, `network-acl-id`, `owner-id`, `tag-key`, `tag:<key>`, `vpc-id` — **all nineteen** | — |
| DescribeDhcpOptions | `dhcp-options-id`, `key`, `owner-id`, `tag-key`, `tag:<key>`, `value` — **all six** | — |
| DescribeFlowLogs | `deliver-log-status`, `flow-log-id`, `log-destination-type`, `log-group-name`, `resource-id`, `tag-key`, `tag:<key>`, `traffic-type` — **all eight** | — |
| DescribeInternetGateways | `attachment.state`, `attachment.vpc-id`, `internet-gateway-id`, `owner-id`, `tag-key`, `tag:<key>` — **all six** | — |
| DescribeKeyPairs | `fingerprint`, `key-name`, `key-pair-id`, `tag-key`, `tag:<key>` — **all five** | — |
| DescribeAvailabilityZones | `region-name`, `state`, `zone-id`, `zone-name` | `group-long-name`, `group-name`, `message`, `opt-in-status`, `parent-zone-id`, `parent-zone-name`, `zone-type` |
| DescribePlacementGroups | `group-arn`, `group-name`, `state`, `strategy`, `tag-key`, `tag:<key>` | `spread-level` |
| DescribeAddresses | `allocation-id`, `association-id`, `instance-id`, `network-interface-id`, `private-ip-address`, `public-ip`, `tag-key`, `tag:<key>` | `network-border-group`, `network-interface-owner-id` |
| DescribeRegions | `endpoint`, `opt-in-status`, `region-name` — **all three** | — |
| DescribeInstanceTypes | `current-generation`, `instance-type`, `memory-info.size-in-mib`, `processor-info.supported-architecture`, `supported-usage-class`, `vcpu-info.default-vcpus` | the other fifty — the `ebs-info.*`, `network-info.*`, `instance-storage-info.*`, `nitro-tpm-info.*`, `vcpu-info.*` (bar `default-vcpus`) and `processor-info.*` (bar `supported-architecture`) families, plus `auto-recovery-supported`, `bare-metal`, `burstable-performance-supported`, `dedicated-hosts-supported`, `free-tier-eligible`, `hibernation-supported`, `hypervisor`, `instance-storage-supported`, `nitro-enclaves-support`, `nitro-tpm-support`, `reboot-migration-support`, `supported-boot-mode`, `supported-root-device-type` and `supported-virtualization-type` |
| DescribeSpotPriceHistory | `availability-zone`, `instance-type`, `product-description`, `spot-price`, `timestamp` | `availability-zone-id` |
| DescribeLaunchTemplates | `create-time`, `launch-template-name`, `tag-key`, `tag:<key>` — **all four** | — |
| DescribeLaunchTemplateVersions | `create-time`, `image-id`, `instance-type`, `is-default-version` | `host-resource-group-arn`, `iam-instance-profile`, `kernel-id`, `license-configuration-arn`, `network-card-index`, `ram-disk-id`, and the four `ebs-optimized`/`http-*` metadata filters |

`tag:<key>` is refused on **nine operations that document no tag filter at all** — neither
`tag:<key>` nor `tag-key`: `DescribeFleets`, `DescribeInstanceTypeOfferings`,
`DescribeInstanceStatus`, `DescribeAvailabilityZones`, `DescribeRegions`,
`DescribeInstanceTypes`, `DescribeSpotPriceHistory`, `DescribeLaunchTemplateVersions` and
`DescribeCapacityReservations`. Some
of those describe resources that plainly carry tags — a fleet and a Capacity Reservation each
carry tags and `DescribeFleets` and `DescribeCapacityReservations` render them, and `DescribeLaunchTemplates` documents both tag filters while
`DescribeLaunchTemplateVersions`, next to it in the same family, documents neither. That is
AWS's set, not an omission here; to find such a resource by tag, use `DescribeTags` or Resource
Groups Tagging.

`tag-key` runs the other way: every tag-bearing describe documents it **except
`DescribeTags`**, which has no such filter because its `key` filter already asks that
question. So `tag-key` is refused there and accepted on its neighbours — again AWS's set.

Read the tag rows in the other direction and **ten** describes filter on tags:
`DescribeInstances`, `DescribeVolumes`, `DescribeImages`, `DescribeSnapshots`,
`DescribeSubnets`, `DescribeVpcs`, `DescribeInternetGateways`, `DescribeKeyPairs`,
`DescribePlacementGroups` and `DescribeAddresses` — the last five since
[#695](https://github.com/scttfrdmn/substrate/issues/695), and the two of those
[#708](https://github.com/scttfrdmn/substrate/issues/708) had to give tag storage to first.
On security groups, route tables and NAT gateways a `tag:<key>` filter is accepted and inert,
so "find the security groups tagged `Env=prod`" answers with *all* of them. Real EC2 offers
three routes to finding a resource by tag: the describe filters, `DescribeTags`, and Resource
Groups Tagging. Substrate serves all three — the first on ten operations, and the other two in
full.

### Filter semantics that apply everywhere

- **Separate `Filter.N` entries AND**; **the values inside one filter OR**. Both are
  documented.
- **Repeated filter names OR their values.** Two `Filter.N` entries sharing a name are one
  filter with the values of both. The second used to silently replace the first everywhere
  except `DescribeImages`.
- **Filter names and values are case-sensitive**, per the `Filter` type. `VPC-Id` is not
  `vpc-id` and is refused.
- **A filter naming no values at all matches nothing** — on every operation and every filter
  name, `tag:<key>` included. AWS says only that a filter value cannot be null, so matching
  nothing is substrate's reading; `tag-key` is the documented way to ask the any-value
  question. Note the distinction from a filter carrying one *empty* value: `Filter.1.Value.1=`
  asks for the empty string, which `DescribeTags`' own Example 6 does, and that is a value like
  any other. Until [#696](https://github.com/scttfrdmn/substrate/issues/696),
  `DescribeSecurityGroups`, `DescribeTags` and `DescribeInstanceTypeOfferings` answered a
  valueless filter with *every* resource, because they shared a matcher that read it as an
  absent filter. An unfiltered answer to a request that asked for a subset is the more
  dangerous silence of the two: a caller cannot tell it from a genuine match on everything.
  An **absent** filter still constrains nothing, as it always has.
- **Wildcards work in every filter value**, on every operation — see below.
- **The documented request limits are enforced**: at most **50 filters** and **200 total
  filter values** per request, and at most **255 characters** per filter value. Exceeding any
  of them is `InvalidParameterValue`, HTTP 400. The limits are AWS's, from Using_Filtering's
  "Filtering considerations"; the error code is substrate's reading, because EC2's error tables
  publish no filter-limit code — every `*LimitExceeded` code there names a resource quota, not
  a request-shape limit. The 255 applies to values, not names: every documented name is a short
  fixed literal, so a length rule on names could only fire after the refusal already had.

### Wildcards in filter values

Since [#697](https://github.com/scttfrdmn/substrate/issues/697), one matcher backs every EC2
describe filter, so these rules hold for all of them — previously only
`DescribeInstanceTypeOfferings` and `DescribeTags` honored wildcards, the two operations whose
reference pages state them outright, and the other nine compared exactly.

| Value | Matches |
|---|---|
| `c5.2xlarge` | that value exactly |
| `c5.*` | the eight `c5` sizes — `*` matches zero or more characters |
| `c5*` | `c5` **and** `c5a`, since `*` also matches the `a` |
| `t3?.micro` | `t3.micro` and `t3a.micro` — `?` matches zero **or one** character |
| `m5.larg\*` | the literal string `m5.larg*`; a backslash escapes a wildcard |
| `M5.XLarge` | nothing — values are case-sensitive |

**`?` matches zero or one character, and AWS's page disagrees with itself about that.** The
resolution is substrate's, and it is the reading two normative statements and three worked
examples support: Using_Filtering's "Filtering considerations" list — the one that governs the
API rather than the console — says "a question mark (?) matches zero or one character", and the
console's wildcard section says the same and works it through (`prod?` matches `prod` and
`prods`, not `production`). One sentence in the CLI examples says "The ? wildcard matches
exactly 1 character" and is refuted by its own example in the next breath, which returns
descriptions that are `"database"` *or* `"database"` plus one character, and by `database????`
returning `"database"` plus **up to** four. `DescribeTags`' Example 4 settles nothing either
way: `?ebserver` finding `webserver` or `Webserver` is consistent with both readings, because
the string a zero-or-one `?` would additionally match is not in AWS's data set.

Two comparisons deliberately stay exact, because they are not filter values. **Identifier
parameters** — `KeyName.N`, `KeyPairId.N`, `GroupName.N`, `GroupId.N`, `ZoneName.N`, `ZoneId.N`,
`AllocationId.N`, `PublicIp.N`, `LaunchTemplateId.N`, `LaunchTemplateName.N`, `FleetId.N`,
`RegionName.N`, `InstanceType.N` — assert the resource exists and answer `Invalid*.NotFound`
when it does not, so globbing them would turn a NotFound contract into a match. And **filter
names**, which are one of a fixed documented set.

A filter whose *values* happen to be identifiers is still a filter: `DescribeRouteTables`'
`association.subnet-id`, `DescribeSubnets`' `vpc-id`, `DescribeSnapshots`' `volume-id` and the
rest all glob. So `association.subnet-id=subnet-*` narrows the answer to the route tables
associated with any subnet at all, where the same string in `SubnetId.N` is a malformed ID.

One case-*insensitive* comparison was removed:
`attachment.delete-on-termination` on `DescribeVolumes` accepted `True` for `true`, which AWS's
"Filter values are case sensitive" does not, and which no sibling boolean filter did.

### Twelve describes gained filters

Until [#695](https://github.com/scttfrdmn/substrate/issues/695), twelve EC2 describes never
parsed `Filter.N` at all: the parameter reached the handler and was discarded, so a filtered
request was answered with **every resource in the region** and nothing in the response said the
filter had been dropped. That was the same defect
[#685](https://github.com/scttfrdmn/substrate/issues/685) fixed for `DescribeSubnets`, twelve
times over, and it is the most consequential kind of silence in an emulator: a consumer's test
asserting "the query returns only the tagged VPC" passed while the query was never applied.

All twelve now carry a filter spec, so each one applies what it can answer, accepts the
documented names it cannot, and refuses the rest — see the table above for the split per
operation. What follows is everything else that had to change to make those filters *readable*,
plus the gaps left standing.

**Response members were added, because a filter you cannot read back is not usable.** Filtering
on a value the response never renders leaves a caller unable to tell a correct answer from a
wrong one. Six members are new:

| Operation | New in the response |
|---|---|
| `DescribeVpcs`, `CreateVpc` | `ownerId`, `tagSet`, and `state` **renamed** from `vpcState` |
| `DescribeInternetGateways`, `CreateInternetGateway` | `ownerId`, `attachmentSet`, `tagSet` |
| `DescribePlacementGroups` | `groupArn` |
| `DescribeAddresses` | `tagSet` |
| `DescribeInstanceStatus` | `availabilityZone` |

The `vpcState` → `state` rename is a **wire fix**: AWS's own samples for both `CreateVpc` and
`DescribeVpcs` render `<state>available</state>`, so an SDK caller decoded an empty `State` from
a VPC that was in fact available — and could not distinguish that from a VPC whose state
substrate had failed to set. Nothing in substrate pinned the old spelling.

`CreateVpc` and `CreateInternetGateway` now render through the same code as their describes, so
the two responses cannot drift — the reason `DescribeSubnets` shares a renderer with
`CreateSubnet`.

**An empty `tagSet` follows the operation's own page, not a house rule.**
`DescribeInternetGateways` renders a present-but-empty `<tagSet/>` and `<attachmentSet/>` on an
unattached, untagged gateway, because its AWS sample does. `DescribeVpcs` and
`DescribeAddresses` **omit** an empty `tagSet`, because theirs do. The inconsistency is AWS's,
and reproducing it per page is the point: a sweep "for consistency" would break one of them
against its own reference.

**Paired identity parameters union rather than intersect.** Six operations take two lists that
name the same resource two ways — `KeyName.N`+`KeyPairId.N`, `ZoneName.N`+`ZoneId.N`,
`AllocationId.N`+`PublicIp.N`, `LaunchTemplateId.N`+`LaunchTemplateName.N`, and
`GroupName.N`+`GroupId.N` on both `DescribePlacementGroups` and — since
[#749](https://github.com/scttfrdmn/substrate/issues/749) — `DescribeSecurityGroups`. A request
carrying both is answered with the resources named by **either**:

```
DescribeKeyPairs&KeyPairId.1=key-0abc…&KeyName.1=deploy   → both key pairs
```

*This reading is substrate's* — AWS documents no rule for the combination. It follows from what
AWS *does* document: an unresolvable name or ID in either list is a `NotFound` error, which only
makes sense if every resource named is expected in the answer. Intersecting would answer empty
while reporting both lists resolved.

Two of those lists were also **read only at index 1** before #695 —
`DescribeLaunchTemplates`' `LaunchTemplateId`/`LaunchTemplateName`, and
`DescribeSpotPriceHistory`' `ProductDescription` — so a caller naming three templates was
answered about one, indistinguishable from the other two not existing. All indices are read now.
Naming one resource in both lists still returns it once.

**Filters apply before pagination** on `DescribeLaunchTemplateVersions` — and, since #1024, on
`DescribeLaunchTemplates` too. A page holds `MaxResults` *matching* records; filtering after paging
would answer `MaxResults=1` with an empty page and a `nextToken`, which reads as "nothing
matches" to a caller that does not follow the token. `DescribeLaunchTemplateVersions`' filter names
are also checked **before** the template is resolved, so a typo answers `InvalidParameterValue`
rather than a launch-template `NotFound`; at `DescribeLaunchTemplates` the `MaxResults` and
`NextToken` refusals come first, ahead of the filter-name check, which is the order every operation
on the shared paginator uses.

**Gaps left standing**, each deliberate:

- **No pagination was added by #695.** `DescribeAddresses`, `DescribeKeyPairs`,
  `DescribeAvailabilityZones` and `DescribePlacementGroups` document **no** `MaxResults` or
  `NextToken` at all, so there is nothing to add. Every operation here that *does* document the pair
  now reads it: `DescribeVpcs` was the first out of this list when #917 converted it, along with
  `DescribeSubnets`, `DescribeSecurityGroups`, `DescribeInstances`, `DescribeImages`,
  `DescribeVolumes` and `DescribeSnapshots`; #1024 converted `DescribeInstanceStatus`,
  `DescribeSpotPriceHistory` and `DescribeFleets`, then `DescribeInternetGateways`,
  `DescribeNatGateways`, `DescribeRouteTables`, `DescribeInstanceTypes` and
  `DescribeInstanceTypeOfferings`, and finally `DescribeLaunchTemplates` — see
  [One offset paginator, shared](#one-offset-paginator-shared).
- **`IncludeAllInstances` is not read** on `DescribeInstanceStatus`. AWS defaults it to `false`,
  meaning "running instances only"; substrate reports every instance whatever its state, so a
  caller relying on the default to exclude stopped instances gets them. Filter on
  `instance-state-name` instead, which is evaluated.
- **`AllRegions` is inert** on `DescribeRegions`, and harmlessly so: every seeded region is
  `opt-in-not-required`, so the opt-in filtering the parameter controls has nothing to exclude.
- **`IncludeUnsupportedInRegion` is not read** on `DescribeInstanceTypes`; the seeded catalog is
  the same in every region.
- **`RunInstances` does not validate `InstanceType` against the catalog**, so it launches a type
  `DescribeInstanceTypes` refuses in the same session. Deliberate, because the catalog is not
  exhaustive; see
  [RunInstances accepts a type DescribeInstanceTypes refuses](#runinstances-accepts-a-type-describeinstancetypes-refuses)
  for why and for what an instance's reported `instanceType` does and does not mean.
- **Nine selector families answer an empty set where AWS answers `NotFound`.** `KeyName.N`
  and `KeyPairId.N` (AWS: `InvalidKeyPair.NotFound`), `GroupName.N` and `GroupId.N` on
  `DescribePlacementGroups` (`InvalidPlacementGroup.Unknown`), `ZoneName.N`/`ZoneId.N`,
  `PublicIp.N`, `DescribeLaunchTemplates`' selectors, `DescribeFleets`' `FleetId.N`,
  `DescribeCapacityReservations`' `CapacityReservationId.N`,
  `DescribeRegions`' `RegionName.N` and `DescribeSecurityGroups`' `GroupName.N` (whose
  `GroupId.N` **does** assert) all select by membership rather than through an
  [ID assertion](#explicit-resource-ids) — so naming one that does not exist narrows the
  answer to nothing instead of failing. This read "six of the new selectors" and named
  four families until #731, which added `DescribeLaunchTemplates` and the two whose
  reasons are permanent rather than pending, and recorded a reason for each; see
  [Which selectors assert existence](#which-selectors-assert-existence), where the two whose
  reasons are permanent are separated from the five that are merely unimplemented. Within
  the twelve, `InstanceId.N`, `VpcId.N`, `InternetGatewayId.N`, `AllocationId.N` and
  `DescribeLaunchTemplateVersions`' template selector **do** assert existence, as does
  `DescribeInstanceTypes`' `InstanceType.N` (`InvalidInstanceType`).

## RunInstances requires a resolvable AMI

`RunInstances` must end up with an AMI from *some* source, or it fails with
`MissingParameter` / "The request must contain the parameter ImageId", HTTP 400.

AWS documents `ImageId` as **Required: No** only because a launch template may
supply it, so substrate checks *after* template resolution rather than on the way
in. Both of these are valid:

- `ImageId` given directly.
- `LaunchTemplate.LaunchTemplateId` (or `…Name`) naming a template whose data
  carries an `ImageId`.

The request fails when neither applies — including when the named template
resolves but carries no AMI of its own, and when the template name does not exist
at all.

Note that `ImageId` is an optional `*string` in the typed SDKs, so
`aws.String("")` serializes as **absent from the wire**: an empty AMI reaches the
service rather than being caught client-side. That is the shape this check exists
for.

**The AMI must also exist.** Once a value is in hand, substrate answers the way
EC2 does:

| The value | Answer |
|---|---|
| `ami-` + a run of lowercase hex naming an AMI substrate can resolve | the launch proceeds |
| `ami-` + a run of lowercase hex naming nothing | `InvalidAMIID.NotFound` / "The image ID '…' does not exist. No image with this ID is registered in this account and Region: register one with RegisterImage, or launch a public AMI resolved from an SSM public parameter such as /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 (DescribeImages with Owner=amazon lists them)", HTTP 400 |
| anything else — `not-an-ami`, `ami-EXAMPLE`, `ami-zzzzzzzz`, a bare `ami-` | `InvalidAMIID.Malformed` / `Invalid id: "…"`, HTTP 400 |

The not-found message names where an AMI comes from as of
[#1432](https://github.com/scttfrdmn/substrate/issues/1432): a fresh server holds no image the
account owns, so a test launching from a fabricated `ami-12345678` fails here, and the code alone
pointed at the ID rather than at the missing step. The code and the leading sentence are unchanged,
so a caller dispatching on either is unaffected; `DescribeImages` with `Owner=amazon` lists the
[bundled AMIs](#which-amis-resolve) a launch can name.

Syntax is reported before absence, as everywhere else substrate names an EC2
resource — see [Explicit resource IDs](#explicit-resource-ids), whose table this
AMI pair now joins. Length is deliberately not part of the syntax rule: AWS itself
accepts both the legacy 8-character and the current 17-character form.

Substrate raises `InvalidAMIID.Unavailable` nowhere. AWS publishes it for an AMI
"deregistered and no longer available", and substrate models no such state —
`DeregisterImage` deletes the record — so an unavailable AMI reads as absent,
which is what a caller polling for one observes anyway.

`CreateFleet` inherits the rule, because its instances launch through the same
path. `CreateLaunchTemplate` and `CreateLaunchTemplateVersion` deliberately do
**not**: AWS reports mapping problems on those operations through the response's
`warning` member rather than by refusing, so an AMI rule there would be a refusal
AWS does not make. A template may therefore carry an AMI that no longer resolves;
the launch from it is where that surfaces.

## An ImageId may name a Systems Manager parameter

An `ImageId` of `resolve:ssm:<parameter>` — in `RunInstances` or in a launch template's data —
resolves to the AMI the parameter holds **at launch**
([#1506](https://github.com/scttfrdmn/substrate/issues/1506)). The forms are the ones
API_RequestLaunchTemplateData publishes: `resolve:ssm:public-parameter`,
`resolve:ssm:parameter-name`, `resolve:ssm:parameter-ARN`, each optionally followed by
`:version-number` or `:label`. RunInstances' own use of the prefix is the EC2 user guide's (`--image-id
resolve:ssm:/aws/service/ami-amazon-linux-latest/...`). The instance reports the resolved AMI ID, and
the template keeps the reference, so a launch after the parameter moves gets the new AMI — the user
guide's point.

The parameter is read from substrate's SSM state: one the account stored with `PutParameter` first,
then the [bundled public parameters](#which-amis-resolve), the order `GetParameter` answers in. A name
with or without its leading slash names one parameter, as SSM stores it.

| Reference | Answer |
|---|---|
| a stored `String` parameter, or a bundled public one | its value, then held to the AMI rules above |
| `:N` naming the parameter's current version (a public one is version 1) | the same |
| a parameter that does not exist | `InvalidParameterValue` |
| a parameter of type `StringList` or `SecureString` | `InvalidParameterValue` — the user guide requires type `String` |
| `:N` naming an older version | `InvalidParameterValue` — substrate's SSM keeps no parameter history |
| `:label` | `InvalidParameterValue` — `LabelParameterVersion` records nothing here |
| a trailing `:` with no selector, or an ARN whose resource is not `parameter/`*name* | `InvalidParameterValue` — not a published form |
| an ARN in another account or Region | `InvalidParameterValue` — substrate models no parameter sharing |

Each refusal is HTTP 400 and its **message is substrate's own**: no capture of real EC2 refusing an
unresolvable `resolve:ssm:` `ImageId` was found, and the reference publishes none. A value that is not
an AMI is refused by the AMI rule itself, as `InvalidAMIID.Malformed`. SSM state that cannot be read,
or a stored parameter that does not decode, is an internal error — never "does not exist", and never a
fall back to the bundled parameters.

The reference is resolved once per launch, after the template merge, and every reader past that point
sees the AMI: the instance's `imageId`, the existence and architecture checks, and a Spot request's
`launchSpecification.imageId` and `productDescription`. That includes a `RequestSpotInstances` request
a [seeded shortfall](#seeding-a-spot-request-outcome) leaves open, and a persistent request's
replacements, which launch the AMI it was fulfilled with. Neither `RequestSpotLaunchSpecification` nor
`LaunchSpecification` mentions Systems Manager parameters — both say "The ID of the AMI" — so reporting
the AMI ID there is substrate's reading. The user guide also requires
data type `aws:ec2:image`; substrate's SSM does not store `DataType`, so that half is not checked.

`DescribeLaunchTemplateVersions` reads the reference back **as stored**, and reports the resolved AMI
only with `ResolveAlias=true` — the reference documents both, with "Default: `false`". A reference
that does not resolve is left as stored there, since the describe is not a launch.

Auto Scaling launches resolve the same way, and its attribute-based selection narrows by the
**resolved** AMI's architecture. Creating or updating a mixed-instances group whose
`InstanceRequirements` override uses a template with a `resolve:ssm:` `ImageId` is refused with
`ValidationError`, as the Auto Scaling user guide states ("Creating or updating mixed instances groups
with attribute-based instance type selection using a launch template that specifies a Systems Manager
parameter is not supported"); that message is substrate's own too, and names where the parameter came
from — the override's own `ImageId`, or the launch template and version. A group already created whose
template later gains such a version is not refused at launch; it narrows by the resolved AMI.

Not modeled: the `ssm:GetParameters` permission the user guides say a launch needs, the rule that only
`instant` EC2 Fleets accept a parameter, and validation of the reference at `CreateLaunchTemplate`
([#1532](https://github.com/scttfrdmn/substrate/issues/1532)); SSM's `DataType`, parameter history and
labels ([#1531](https://github.com/scttfrdmn/substrate/issues/1531)).

## An AMI the instance type cannot run is refused

A launch whose AMI architecture the instance type does not support is refused with
`InvalidParameterValue`, HTTP 400
([#1505](https://github.com/scttfrdmn/substrate/issues/1505)):

> The architecture 'x86_64' of the specified instance type does not match the architecture 'arm64'
> of the specified AMI. Specify an instance type and an AMI that have matching architectures, and
> try again. You can use 'describe-instance-types' or 'describe-images' to discover the architecture
> of the instance type or AMI.

The code and message are **observed**, not published: real RunInstances refusals quote them verbatim
(a Terraform capture with status 400, a Stack Overflow report for `t2.micro` whose type half reads
`'i386,x86_64'`, and a Rancher Spot request in the other direction), and the Auto Scaling AMI
troubleshooting guide quotes the same sentence in a group's activity. The type half is the type's
`supportedArchitectures`, comma-joined. The check runs after the AMI is resolved and before anything
is written, so `CreateFleet` and Auto Scaling, which launch through the same path, inherit it — an
Auto Scaling launch records it in a `Failed` activity.

Two things are deliberately **not** refused: an instance type the catalog does not carry (RunInstances
validates no type, and refusing here would be that wider change), and an AMI whose architecture is
empty, `i386` — `RegisterImage`'s published default — or a `mac` value, none of which a catalog type
reports. Auto Scaling's attribute-based selection already narrows by the AMI's architecture
([#1497](https://github.com/scttfrdmn/substrate/issues/1497)), so a group that selects by attributes
does not newly fail; one that names an override type its AMI cannot run does, as on AWS. A launch
that names no type takes substrate's default, `t3.micro`, which is x86_64 — so an arm64 AMI now needs
a Graviton type named.

## An instance's `iamInstanceProfile` is resolved from IAM, not minted per read

**Substrate reports the id and ARN IAM stored for the profile the instance names.** Real EC2
reports the id of the actual instance profile, which is stable for the life of that profile, and
`RunInstances` and `DescribeInstances` report the same one.

This was a read that was not idempotent ([#1291](https://github.com/scttfrdmn/substrate/issues/1291)).
The builder both operations render an instance through drew the id on **every call**, so two
`DescribeInstances` on one unchanged instance reported two different `iamInstanceProfile.id`
values — each in a 200, each well-formed, so a consumer that describes and compares (a drift
check, a Terraform refresh, a cache) saw a change that had not happened. The id also named
nothing: substrate's IAM plugin mints a real `AIPA…` id when `CreateInstanceProfile` runs, and
the drawn value was unrelated to it, so a caller handing the id back to IAM got a not-found.

Resolving also fixes the ARN, which a synthesized value could not get right. IAM's ARN carries
the profile's **path**, so a profile created at `/dev/` is now reported as
`arn:aws:iam::123456789012:instance-profile/dev/app` rather than at the root — the member a
caller writes into an `iam:PassRole` condition.

| The instance recorded | Reported |
|-----------------------|----------|
| A name IAM has a profile for | That profile's stored id and ARN, path included |
| An ARN IAM has a profile for | The same, resolved in the **ARN's** account, not the caller's |
| A name or ARN IAM has no profile for | The ARN as named, and an id derived from the account and the profile name |

The account comes from the ARN and never from the caller's context (#826): an instance launched
with `arn:aws:iam::999999999999:instance-profile/app` must not report the caller's same-named
profile's id. An ARN that omits the account field names no other account, so it resolves for the
caller. The profile's **name** is the ARN's last segment, because an ARN for a profile at a path
spells the path out — `instance-profile/team/app` — where IAM keys the profile by `app` alone.

Substrate lets an instance launch with a profile name that was never created, so the last row
has to answer for a profile that does not exist. It **derives** the id from the account and the
name — as a public IP is derived from its instance id and a secret's ARN from its name — rather
than drawing one, which keeps the stability property for that instance too: one profile name is
one id, whichever instance names it. It deliberately does not derive from the request id the way
[#856](https://github.com/scttfrdmn/substrate/issues/856) derives a minted identifier; a
request-derived value is stable across a *replay* of one request and would still differ between
two describes, which is the bug. A record that cannot be read — bytes that do not parse, or a
record carrying no id, which a state encoding from an older substrate can present — is treated as
absent and derived for instead, since reporting what it holds would put an empty `id` into the
response.

There is no migration for an instance record written before this. The record stores the profile the
launch named and nothing else — it never held an id, which is why one was invented at render time —
so a record restored from an older event log resolves exactly as a new one does. The alternative of
storing a resolved id on the instance at launch was considered and rejected for that reason: it
would make the describe a pure render at the cost of a record that can disagree with IAM, and a
profile can be created, deleted and recreated after the launch that named it.

EC2's `API_IamInstanceProfile` publishes `id` as a String with no pattern and no length, so the
shape comes from the IAM page the value belongs to: `API_InstanceProfile` publishes
`InstanceProfileId` with a minimum length of 16, a maximum of 128, and pattern `[\w]+`. A derived
id uses the 21-character `AIPA` + `[A-Z0-9]` rendering substrate's IAM plugin already mints, so a
consumer cannot tell a derived id from a stored one. The old 20-character lowercase-hex form
satisfied the published pattern too — this was not a #671 violation — but it disagreed with the
rendering a resolved profile reports.

## Which AMIs resolve

Three kinds of AMI resolve, and nothing else does.

1. **One the caller made** — through `CreateImage` or `RegisterImage`, in the
   caller's own account and region.
2. **A bundled public AMI**, keyed by the SSM public parameter a consumer
   discovers it through. Substrate answers thirteen parameter names with a real image:

   | Parameter | Image |
   |---|---|
   | `/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64` | Amazon Linux 2023, x86_64 |
   | `/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64` | Amazon Linux 2023, arm64 |
   | `/aws/service/ami-amazon-linux-latest/al2023-ami-minimal-kernel-default-x86_64` | Amazon Linux 2023 minimal, x86_64 |
   | `/aws/service/ami-amazon-linux-latest/al2023-ami-minimal-kernel-default-arm64` | Amazon Linux 2023 minimal, arm64 |
   | `/aws/service/ami-windows-latest/Windows_Server-2022-English-Full-Base` | Windows Server 2022 Full Base |
   | `/aws/service/ecs/optimized-ami/amazon-linux-2023/recommended/image_id` | ECS-optimized Amazon Linux 2023 |
   | `/aws/service/ecs/optimized-ami/amazon-linux-2/recommended/image_id` | ECS-optimized Amazon Linux 2 |
   | `/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id` | Ubuntu Server 24.04 LTS, amd64 |
   | `/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id` | Ubuntu Server 22.04 LTS, amd64 |
   | `/aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id` | ECS-optimized Amazon Linux 2023, arm64 |
   | `/aws/service/ecs/optimized-ami/amazon-linux-2/arm64/recommended/image_id` | ECS-optimized Amazon Linux 2, arm64 |
   | `/aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id` | Ubuntu Server 24.04 LTS, arm64 |
   | `/aws/service/canonical/ubuntu/server/22.04/stable/current/arm64/hvm/ebs-gp2/ami-id` | Ubuntu Server 22.04 LTS, arm64 |

   The last four are the arm64 counterparts of the ECS and Ubuntu entries, added so a Graviton
   launch that discovers its AMI the documented way has one
   ([#1497](https://github.com/scttfrdmn/substrate/issues/1497)). Windows Server publishes no
   arm64 parameter.

3. **Any other `/aws/service/…` AMI parameter path**, which resolves to its
   family's entry above — a Windows path to the Windows image, an Ubuntu path to
   Ubuntu 24.04, an ECS path to ECS-on-AL2023, anything else to Amazon Linux 2023 —
   **in the architecture the path names**: a path carrying `arm64` as a token anywhere —
   delimited by `/`, `-` or `_`, as in `.../arm64/hvm/...`, `al2023-ami-kernel-6.1-arm64`,
   `amzn2-ami-hvm-arm64-gp2` or EKS's `.../amazon-linux-2-arm64/recommended/image_id` —
   resolves to the family's arm64 image, any other to its x86_64 one.
   Before #1497 every unlisted path resolved to x86_64, so a Graviton launch that
   discovered its AMI correctly was handed an image of the wrong architecture. Substrate has answered every AMI-shaped `/aws/service/` path since
   it gained `GetParameter`, and this is why: an unlisted path must still resolve
   to something launchable, or `GetParameter` would hand out an AMI that
   `RunInstances` then refuses. The cost is that two unlisted paths in one family
   share an image.

The AMI ID a bundled image gets is **derived** — `sha256(region + ":" +
parameter)`, rendered as `ami-` + 17 hex characters — not random and not written
into state at startup. So it is the same across runs, processes and replays, and
it differs per region exactly as on AWS, where an AMI ID names an image in one
region and nothing in any other. The Go helper `emulator.BundledImageID(region,
parameterName)` returns it, which is what a fixture should name rather than
inventing a literal.

Two things follow from bundled images living outside state.

- **They are listed by owner, not unqualified.** `DescribeImages` with `Owner.N=amazon` lists
  the nine AWS-published entries — every row above but the four Canonical Ubuntu images, which
  carry no owner alias — and applies the request's filters to them, so the discovery AWS's own
  examples use (`Owners=amazon` and a `name` filter) finds an AMI on a fresh server
  ([#1432](https://github.com/scttfrdmn/substrate/issues/1432)). An *unqualified*
  `DescribeImages` still lists only what the account owns: real AWS would return every public
  image, tens of thousands of them, and substrate's reading keeps the unqualified answer meaning
  what it meant. Naming a bundled AMI in `ImageId.N` answers with it too, because a describe and
  a launch must agree about which AMIs exist — see [Explicit resource
  IDs](#explicit-resource-ids). The full `Owner.N` reading:

  | `Owner.N` value | Selects |
  |---|---|
  | `self`, or the caller's account ID | the images the account registered or created |
  | `amazon` | the bundled AMIs published under that alias |
  | `099720109477` | the four bundled Canonical Ubuntu AMIs |
  | anything else — another account ID, `aws-marketplace`, `aws-backup-vault` | nothing substrate holds |

  Several values union. `099720109477` is Canonical's publishing account, as Ubuntu's own
  [Find Ubuntu images on AWS](https://documentation.ubuntu.com/aws/aws-how-to/instances/find-ubuntu-images/)
  page gives it (`aws ec2 describe-images --owners 099720109477`); the Ubuntu entries report it as
  `imageOwnerId`, so the `owner-id` filter selects them too
  ([#1494](https://github.com/scttfrdmn/substrate/issues/1494)). It is the commercial partition's
  account; substrate models no GovCloud or China Region, whose Canonical accounts differ.

  `ExecutableBy.N` is read as of #1494, per API_DescribeImages — "Scopes the images by users with
  explicit launch permissions":

  | `ExecutableBy.N` value | Selects |
  |---|---|
  | `all` | every public AMI — the bundled catalog, since an AMI a caller registers is never public here |
  | `self`, the caller's account ID, or another account ID | nothing: these name AMIs *shared* through launch permissions, and substrate stores none (`ModifyImageAttribute` is not modeled, [#1534](https://github.com/scttfrdmn/substrate/issues/1534)) |

  An AMI the caller owns is not shared with it, so `self` does not select one — `Owner.N=self` is
  what names the caller's own. Several values union, and `ExecutableBy.N` and `Owner.N` narrow
  together: an image is answered when both select it.
- **They are not the caller's.** An AWS-published bundled image reports no owner, and an
  Ubuntu one reports Canonical's, so neither is taggable, an `Owners=self` describe does not
  match it, and the `owner-id` filter naming the caller's account selects nothing for it. It reports `imageOwnerAlias` instead where AWS publishes
  one — see [An AMI reports its architecture, platform and root
  device](#an-ami-reports-its-architecture-platform-and-root-device). Register your
  own AMI when the test is about owning one.

Deliberately **not** bundled: the example IDs AWS's own reference pages use,
`ami-0abcdef1234567890` and `ami-1234567890abcdef0`. Generated IaC copies them,
and it fails on real AWS when it does. Making them launch here would recreate the
exact divergence this check removes, so substrate refuses them like any other AMI
that names nothing.

## A launch template merges with the request, field by field

Naming a launch template does not replace the request, and the request does not
replace the template: the two are merged per field, with the request winning any
field it names. AWS's `RunInstances` reference states the rule directly — "Any
additional parameters that you specify for the new instance overwrite the
corresponding parameters included in the launch template."

| Field | Request | Template | Result |
|---|---|---|---|
| `ImageId` | `ami-request` | `ami-template` | `ami-request` |
| `ImageId` | absent | `ami-template` | `ami-template` |
| `InstanceType` | `m5.large` | `c5.xlarge` | `m5.large` |
| `InstanceType` | `t3.micro` | `m5.large` | `t3.micro` |
| `InstanceType` | absent | `m5.large` | `m5.large` |
| `InstanceType` | absent | absent | `t3.micro` (substrate's default) |
| `KeyName` | `k-request` | `k-template` | `k-request` |
| `TagSpecification` (instance-scoped) | `Env=req` | `Env=tmpl,Team=x` | `Env=req` alone — replace, not merge |
| `TagSpecification` (instance-scoped) | absent | `Env=tmpl` | `Env=tmpl` |
| `TagSpecification` (volume-scoped) | absent | `Env=tmpl` | `Env=tmpl` on every volume the launch makes |
| `IamInstanceProfile` | `p-request` | `p-template` | `p-request` |
| `IamInstanceProfile` | absent | `p-template` | `p-template` |
| `DisableApiTermination` | `false` | `true` | `false` |
| `DisableApiTermination` | absent | `true` | `true` |
| `MetadataOptions` | `HttpPutResponseHopLimit=1` | `HttpTokens=required`, `HttpPutResponseHopLimit=2` | `HttpTokens=required`, `HttpPutResponseHopLimit=1` — per option, see [Instance metadata options](#instance-metadata-options) |
| `SubnetId`, security groups, `AssociatePublicIpAddress` | see [Launch template networking](#launch-template-networking) | | |

Which *version* of the template supplies those values is resolved from
`LaunchTemplate.Version`; an absent version means the template's **default**
version, not its latest. See [Launch template versions](#launch-template-versions).

Two details are worth stating, because both were wrong before and each fails
silently rather than loudly.

Substrate used to consult the template **only when the request omitted `ImageId`**.
A request naming both an AMI and a template therefore ignored the template
entirely — its instance type, key name, user data, subnet, security groups and
public-IP preference were all dropped — and the launch still succeeded. The
instance simply was not the one that was asked for, which is the hardest kind of
infidelity to notice from a test that only checks that the call worked.

The `t3.micro` default is now applied **last**, after the template has had its
chance. It used to be applied first, and the template fallback then treated
`t3.micro` as a proxy for "the request named no instance type" — so a request
explicitly asking for `t3.micro` alongside a template naming something else got
the template's type, exactly inverting the documented precedence. An explicit
`t3.micro` is now honored, and the default applies only when neither side names
a type.

A template's `TagSpecifications` and `IamInstanceProfile` used to be accepted and
stored nowhere, so a template that tagged its instances produced untagged ones and a
template naming a role produced an instance with none — with nothing failing to say
so. That is worse than a dropped `KeyName`, because a `tag:` filter is how IaC finds
the resources it just created: `DescribeInstances --filters tag:Env,Values=prod`
simply returned nothing, and a suite asserting on the tags it asked for had an
assertion that could not pass.

Both now participate in the merge, with two things worth stating:

- **Tags replace rather than merge.** A request naming `Env=req` against a template
  naming `Env=tmpl,Team=x` yields `Env=req` alone; `Team` is not inherited. The
  reference gives no `TagSpecifications`-specific merge semantics, only the general
  "overwrite the corresponding parameters" rule quoted above, and replacement is that
  rule applied to the whole specification.
- **Substrate's own `aws:ec2:fleet-id` stamp does not count as the request naming
  tags.** A fleet instance already carries that reserved key by the time the merge
  runs, so the fallback tests for a non-reserved key rather than for an empty set —
  otherwise a fleet launched from a tagging template would silently lose the
  template's tags. See [Reserved tag keys](#reserved-tag-keys).

**The `instance` and `volume` scopes are modelled, and they resolve
independently.** Each is stored in its own field, so a request naming volume tags
alone still inherits the template's instance tags and vice versa, and each replaces
rather than merges within its own scope. The volume scope applies to every volume the
launch materializes, including the root volume substrate synthesizes when no mapping
declares one — AWS's structure has no way to tag one mapping's volume differently
from another's.

Keeping the two in separate fields is deliberate rather than incidental. Widening the
existing instance field to carry a `ResourceType` discriminator would unmarshal every
template already in an event log without error — into an element with an empty
resource type and no tags — so every stored template would silently start launching
untagged instances. A change that breaks replay while compiling is exactly the shape
this project's persisted structures are grown to avoid.

Both scopes read back from `DescribeLaunchTemplateVersions`, as two `tagSpecificationSet`
items; the volume scope did not until
[#1471](https://github.com/scttfrdmn/substrate/issues/1471), though it was stored and applied.
A template may also scope tags to `network-interface` or `spot-instances-request`.
Those are still recorded nowhere rather than misapplied: they neither reach the launch
nor read back from `DescribeLaunchTemplateVersions`, because a recorded tag no read
surfaces is indistinguishable from a discarded one.

Note that a template's instance-scoped tags land on the *instance*, not on the
template — the reference is explicit that "these tags are not applied to the launch
template."

A template's tags are subject to both tag rules, so a template is not a second
unrestricted tagging path: a `TagSpecifications` naming an `aws:`-prefixed key or
exceeding the 50-tag limit is rejected at `CreateLaunchTemplate` and at
`CreateLaunchTemplateVersion` (after any `SourceVersion` inheritance, so an inherited
violation is caught too). The launch checks again, because a template written
straight into state by a replayed event log can predate those checks.

The instance profile is stored as the single string the request supplied, matching
the shape an instance holds, so it is echoed back from
`DescribeLaunchTemplateVersions` in whichever member it arrived in — `arn` for an
`arn:`-prefixed value and `name` otherwise. `DescribeInstances` surfaces it as an ARN
either way, because AWS's instance response shape has no name member — the ARN it
reports is [resolved out of IAM](#an-instance-s-iaminstanceprofile-is-resolved-from-iam-not-minted-per-read),
so a bare name reaches the instance response with the profile's path; for a
*template* read-back, synthesizing the other member would report the template as
naming something the caller never wrote.

## Launch template versions

Launch templates are versioned. `CreateLaunchTemplate` creates version 1, each
`CreateLaunchTemplateVersion` appends the next number, and `ModifyLaunchTemplate`
moves the default.

**An absent `LaunchTemplate.Version` means the default version, not the latest.**
aws-sdk-go-v2 documents this on `LaunchTemplateSpecification.Version` — "Default:
The default version of the launch template" — and it is the detail worth stating
loudest, because a new version does *not* become the default. A consumer that
creates version 2 and launches without naming a version still gets version 1.

| `LaunchTemplate.Version` | Resolves to |
|---|---|
| absent | the **default** version |
| `$Default` | the default version |
| `$Latest` | the highest version number |
| a number | that version, or `InvalidLaunchTemplateId.VersionNotFound` |

Both aliases are matched case-insensitively, so a hand-built `$latest` works. A
version that does not exist is an error rather than a silent fallback: a fallback
would launch instances from parameters the caller never asked for.

`CreateLaunchTemplateVersion`'s `SourceVersion` is the asymmetry to know:

- **With `SourceVersion`**, the new version inherits that version's parameters and
  the request's values overwrite the ones they name. Every parameter substrate stores
  is inherited, including block device mappings and volume tag specifications —
  those two were silently dropped until #693, so a version derived from a template
  with a 25 GiB mapping launched an 8 GiB default root device instead, and nothing in
  the response revealed the loss.
- **Without it**, the new version holds *only* what the request names. Nothing is
  inherited — not from version 1, not from the latest.

`DeleteLaunchTemplateVersions` reports **per version**, at HTTP 200:
`successfullyDeletedLaunchTemplateVersionSet` and
`unsuccessfullyDeletedLaunchTemplateVersionSet`. A request naming a deletable and an
undeletable version puts one entry in each set and still returns 200, so a caller
checking only the status code sees success. The default version cannot be deleted
("you must first assign a different version as the default"), and a deleted version
number is never reused.

The `responseError.code` on a failed item is `launchTemplateVersionDoesNotExist` for
a missing version. For the default-version rejection substrate emits
`unexpectedError`: `ResponseError.code` is a **closed six-value enum** in the AWS
SDK models (`launchTemplateIdDoesNotExist`, `launchTemplateIdMalformed`,
`launchTemplateNameDoesNotExist`, `launchTemplateNameMalformed`,
`launchTemplateVersionDoesNotExist`, `unexpectedError`) with no default-version
member, and a typed SDK deserializes anything outside it as an unknown variant.
AWS's real code for this case is not published and no capture of the rejection
exists — the code is the modeled catch-all and the message is the reference's own
sentence. Both are inferred, not captured.

Omitting both `LaunchTemplateId` and `LaunchTemplateName` from
`DescribeLaunchTemplateVersions` selects the **account-wide** form, which lists every
template's `$Latest` and/or `$Default`. As AWS does, it accepts only those two
aliases — a version number means nothing across templates — and rejects a request
naming neither.

**A template stored before versioning existed reads back as version 1, default.**
Its single stored parameter set *is* its version 1, synthesized on read, so a
replayed event log recorded against an earlier substrate still launches instances
and still describes correctly. No event rewriting is involved.

## Launch template networking

A launch template's subnet, security groups and public-IP preference are read from
its **primary network interface** — the one whose `DeviceIndex` is lowest:

```
LaunchTemplateData.NetworkInterface.N.SubnetId
LaunchTemplateData.NetworkInterface.N.SecurityGroupId.N
LaunchTemplateData.NetworkInterface.N.AssociatePublicIpAddress
```

That is not a stylistic choice. AWS's `RequestLaunchTemplateData` has **no
top-level `SubnetId` member** — a network interface is the only place a template
can name a subnet, and the only place `AssociatePublicIpAddress` exists at all. So
a template configured the way AWS requires is precisely the one whose networking
substrate used to discard.

Note the group parameter name: the AWS model calls that member `Groups` but gives
it the `locationName` `SecurityGroupId`, so real SDKs send `SecurityGroupId.N`.
Substrate accepts `Groups.N` as well, for hand-built requests.

Precedence when the same value is available from several sources, matching AWS:

| Source | Wins over |
|---|---|
| The request itself — `SubnetId`, or the primary `NetworkInterface.N.SubnetId` | everything below |
| A `CreateFleet` override's `SubnetId` | the template (it reaches `RunInstances` as a request-level value) |
| The launch template's network interface | the default VPC |
| The [default VPC](#the-default-vpc)'s zone-a default subnet | — |

`AssociatePublicIpAddress` is three-valued, and only a non-default subnet without
`MapPublicIPOnLaunch` distinguishes them: **absent** uses the subnet's own
behavior, **`true`** forces a public IP anyway, and **`false`** suppresses one.

Every declared interface is parsed, on both `RunInstances` and launch templates — see
[Multiple network interfaces](#multiple-network-interfaces). The flat fields above
describe the **primary** interface, which is what a launch from a multi-interface
template resolves its subnet and groups from.

## What a launch template stores

`DescribeLaunchTemplateVersions` reports the members a version carries and **nothing else**
([#1471](https://github.com/scttfrdmn/substrate/issues/1471)). Every list member —
`blockDeviceMappingSet`, `securityGroupIdSet`, `networkInterfaceSet`, `tagSpecificationSet`, and
an interface's `groupSet` — is left out when the template has none, where it used to be
rendered as an empty element that an SDK decodes as a present, empty list: a template naming no
network interface read back with a `NetworkInterfaces` key it was never sent. An interface's
`deleteOnTermination` reads back only when the template stated it; the launch still applies the
default (true for an interface the launch creates) either way.

These `RequestLaunchTemplateData` members are stored, read back and applied at launch:
`BlockDeviceMapping.N`, `DisableApiTermination`, `IamInstanceProfile`, `ImageId`,
`InstanceType`, `KeyName`, `MetadataOptions`, `NetworkInterface.N`, `SecurityGroupId.N` (every
index — only the first was read before #1471), `TagSpecification.N` scoped to `instance` or
`volume`, and `UserData`. `DisableApiTermination` is three-valued like
`AssociatePublicIpAddress`: absent leaves the launch's own value, so a template that says nothing
does not read back as `false`.

**Not read**, and therefore neither stored nor read back: `CapacityReservationSpecification`,
`CpuOptions`, `CreditSpecification`, `DisableApiStop`, `EbsOptimized`, `EnclaveOptions`,
`HibernationOptions`, `InstanceInitiatedShutdownBehavior`, `InstanceMarketOptions`,
`InstanceRequirements`, `KernelId`, `LicenseSpecification.N`, `MaintenanceOptions`, `Monitoring`,
`NetworkPerformanceOptions`, `Operator`, `Placement`, `PrivateDnsNameOptions`, `RamDiskId`,
`SecurityGroup.N` (names), and the `network-interface` and `spot-instances-request` tag scopes.
Each is a property no instance record here carries, so storing one would read back a value no
launch honours.

## A missing launch template is refused by the identifier named

Every operation that names one launch template refuses a template that does not exist with the
code for the identifier the request used ([#1469](https://github.com/scttfrdmn/substrate/issues/1469)):

| Named by | Code | Message |
|---|---|---|
| `LaunchTemplateId` | `InvalidLaunchTemplateId.NotFound` | The specified launch template, with template ID `lt-…`, does not exist. |
| `LaunchTemplateName` | `InvalidLaunchTemplateName.NotFoundException` | The specified launch template, with template name `…`, does not exist. |

Both codes are in EC2's published error table; the messages are real EC2's, observed (the name
form is quoted verbatim in hashicorp/packer#7968 and aws/karpenter-provider-aws#2101). That holds
for `CreateLaunchTemplateVersion`, `ModifyLaunchTemplate`, `DescribeLaunchTemplateVersions`,
`DeleteLaunchTemplateVersions`, `DeleteLaunchTemplate`, `RunInstances`' `LaunchTemplate.*` and
`CreateFleet`'s launch template configs (whose codes were already these, with messages of their own).
Before #1469 all of them answered the ID code, so a request naming its template by name — how
most IaC names one — was refused with a code about an ID it never sent, and `RunInstances` did
not refuse at all: it skipped the template and failed, if it failed, on whatever the template
would have supplied. `DescribeLaunchTemplates`' list form answers the same two codes with its own
message; see [Which selectors assert existence](#which-selectors-assert-existence).

**An identifier of the wrong shape that names no template is refused as Malformed**
([#1507](https://github.com/scttfrdmn/substrate/issues/1507)), with the error table's two codes, on
every operation above and on `DescribeLaunchTemplates`:

| Identifier | Rule | Code | Message |
|---|---|---|---|
| `LaunchTemplateId` | `lt-` followed by lowercase hex | `InvalidLaunchTemplateId.Malformed` | The specified launch template ID '`…`' is malformed. |
| `LaunchTemplateName` | 3 to 128 characters of letters, digits and `-` `_` `.` `/` `(` `)` | `InvalidLaunchTemplateName.MalformedException` | The specified launch template name is invalid. A launch template name must be between 3 and 128 characters, and may contain letters, numbers, and the following characters: '-', '_', '.', '/', '(', and ')'. |

The ID message is observed, quoted from a `CreateLaunchTemplateVersion` refusal on AWS re:Post; the
name message is the error table's own description of the code, since no observed wording was found.
The ID's length is not checked, as for every [EC2 resource ID](#explicit-resource-ids): substrate
mints `lt-` and 16 hex characters where AWS mints 17. The rule is applied to the identifier the
request resolves by — the ID when it names one, the name otherwise — so the syntax answer and the
not-found answer are about the same identifier. `DescribeLaunchTemplates` looks every listed
identifier up, then refuses a malformed one that named nothing before a missing well-formed one —
IDs, then names, each in request order. `CreateLaunchTemplate` applies the name rule to the template
it creates, which it accepted unchecked before.

The shape is judged only for an identifier that resolves to nothing. A template already in state is
found and acted on by its identifier whatever that identifier looks like — one created before the
rule existed, or one a recorded event log writes on replay, so an older recording still replays and
its templates stay reachable. A new name is held to the rule where it is introduced, at
`CreateLaunchTemplate`. For an identifier AWS could have issued, the two orders give the same answer.

CloudFormation's `AWS::EC2::LaunchTemplate` with no `LaunchTemplateName` gets a generated
`{stack}-{logicalId}-{suffix}` name, as the Template Reference says ("If you do not specify a name,
CloudFormation creates a name for you"). It used the logical ID verbatim, which a short one such as
`LT` would now fail the name rule with.

`DeleteLaunchTemplateVersions` is the exception: it publishes a per-version
`ResponseError.code` for exactly this — `launchTemplateIdMalformed` and
`launchTemplateNameMalformed` — so a malformed identifier naming nothing fails each requested version in
`unsuccessfullyDeletedLaunchTemplateVersionSet` at HTTP 200, with the messages above. A template
that does not exist is still the whole-request NotFound; the enum's `launchTemplateIdDoesNotExist`
and `launchTemplateNameDoesNotExist` are not raised, because no capture shows which form real EC2
uses for absence ([#1533](https://github.com/scttfrdmn/substrate/issues/1533)).

## Instance metadata options

`MetadataOptions` — `HttpTokens`, `HttpPutResponseHopLimit`, `HttpEndpoint`, `HttpProtocolIpv6`
and `InstanceMetadataTags` — is read on `CreateLaunchTemplate`, `CreateLaunchTemplateVersion`
and `RunInstances`, and is what `ModifyInstanceMetadataOptions` changes
([#1471](https://github.com/scttfrdmn/substrate/issues/1471)). It is how a template requires
IMDSv2 and bounds the hop limit, which is what a security test asserts; until #1471 every one of
those operations discarded it.

- **Validated** against the published values: `optional | required` for `HttpTokens`,
  `disabled | enabled` for the other three strings, and an integer from 1 to 64 for the hop limit.
  Anything else is `InvalidParameterValue`.
- **A template reads back exactly the options it was given**, with no defaults filled in and no
  `state`.
- **A launch merges per option.** The user guide's order of precedence says "Each metadata option
  is evaluated separately", so an option the launch names wins and the rest come from the template.
  A `SourceVersion`-derived template version is different: `MetadataOptions` is one parameter, so a
  version naming any option replaces the source's whole.
- **Every instance reports `metadataOptions`**, with `state` `pending` from `RunInstances` — what
  every example output on the CLI's run-instances page shows — and `applied` from
  `DescribeInstances`. `httpEndpoint`, `httpProtocolIpv6` and `instanceMetadataTags` are always
  present, at the launch's value or at the published default (`enabled`, `disabled`, `disabled`).
  `httpTokens` and `httpPutResponseHopLimit` are present **only when the launch or its template
  stated them**: their defaults come from the AMI's `ImdsSupport` (`required` and a hop limit of 2
  for a `v2.0` AMI), which substrate does not model, so either value would be a guess about the
  image.
- **`ModifyInstanceMetadataOptions`** replaces the options it names and keeps the rest, answering
  `state` `pending`; every later `DescribeInstances` reports `applied`. The transition is settled by
  the next observation rather than timed.

Account-level IMDS defaults (`ModifyInstanceMetadataDefaults`) and IMDSv2 enforcement are not
modelled. What the options *do* inside an instance — refusing a request without a token, how far
a PUT response travels — is resource-internal and out of scope.

## The default VPC

**Every account has a default VPC in each Region, seeded on its first EC2 request**
([#1464](https://github.com/scttfrdmn/substrate/issues/1464)). Until then a fresh substrate
held none — `DescribeVpcs` and `DescribeSubnets` answered nothing, with or without `is-default`
and `default-for-az` — and one was created only by a `RunInstances` that named no subnet, with a
single subnet. Tooling that places resources in the default VPC found nothing to place them in.

Before the first EC2 request an account makes in a Region is dispatched, substrate writes what
`CreateDefaultVpc` documents — "a default VPC with a size /16 IPv4 CIDR block and a default subnet
in each Availability Zone" — and what the VPC User Guide lists alongside it:

| Resource | What it is |
|---|---|
| VPC | `172.31.0.0/16`, `isDefault=true`, DNS support and hostnames on |
| Subnets | one per seeded zone — `172.31.0.0/20` in `a`, `172.31.16.0/20` in `b`, `172.31.32.0/20` in `c` — each `defaultForAz=true` and `mapPublicIpOnLaunch=true` |
| Security group | named `default`, "default VPC security group", with an all-traffic ingress rule from itself and the all-traffic egress rule |
| Internet gateway | attached to the VPC |
| Route table | the VPC's main one: `172.31.0.0/16 → local` and `0.0.0.0/0 →` the gateway |

So the very first call — whatever it is — already sees them, and `is-default=true` and
`default-for-az=true` select them. A launch naming no subnet uses the default subnet of its
`Placement.AvailabilityZone` — the zone-a subnet when it names none — and the `default` group; a
named zone with no default subnet is refused with `InvalidInput` / "No default subnet for
availability zone: '…'", EC2's observed answer. `CreateSecurityGroup` without a `VpcId` places the
group in the default VPC, and refuses the reserved name `default` (any case) with
`InvalidParameterValue` / "Cannot use reserved security group name: default". Time travel
(`/v1/debug/events/{seq}/state`) replays with the server's own `ec2.default_vpc` setting.

**The identifiers derive from the account and Region**, not from the request that triggered the
seed: one account and Region always get the same default VPC ID, and the triggering request's own
mint is not advanced, so a recorded `CreateVpc` that was the first EC2 call in its run still mints
the `vpc-…` it recorded. The check every later request makes is one read of a marker record.

**A deleted default VPC stays deleted.** It is deleted as any VPC is, once its subnets are deleted
and its gateway detached and deleted — see [A VPC's dependents](#a-vpc-s-dependents). `DeleteVpc`
on it leaves a tombstone in the marker, so the
next request does not seed it again — as on AWS, where an account gets one back only by asking. A
launch naming no subnet is then refused with `VPCIdNotSpecified` (the client-error table's "You
have no default VPC in which to carry out the request"), and so is a `CreateSecurityGroup` naming
no `VpcId`. `CreateDefaultVpc` recreates it, with new identifiers and every resource above, and
clears the tombstone; `CreateDefaultSubnet` recreates one deleted default subnet, in the first
`172.31.x.0/20` that overlaps no subnet of the VPC, and concurrent calls for one zone make one
subnet. A default VPC
whose default subnets are all deleted refuses a subnet-less launch with `MissingInput`, rather than
growing a new subnet as the lazy path did.

**Switching it off.** `ec2.default_vpc: false` in `substrate.yaml` (or
`SUBSTRATE_EC2_DEFAULT_VPC=false`) starts every account and Region without a default VPC, the state
an account whose default VPC was deleted is in; `emulator.WithoutDefaultVPC()` does the same for
`StartTestServer`. The default is on.

**Compatibility.** A fresh account now holds a VPC, three subnets, a security group, an internet
gateway and a route table before it creates anything, so a test counting resources in a fresh
account counts them too.

**A recording made before the change does not replay identically when it relied on the old lazy
default VPC.** The lazy path drew the default VPC's identifiers from the triggering request's own
mint, and the seed does not, so a recorded subnet-less `RunInstances` — or any EC2 call that relied
on the default VPC the old path created — replays to different reservation, instance, subnet and
group IDs, and every later event naming them diverges. No compatibility shim is offered; re-record
such a stream. A recording that names its own VPCs and subnets — one starting with `CreateVpc`, say
— keeps its identifiers, and differs only by the seeded records a state-hash comparison sees. A
recording made under this release replays identically, state hashes included.

## A VPC's dependents

**`DeleteVpc` refuses a VPC that still holds something**
([#1492](https://github.com/scttfrdmn/substrate/issues/1492)). `API_DeleteVpc`: "You must detach
or delete all gateways and resources that are associated with the VPC before you can delete it ...
When you delete the VPC, it deletes the default security group, network ACL, and route table for
the VPC." Until this change the VPC record was deleted whatever it held, and its subnets, groups and
route tables stayed listed, so a teardown in the wrong order passed here and failed against AWS.

| Dependent | Holds the VPC while |
|---|---|
| a subnet | it exists |
| an internet gateway | it is attached (detach it with `DetachInternetGateway`) |
| a security group | it exists and is not the VPC's `default` group |
| a route table | it exists and is not the VPC's main one |
| a network ACL | it exists and is not the VPC's default one ([#1614](https://github.com/scttfrdmn/substrate/issues/1614)) |
| a NAT gateway | its state is not `deleted` |
| an instance | its state is not `terminated` |

Any of them is `DependencyViolation` / "The specified object has dependent resources." — the
client-error table's gloss — and nothing is written. Once none remains the VPC is deleted, and its
`default` security group, main route table and default network ACL with it. Dependents substrate does not store as
records of their own — network interfaces, VPC endpoints, peering connections, egress-only
gateways — cannot hold a VPC here.

**Every VPC has a `default` security group.** `CreateVpc` writes one alongside the main route
table, as the VPC User Guide describes — "Your default VPCs and any VPCs that you create come with a
default security group" — with the same rules as the [default VPC](#the-default-vpc)'s. Before,
only the seeded default VPC had one.

**`DeleteInternetGateway` refuses an attached gateway** with `DependencyViolation`, per "You must
detach the internet gateway from the VPC before you can delete it". So does a gateway's teardown in
CloudFormation: `AWS::EC2::VPCGatewayAttachment` now really attaches its internet gateway, and the
stack's delete detaches it before the gateway and the VPC are deleted. A virtual private gateway
attachment is still a stub.

**Compatibility.** A teardown that deletes a VPC before its subnets, groups, route tables or
gateway — or deletes an attached gateway — is now refused, as on AWS. A test counting a new VPC's
security groups counts the `default` one. `CreateVpc` mints the group's ID after the route table's,
so a recorded `CreateVpc` replays to the same VPC, route table and association IDs; the group is a
record the recording did not have.

## What a delete refuses on

**`DeleteSecurityGroup` and `DeleteSubnet` refuse a resource something still holds**
([#1551](https://github.com/scttfrdmn/substrate/issues/1551)). Before it, both deleted the record whatever depended on it,
leaving instances and rules naming a group or subnet that no longer existed. Each refusal is
answered before anything is written.

| Delete | Refused while | Answer |
|---|---|---|
| `DeleteSecurityGroup` | the group is a VPC's default group | `CannotDelete` / "You cannot delete the 'default' security group in your VPC, but you can change its rules." |
| `DeleteSecurityGroup` | another group in the same VPC references it in an ingress or egress rule | `DependencyViolation` / "resource sg-… has a dependent object" |
| `DeleteSecurityGroup` | an instance that is not terminated carries it, on any of its interfaces | the same |
| `DeleteSubnet` | an instance that is not terminated has an interface in it | `DependencyViolation` / "The subnet '…' has dependencies and cannot be deleted." |
| `DeleteSubnet` | a NAT gateway in it is not yet `deleted` | the same |

- **The default group.** The VPC User Guide: "You can't delete a default security group. If you try
  to delete a default security group, we return the following error code: `Client.CannotDelete`."
  The wire code is the client-error table's `CannotDelete`, and the message is that table's gloss.
  The group is flagged as the default when it is minted, so recognising it does not depend on its
  name; a group from a recording that predates the flag is recognised by the name `default`, which
  no other group can hold. `DeleteVpc` still removes it with the VPC.
- **A group in use.** `API_DeleteSecurityGroup`: "If you attempt to delete a security group that is
  associated with an instance or network interface, is referenced by another security group in the
  same VPC, or has a VPC association, the operation fails with `DependencyViolation`." The page names
  that one code for a reference too, so the client-error table's `InvalidGroup.InUse` is not used. A
  group's rules about itself do not hold it, and nor does a reference from another VPC's group — a
  peered VPC's — since the page limits a reference to "the same VPC". An instance's groups are read from every interface it
  was launched with, since substrate stores no standalone network interface. VPC associations and
  resources other services hold (load balancers, Lambda functions, RDS instances) are not consulted.
- **A subnet in use.** `API_DeleteSubnet`: "You must terminate all running instances in the subnet
  before you can delete the subnet." A NAT gateway holds its subnet as it holds its VPC.

**A launch is serialized with both deletes.** `RunInstances` holds the lock the two deletes hold,
from its subnet and group checks through its writes, so a launch cannot land in a subnet or carry a
group a concurrent delete has just removed. A launch naming a subnet that does not exist is now
`InvalidSubnetID.NotFound`; before, the ID was stored on the instance unchecked.

Neither code's message is published; both are EC2's observed wording
(hashicorp/terraform-provider-aws#32745 for the group, terraform-aws-modules/terraform-aws-vpc#1081
for the subnet).

**Compatibility.** A teardown that deletes a group before the instances using it or the rules
referencing it, a subnet before its instances or NAT gateway, or a default group at all, is now
refused, as on AWS, and so is a launch naming a subnet that does not exist. CloudFormation's delete
sweep reverses dependency order, so a stack whose template orders them validly still tears down.

## Network ACLs, DHCP option sets and flow logs

[#1614](https://github.com/scttfrdmn/substrate/issues/1614) adds the three VPC resources a
Terraform VPC reads on every refresh. The `aws_vpc` resource reads `default_network_acl_id` from
`DescribeNetworkAcls` filtered on `vpc-id` and `default=true`, and `dhcp_options_id` from
`DescribeVpcs`; `terraform-aws-modules/vpc` manages the default ACL with `aws_default_network_acl`.
Each is recorded and rendered, never enforced: substrate carries no packets, runs no DHCP server
and delivers no flow-log records, so what a caller can observe is the configuration itself.

### Network ACLs

**Every VPC has a default network ACL**, as the VPC User Guide says: "Your virtual private cloud
(VPC) automatically comes with a default network ACL." It carries the guide's default rules in
each direction: rule 100 allowing all IPv4 traffic, and the catch-all deny rendered as rule 32767.
The IPv6 rules are "added only if you create the VPC with an associated IPv6 CIDR block", which
substrate does not model. `CreateVpc`, the seeded [default VPC](#the-default-vpc) and
`CreateDefaultVpc` all create one. Every subnet is associated with its VPC's default ACL when it is
created, through an `aclassoc-` association that `ReplaceNetworkAclAssociation` replaces with a
freshly minted one. A custom ACL starts with only the two denies.

**Entries are stored the way AWS returns them.** Protocol names (`tcp`, `udp`, `icmp`, `icmpv6`,
`all`) are stored as numbers, and the `entry.protocol` filter accepts either spelling. A port range
is kept only for TCP and UDP, and an ICMP type and code only for ICMP and ICMPv6. For any other
protocol "traffic on all ports is allowed, regardless of any ports or ICMP types or codes that you
specify", so a range sent with protocol `-1` is not kept, and a reader sees no `portRange`. The
CIDR is canonicalised (`100.68.0.18/18` is stored as `100.68.0.0/18`). Entries render egress first,
each direction by rule number, as AWS's `DescribeNetworkAcls` sample does. Each `entry.*` and
`association.*` filter matches an ACL when any one of its entries or associations does.

| Request | Answer |
|---|---|
| A rule number outside 1–32766 | `InvalidParameterValue` — "Positive integer from 1 to 32766. The range 32767 to 65535 is reserved for internal use." |
| A rule number the direction holds, with different attributes | `NetworkAclEntryAlreadyExists` |
| The same, with identical attributes | success, unchanged |
| A 21st rule in one direction | `NetworkAclEntryLimitExceeded` (the quota's default of 20) |
| `ReplaceNetworkAclEntry` or `DeleteNetworkAclEntry` on a rule that does not exist | `InvalidNetworkAclEntry.NotFound` |
| `DeleteNetworkAcl` on a default ACL | `InvalidParameterValue` / "cannot delete default network ACL acl-…" |
| `DeleteNetworkAcl` on an ACL a subnet is associated with | `DependencyViolation` |
| `DeleteVpc` on a VPC holding a custom ACL | `DependencyViolation`; the default ACL goes with the VPC |

**A VPC or subnet recorded before #1614 reads as if it had always had one.** Such a VPC has no ACL
record, so `DescribeNetworkAcls` synthesizes its default ACL at read time. The ID is derived from
the account, Region and VPC ID, and a subnet's association ID from its subnet ID, so the same VPC
always reports the same `acl-` ID and no request's identifier sequence moves. A write that changes
a synthesized ACL — an entry, a replaced association, a tag — stores it under that same ID.

### DHCP option sets

**Each Region has a default DHCP option set**: `domain-name` is `ec2.internal` in `us-east-1` and
`<region>.compute.internal` elsewhere, and `domain-name-servers` is `AmazonProvidedDNS`. These are
the values `CreateDhcpOptions` documents for `AmazonProvidedDNS`. The set's ID is derived from the
account and Region, so it is the same on every read. A new VPC is associated with it, and DescribeVpcs
renders the association as `dhcpOptionsId`; a VPC recorded before #1614 reads as associated with
it too. `AssociateDhcpOptions` with `default` associates a VPC with no set, which DescribeVpcs
renders as `dhcpOptionsId` `default`. The `dhcp-options-id` filter on `DescribeVpcs` selects on
the same value.

`DescribeDhcpOptions` answers an unknown `DhcpOptionsId.N` with `InvalidDhcpOptionID.NotFound`, and
`DeleteDhcpOptions` and `AssociateDhcpOptions` answer an unknown set with
`InvalidDhcpOptionsID.NotFound`. The error reference publishes both spellings, and the Terraform
AWS provider expects each from the operation it reads it from. A set any VPC uses is not deleted:
`DependencyViolation` / "The dhcpOptions 'dopt-…' has dependencies and cannot be deleted."

### Flow logs

**A flow log is recorded intent.** `CreateFlowLogs` stores the configuration — resource, traffic
type, destination type and destination or log group, delivery role, format, aggregation interval
and S3 destination options. `DescribeFlowLogs` reports it with `deliverLogsStatus` `SUCCESS` and
`flowLogStatus` `ACTIVE`. Nothing is written to CloudWatch Logs, S3 or Firehose, and the
destination need not exist. A flow log created without `LogFormat` reports the default format; one
created with a CloudWatch Logs ARN also reports the group's name as `logGroupName`.

| Request | Answer |
|---|---|
| A resource that does not exist | the whole request is `InvalidVpcID.NotFound`, `InvalidSubnetID.NotFound` or `InvalidNetworkInterfaceID.NotFound`; nothing is created |
| A resource already holding a flow log with the same traffic type and destination | an `Unsuccessful` item with `FlowLogAlreadyExists`; the request's other resources still get theirs |
| `cloud-watch-logs` with no `DeliverLogsPermissionArn` | `MissingParameter` |
| `LogGroupName` with an `s3` or `kinesis-data-firehose` destination, or with `LogDestination` | `InvalidParameterCombination` |
| `MaxAggregationInterval` other than 60 or 600 | `InvalidParameterValue` |
| A transit gateway, regional NAT gateway or Cloud WAN `ResourceType` | `InvalidParameterValue` — substrate models none of them |
| `DeleteFlowLogs` naming an ID that does not exist | a top-level `InvalidFlowLogId.NotFound`, and the request's other flow logs are kept |

The delete's shape is EC2's observed behavior (aws-controllers-k8s/ec2-controller#402). No
source shows which shape `CreateFlowLogs` uses for a missing resource; substrate answers it the
same way.

## VPC attributes and internet gateways

**`ModifyVpcAttribute` takes one attribute per request** ([#1554](https://github.com/scttfrdmn/substrate/issues/1554)). The page:
"You cannot modify the DNS resolution and DNS hostnames attributes in the same request. Use separate
requests for each attribute." A request naming two is `InvalidParameterCombination` / "Fields for
multiple attribute types specified: enableDnsSupport, enableDnsHostnames" — EC2's observed answer,
from an SDK capture (aws/aws-sdk-go#415). That message speaks of attribute types generally, so
substrate applies it to `EnableNetworkAddressUsageMetrics` as well, which is its reading; a request
naming none is the same code, whose client-error gloss covers "a missing parameter", with a message
of substrate's own.

**DNS hostnames need DNS support.** "You can only enable DNS hostnames if you've enabled DNS
support." Enabling hostnames on a VPC whose DNS support is off is `InvalidParameterValue`, with that
sentence as the message; no code is published for it, so the client-error table's is substrate's
reading. Turning support off while hostnames are on is not refused, since no page says it is.

**A VPC holds one internet gateway.** "You can attach only one internet gateway to a VPC at a time"
(Amazon VPC quotas). `AttachInternetGateway` to a VPC that already has one is
`InvalidParameterValue` / "Network vpc-… already has an internet gateway attached", EC2's observed
answer (terraform-aws-modules/terraform-aws-vpc#282). The [default VPC](#the-default-vpc) is seeded
with a gateway, so a new gateway attaches to it only after that one is detached. A gateway already
attached elsewhere is still `Resource.AlreadyAssociated`. Every read-check-write of a gateway's
attachments — attach, detach, `DeleteInternetGateway`, and `DeleteVpc`'s dependency check — holds
one lock, so a concurrent attach cannot attach a gateway being deleted or attach to a VPC being
deleted.

**Compatibility.** A modify naming two attributes, a hostnames enable on a VPC without DNS support,
and a second gateway attached to one VPC all succeeded before.

## Route targets

**`CreateRoute` reads every target it publishes**
([#1187](https://github.com/scttfrdmn/substrate/issues/1187)). Until this change it read
`GatewayId` alone, so the private-subnet default route every three-tier template emits —
`NatGatewayId` to `0.0.0.0/0` — was stored with an empty target, and `ReplaceRoute` read six
targets and folded them onto one field. Both now read the twelve the pages publish, and a route
stores and renders each under its own `Route` element:

| Request parameter | Rendered as | Resolved |
|---|---|---|
| `GatewayId` | `gatewayId` | an `igw-` gateway, as `InvalidGatewayID.NotFound` |
| `NatGatewayId` | `natGatewayId` | yes, as `InvalidNatGatewayID.NotFound` |
| `InstanceId` | `instanceId`, with `instanceOwnerId` | yes, as `InvalidInstanceID.NotFound` |
| `NetworkInterfaceId` | `networkInterfaceId` | no — network interfaces are not stored as their own records yet ([#1522](https://github.com/scttfrdmn/substrate/issues/1522)) |
| `TransitGatewayId` | `transitGatewayId` | no |
| `VpcPeeringConnectionId` | `vpcPeeringConnectionId` | no |
| `EgressOnlyInternetGatewayId` | `egressOnlyInternetGatewayId` | no |
| `LocalGatewayId` | `localGatewayId` | no |
| `CarrierGatewayId` | `carrierGatewayId` | no |
| `CoreNetworkArn` | `coreNetworkArn` | no |
| `OdbNetworkArn` | `odbNetworkArn` | no |
| `VpcEndpointId` | `gatewayId`, because `Route` publishes no `vpcEndpointId` member | no |

A target substrate does not store is recorded as sent. A target that does not exist is refused
with its NotFound code. A target that exists but cannot carry traffic is stored `blackhole`, which
is how `API_Route` defines the state — a target that "isn't available (for example, the specified
gateway isn't attached to the VPC, or the specified NAT instance has been terminated)": a NAT
gateway that is `deleted` or `failed`, or an instance that is `shutting-down` or `terminated`.
Every other route is `active`. The state is decided when the route is written, by `CreateRoute` or
`ReplaceRoute`; a route is not moved to `blackhole` when its target is later deleted
([#1552](https://github.com/scttfrdmn/substrate/issues/1552)).

The two composition rules both pages open with are enforced, in that order, after the route table
resolves: "You must specify either a destination CIDR block or a prefix list ID" — none of
`DestinationCidrBlock`, `DestinationIpv6CidrBlock` and `DestinationPrefixListId` is
`MissingParameter`, more than one `InvalidParameterCombination` — and "You must also specify exactly
one of the resources from the parameter list", refused the same two ways. Neither page publishes a
code, so both come from the client-error table, and the messages are substrate's own. `ReplaceRoute`
also accepts `LocalTarget=true` as its target.

`CreateRoute` canonicalises an IPv4 destination, "if you specify `100.68.0.18/18`, we modify it to
`100.68.0.0/18`"; `ReplaceRoute` and `DeleteRoute` canonicalise theirs the same way, so the block
a route was created with finds it, and then match exactly, as their pages say they must. A
destination no route carries is `InvalidRoute.NotFound` on both. A destination the table already
routes is `RouteAlreadyExists`. A route to an internet
gateway with a canonical CIDR answers exactly as before, and a route stored by an earlier substrate
— whatever target it folded into the gateway field — still renders as `gatewayId`. `gatewayId` is
now omitted from a route that names another target, where an empty `gatewayId` used to render.

## Security groups on an instance

Both `RunInstances` and `DescribeInstances` report an instance's security groups as
`groupSet>item`, with `groupId` and `groupName` — the same shape as AWS's
`GroupIdentifier`. Groups appear in the order the launch resolved them, whichever
source supplied them:

- `SecurityGroupId.N` (or `SecurityGroupIds.N`) on the request, or the nested
  `NetworkInterface.N.SecurityGroupId.N` / `NetworkInterface.N.Groups.N` of the
  **primary** interface.
- The launch template's [network interface](#launch-template-networking).
- The [default VPC](#the-default-vpc)'s group named `default`, when the launch names none.

`groupName` is **omitted when the group cannot be resolved** — for example after
the group is deleted once the instance it was launched with is terminated, which a terminated
instance still describes (a group an instance still uses cannot be deleted; see
[What a delete refuses on](#what-a-delete-refuses-on)). The
`groupId` is still reported, because that is what the launch actually recorded; a
name is not invented to fill the field.

The top-level `groupSet` reports the **primary** interface's groups, matching AWS.
A secondary interface's own groups appear on that interface inside
[`networkInterfaceSet`](#multiple-network-interfaces).

## An instance's architecture and placement group

**An instance reports its `architecture`** ([#1525](https://github.com/scttfrdmn/substrate/issues/1525)). `API_Instance` publishes it
as "The architecture of the image", with the Valid Values `i386 | x86_64 | arm64 | x86_64_mac |
arm64_mac`. `RunInstances` stores the launched AMI's architecture on the instance and both
`RunInstances` and `DescribeInstances` render it, so a Graviton type launched from an arm64 AMI
reports `arm64` and an Intel or AMD type `x86_64`, agreeing with `DescribeImages`' `architecture` and
`DescribeInstanceTypes`' `processorInfo.supportedArchitectures`. An AMI that states no architecture,
and an instance recorded before the field existed, take the instance type's from the catalog —
`arm64` when the type supports it, `x86_64` when it supports that — which is the only one such a
launch could have run, since a mismatched launch [is refused](#an-ami-the-instance-type-cannot-run-is-refused).
A type the catalog does not carry reports no `architecture`.

**An instance reports its placement group** ([#1193](https://github.com/scttfrdmn/substrate/issues/1193)). `RunInstances` reads
`Placement.GroupName` and `Placement.GroupId` — "On input, you can specify `GroupId` or `GroupName`,
but not both" (`API_Placement`) — and resolves either to the group, which must exist
(`InvalidPlacementGroup.Unknown`). Naming both is `InvalidParameterCombination`, the Common Error
Types code for "an incorrect combination of parameters", with the page's sentence as the message;
`RunInstances` publishes no code of its own, so that choice is substrate's reading. Before, only the
name was read, its existence checked, and the group dropped. The group is stored on each instance,
and `placement` renders it:

| `placement` member | Reported |
|---|---|
| `availabilityZone` | always |
| `groupName`, `groupId` | when the launch named a group; omitted otherwise, so a launch with none renders as before |
| `affinity`, `hostId`, `hostResourceGroupArn` | no — Dedicated Hosts are not modelled |
| `availabilityZoneId` | no — there is no per-account zone-ID mapping |
| `partitionNumber` | no — partition assignment is not modelled |
| `spreadDomain` | no — "Reserved for future use" |
| `tenancy` | no — `Placement.Tenancy` is not read, so reporting `default` would claim something about a parameter substrate ignores |

**A placement group holding instances cannot be deleted.** `API_DeletePlacementGroup`: "You must
terminate all instances in the placement group before you can delete the placement group."
`DeletePlacementGroup` refuses a group with an instance that is not terminated with
`InvalidPlacementGroup.InUse` / "The specified placement group is in use. If you are trying to delete
a placement group, ensure that its instances have been terminated." — the client-error table's code
and gloss. An instance recorded before #1193 carries no group, so it cannot hold one. The page's
second precondition, "You cannot delete a placement group that is a parent of a cluster placement
group", is declined: substrate models no relation between placement groups, so no group is a
parent. A store read that fails on either operation is now an error rather than a reported missing
group.

**Compatibility.** A placement group with running instances in it can no longer be deleted, and a
launch naming both `Placement.GroupId` and `Placement.GroupName` is refused. A launch with no
placement group renders the same `placement` as before; every instance gains an `architecture`.

## Multiple network interfaces

`RunInstances` and `CreateLaunchTemplate` parse **every** declared
`NetworkInterface.N.*`, contiguously from 1 and stopping at the first missing
index — the same convention every other indexed list follows. Both `RunInstances`
and `DescribeInstances` report them as `networkInterfaceSet>item`, which is what
the `RunInstances` reference's own sample response shows:

```xml
<networkInterfaceSet>
  <item>
    <networkInterfaceId>eni-1a2b3c4d</networkInterfaceId>
    <subnetId>subnet-0123456789abcdef0</subnetId>
    <status>in-use</status>
    <privateIpAddress>172.31.1.10</privateIpAddress>
    <groupSet><item><groupId>sg-…</groupId><groupName>default</groupName></item></groupSet>
    <attachment>
      <deviceIndex>0</deviceIndex>
      <status>attached</status>
      <deleteOnTermination>true</deleteOnTermination>
    </attachment>
  </item>
</networkInterfaceSet>
```

**Identity is `DeviceIndex`, not the parameter index.** AWS documents
`DeviceIndex` as "the position of the network interface in the attachment order",
and it is not required to agree with the position the request happens to write it
at, so `NetworkInterface.1.DeviceIndex=3` and `NetworkInterface.2.DeviceIndex=0`
makes the *second* one primary. Interfaces are reported in `DeviceIndex` order.

The instance's flat `subnetId`, `privateIpAddress` and `groupSet` describe the
**primary** interface, as real EC2 does — they are not superseded by the set.
`AssociatePublicIpAddress` is honored **only on the primary**, which is what the
reference requires: it "can only be assigned to a network interface for eth0".

`DeleteOnTermination` defaults to `true` for an interface the launch creates and
`false` for an existing one it attaches by `NetworkInterfaceId` — deleting an
interface the caller brought would destroy something the launch did not make. An
explicit value wins over either default.

Substrate's own choices, where the API model does not decide:

- A **secondary** interface that names no `PrivateIpAddress` is given one derived
  from its instance index and device index, so every interface of every instance
  in a multi-`Count` launch has a distinct address a test can assert on. The
  primary's address is the instance's own.
- There are **no standalone ENI resources** — `CreateNetworkInterface` is not
  modeled, so an interface exists only as part of the instance that declared it,
  and an `eni-` ID is minted for one the request did not name. A launch declaring
  no interface reports an empty `networkInterfaceSet` rather than a synthesized
  phantom interface.
- Interfaces report `status: in-use` and attachment `status: attached`
  immediately, because substrate's instances are already `running` by the time
  `RunInstances` answers; an `attaching` attachment would contradict the instance
  state reported beside it.
- `InterfaceType` defaults to `interface`; `efa` and `efa-only` are recorded as
  given. `NetworkCardIndex` is recorded as given and defaults to 0.

## Security group tags and rules

**A security group keeps its tags** ([#1440](https://github.com/scttfrdmn/substrate/issues/1440)).
Before it, `CreateSecurityGroup` discarded `TagSpecification.N`, `DescribeSecurityGroups`
rendered no `tagSet` — so a tag `CreateTags` had stored was unreadable too — and `tag:<key>` was
accepted and inert, so `tag:spawn:managed=true` selected every group. Now:

- `TagSpecification.N` scoped to `security-group` tags the group at creation, under the shared
  [reserved-key](#reserved-tag-keys), [length](#tag-key-and-value-length-limits) and
  [50-tag](#the-50-tag-per-resource-limit) rules; a specification naming another type is skipped.
  `CreateSecurityGroup` answers the tags in `tagSet`.
- `DescribeSecurityGroups` renders `ownerId` and `tagSet` (omitted when untagged) and evaluates
  every filter it documents: `tag:<key>`, `tag-key`, `description`, `owner-id`, and the twenty rule
  filters. The rule filters follow the page's own note — "the results include security groups for
  which any combination of rules - not necessarily a single rule - match all filters" — so each
  filter is answered against every rule in its direction, independently.

**A revoke removes the sources it names**
([#1453](https://github.com/scttfrdmn/substrate/issues/1453)). EC2 stores one rule per source.
Substrate stores the `IpPermission` an authorize carried, and before this change a revoke naming one
of its sources dropped the whole permission, so revoking `0.0.0.0/0` from a permission authorized
with `0.0.0.0/0` and `10.1.0.0/16` left no rule at all. Each source a revoke names — an IPv4 range,
an IPv6 range, a prefix list or a referenced group — is now removed on its own, from every stored
permission with the same protocol and port range, and a permission goes only once it holds no
source. IPv6 ranges (`Ipv6Ranges.M.CidrIpv6`) and prefix lists (`PrefixListIds.M.PrefixListId`) are
stored and rendered as of the same change; before it, a revoke naming only an IPv6 range named no
source and removed the first permission on its port.

A revoke naming a rule the group does not hold is answered the way
`API_RevokeSecurityGroupIngress` says, by VPC:

| The group's VPC | Answer |
|---|---|
| a non-default VPC | `InvalidPermission.NotFound` / "The specified rule does not exist in this security group.", HTTP 400 — and **no rules are revoked**, including any the same request did match |
| the default VPC | `return=true`, the matched sources revoked, and the rest reported in `unknownIpPermissionSet` |

Before it, both answered `return=true` having removed nothing. `SecurityGroupRuleId.N` is not
read, because substrate assigns no `sgr-` rule IDs; a revoke naming rules only by ID still answers
`MissingParameter`. A revoke permission naming no source at all keeps its old meaning — it removes
the first stored permission on its protocol and ports — because AWS describes no such rule.

**A referenced group is stored by ID.** A `Groups.M` source named by `GroupName` alone — the
default-VPC form, and the legacy `SourceSecurityGroupName` — is resolved to the group of that name in
the same VPC, "The source security group must be in the same VPC", on authorize and on revoke alike,
so a rule authorized by name is revoked by ID and the other way round. The seeded default group's
self-reference renders `userId` and `groupId` without a `groupName`, which is the shape AWS's own
DescribeSecurityGroups Example 4 shows for a default VPC's default group.

**An authorize of a rule the group holds is refused**
([#1491](https://github.com/scttfrdmn/substrate/issues/1491)). Each source an
`AuthorizeSecurityGroupIngress` or `Egress` names is checked against the group's rules with the same
protocol and port range — the granularity a revoke removes at — and one already held, or named twice
in the request, is `InvalidPermission.Duplicate` / "The specified inbound or outbound rule already
exists for that security group.", HTTP 400, with nothing written. Before, the rule was appended a
second time. That includes the all-traffic egress rule every group is created with, which is why
CloudFormation revokes it before applying a group's inline `SecurityGroupEgress`: "When you create a
security group, if you do not add egress rules, we add egress rules that allow all outbound IPv4 and
IPv6 traffic. Otherwise, we do not add them" (`AWS::EC2::SecurityGroup`).

**A rule's CIDR is canonicalised** ([#1553](https://github.com/scttfrdmn/substrate/issues/1553)). The authorize and revoke pages
publish: "AWS canonicalizes IPv4 and IPv6 CIDRs. For example, if you specify 100.68.0.18/18 for the
CIDR block, AWS canonicalizes the CIDR block to 100.68.0.0/18. Any subsequent DescribeSecurityGroups
and DescribeSecurityGroupRules calls will return the canonicalized form ... if you attempt to add
another rule with the non-canonical form of the CIDR ... the API throws an duplicate rule error."
Every `CidrIp` and `CidrIpv6` — in `IpPermissions.N` and in the legacy top-level `CidrIp` — is
reduced to its network address before the duplicate check, the revoke match and the write, and
rendered in Go's canonical text form, which for IPv6 is the compressed lower-case RFC 5952 form
(`2001:0DB8::1/32` is stored as `2001:db8::/32`). It is the step `CreateRoute` takes for a route's
destination ([#1187](https://github.com/scttfrdmn/substrate/issues/1187)). A value that does not parse as a CIDR of the parameter's
family is `InvalidParameterValue` / "CIDR block … is malformed", EC2's observed answer
(aws/aws-cli#2846, hashicorp/terraform#9481). The family is decided by the text: `CidrIp` must be an
IPv4 dotted quad, and `CidrIpv6` an IPv6 address that is not IPv4-mapped, so `::ffff:0:0/96` is
malformed in both — as `CidrIp` it would otherwise unmap to `0.0.0.0/0` and open the rule to every
IPv4 address. Refusing the mapped form rather than unmapping it is substrate's reading. A rule stored
before this change keeps the spelling it was sent in, and is compared canonically, so a revoke or
duplicate authorize names it in either spelling. One stored malformed — which an earlier substrate
accepted — is revoked by a request naming its exact text, which is the request CloudFormation's
sweep rebuilds from the template; that text is still refused on authorize, and on a revoke of a group
that does not hold it.

A rule's protocol and ports are compared, on authorize and revoke alike, for what they mean rather
than how they were spelled. `API_IpPermission`'s `IpProtocol` is "The IP protocol name (`tcp`,
`udp`, `icmp`, `icmpv6`) or number", so `tcp`, `TCP` and `6` are one protocol, as are `udp`/`17`,
`icmp`/`1` and `icmpv6`/`58` (the case-insensitive comparison of names is substrate's reading).
And "specifying `-1` or a protocol number other than `tcp`, `udp`, `icmp`, or `icmpv6` allows
traffic on all ports, regardless of any port range you specify", so for those protocols the ports
are not compared: `-1` sent with `-1`/`-1` is the rule `-1` was stored as with none. A rule keeps the
spelling it was authorized with.

**A group name is unique within its VPC.** "Names are case-insensitive and must be unique within
the VPC" (`API_CreateSecurityGroup`): a name another group in the VPC holds, in any case, is
`InvalidGroup.Duplicate` / "You cannot create a security group with the same name as an existing
security group in the same VPC." The same name in another VPC is another group. A CloudFormation
group with no `GroupName` is named `{stack}-{logicalId}-{suffix}`, as CloudFormation names an
unnamed resource, rather than by its logical ID, so two stacks of one template no longer collide.

**`GroupName` means the default VPC.** `DescribeSecurityGroups`' `GroupName.N` — "[Default VPC]
The names of the security groups" — selects only the [default VPC](#the-default-vpc)'s groups, and
`DeleteSecurityGroup`'s `GroupName` resolves there too, and so does the `GroupName` of the four
`Authorize`/`RevokeSecurityGroup*` operations ([#1553](https://github.com/scttfrdmn/substrate/issues/1553)), which publish "[Default
VPC] The name of the security group. For security groups for a default VPC you can specify either
the ID or the name of the security group"; a group in any other VPC is named by its ID, and
`GroupId` wins when both are sent. Before, `GroupName.N` matched account-wide, and
`DeleteSecurityGroup` (until #1491) and the four rule operations (until #1553) did not read
`GroupName`, so a request by name was `MissingParameter`.
Both compare names case-insensitively, as uniqueness does: "Security group names are not
case-sensitive" (VPC User Guide, "Security group basics"). The `group-name` *filter* stays exact,
since "Filter values are case-sensitive" (the `describe-security-groups` CLI reference).

**A group's VPC must exist.** `CreateSecurityGroup` with a `VpcId` naming no VPC is
`InvalidVpcID.NotFound`; before, the group was stored in a VPC that did not exist. Every write to a
group — create, authorize, revoke, delete — and `DeleteVpc`'s dependency check and deletes hold one
lock, so a group cannot be created in a VPC a concurrent `DeleteVpc` has just found empty.

**Compatibility.** A duplicate authorize, a duplicate group name in one VPC, and a describe by name of
a group outside the default VPC all answered differently before: the first two succeeded, and the
third selected the group.

## Launch-time storage

`RunInstances` and `CreateLaunchTemplate` parse every declared
`BlockDeviceMapping.N.*`, contiguously from 1 and stopping at the first missing
index, and a launch **materializes** the EBS volumes those mappings describe. They
are real volumes in the same store `CreateVolume` writes to, so `DescribeVolumes`
is the one place to observe an instance's storage, whether the volume was
provisioned separately or created by the launch — which is how real EC2 unifies
the two. Naming a `VolumeId.N` there is an
[assertion that the volume exists](#explicit-resource-ids), so a mapping whose
volume a test expects is confirmed by an error rather than by a silently empty
answer.

`DescribeVolumes` is also the **only** place a launch-specified size is
observable. AWS's `EbsInstanceBlockDevice` — the shape an instance renders per
device — carries `volumeId`, `attachTime`, `deleteOnTermination` and `status`, but
no size, so a caller that wants to know how large a launch made its root volume
has nowhere else to ask.

**Every launch produces at least one volume.** A real instance always has a root
volume whether or not the request mentions one, and `DescribeImages` already
reports an 8 GiB `/dev/sda1` mapping for every AMI substrate serves, so a launch
that declares no mapping gets a synthesized 8 GiB `gp2` volume at `/dev/sda1`
rather than none. A mapping that *does* name a root device configures that root
volume instead of adding a device beside it, which is AWS's own rule for a mapping
whose device name the AMI's mapping already uses.

Root devices are recognized **by name**: `/dev/sda1` and `/dev/xvda`, the two
spellings AWS's device-naming reference gives for the HVM root ("Differs by AMI").
Substrate stores no per-AMI root device to compare against, so an AMI whose real
root device is neither is out of reach here.

`DeleteOnTermination` defaults to **`true` for the root volume and `false` for a
data volume**, and that split is substrate's resolution of a conflict between two
current AWS pages rather than a value either one simply states.

The API reference documents no default for `EbsBlockDevice.DeleteOnTermination` at
all — its only pointer on the subject is a link that no longer resolves to the
content it cites. Two guide pages do, and they disagree:

- [`block-device-mapping-concepts`][bdm-concepts] gives exactly this split: the
  default is "`true` for the root volume and `false` for attached volumes".
- [`preserving-volumes-on-termination`][preserve-volumes] carries a console-vs-CLI
  table that lists a data volume created at launch **via the CLI** as Delete.

Substrate previously followed the second page and applied `true` to every
launch-created volume. The split wins now for two reasons. Both pages agree that the real
launch default comes from the **AMI's own block device mapping**, and substrate's
AMIs carry none to inherit — so the value is substrate's choice either way, and the
non-destructive side of a genuine ambiguity is the one a test emulator should take:
a volume wrongly preserved is visible and correctable, while one wrongly deleted is
gone and the caller learns of it by its absence rather than by an error. And a
consumer reading `block-device-mapping-concepts` — the page that describes the
mapping shape they are writing — would find substrate wrong rather than
opinionated.

A volume attached later with `AttachVolume` defaults to `false`, because deleting a
volume the caller brought would destroy something the launch did not make. Both
pages agree about that one, so nothing here changes it. An explicit value wins over
either default.

[bdm-concepts]: https://docs.aws.amazon.com/ebs/latest/userguide/block-device-mapping-concepts.html
[preserve-volumes]: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/preserving-volumes-on-termination.html

`TerminateInstances` settles the volumes accordingly: one whose attachment deletes
on termination is removed outright, and one that does not becomes `available` with
no attachment, which is what a preserved volume looks like once its instance is
gone.

A fleet reaches mappings only through its **launch template** — `CreateFleet`
forwards the template reference rather than the caller's own mappings — and the
template merge is all-or-nothing, as it is for every other field: a request naming
one mapping of its own does not inherit a second from the template.

Modeled per mapping: `DeviceName`, `VirtualName`, `NoDevice`, and
`Ebs.{SnapshotId, VolumeSize, VolumeType, Iops, Throughput, Encrypted,
DeleteOnTermination}`. `NoDevice` suppresses the device outright, and a
`VirtualName` with no `Ebs.*` members names an instance store device, which is not
an EBS volume and has no presence in `DescribeVolumes` — either way the mapping is
recorded intent that materializes nothing.

`DescribeVolumes` renders `iops` and `throughput` on the volume,
`deleteOnTermination` on each attachment, and a `tagSet` — and filters on
`volume-id`, `status`, `size`, `volume-type`, `availability-zone`, `snapshot-id`,
`attachment.instance-id`, `attachment.device`,
`attachment.delete-on-termination`, `tag:<key>` and `tag-key`.

Substrate's own choices, where the API model does not decide:

- `VolumeType` defaults to **`gp2`**. `EbsBlockDevice` documents no default at all;
  `CreateVolume` documents `gp2`, and substrate uses it for both so there is one
  default in one place rather than two that can drift. Real AMIs specify `gp3` in
  their own mapping, which substrate's AMIs do not carry.
- A volume takes the **instance's** Availability Zone, not the region's first: a
  volume must be in the same zone as the instance it is attached to, so deriving it
  any other way would produce an attachment real EC2 cannot have.

Not modeled: `Ebs.KmsKeyId`, left out rather than stored and hidden.

### A volume carries tags

A volume is taggable on every path the other EC2 resources are. Until this landed it
was the one taggable EC2 resource with no working path at all: `CreateVolume`
accepted `TagSpecification.N` and stored nothing, `CreateTags` on a `vol-` ID answered
`<return>true</return>` and wrote nothing, and `DescribeVolumes` rendered no `tagSet`
and matched no tag filter. Every call returned success, so an IaC consumer's
tag-everything convention appeared to hold and nothing could observe that it had not.

Four paths, all of them now real:

- **`CreateVolume`** applies `TagSpecification.N` scoped to `volume` and echoes the
  result in a `tagSet`. The element carries no `omitempty`: AWS's own second
  `CreateVolume` example renders `<tagSet/>` for an untagged volume, and an SDK tells a
  present-but-empty element ("no tags") from an omitted one ("unknown").
- **`RunInstances`** applies its volume-scoped `TagSpecification.N` to every volume the
  launch materializes, the synthesized root volume included. The instance scope is
  unaffected: a request naming both gets each on its own resource.
- **A launch template**'s volume-scoped tags reach the launch the same way, on their own
  merge gate; see
  [A launch template merges with the request, field by field](#a-launch-template-merges-with-the-request-field-by-field).
- **`CreateTags`/`DeleteTags`** accept a `vol-` ID like any other taggable ID.

Both tag rules apply on every one of them — the reserved `aws:` prefix and the 50-tag
limit — and on a launch the check runs **before the launch loop**, because a volume is
written after its own instance and a refusal inside the loop would leave the first
instance of a multi-count launch behind. AWS documents no volume-specific tag
constraint, so there is nothing else to enforce.

`DescribeVolumes` supports exactly the two tag filters AWS documents for it,
`tag:<key>` and `tag-key`. There is no `tag-value`; AWS does not define one for this
operation. A `tag:<key>` filter with **no value** matches nothing — substrate's reading,
since the filtering guide says only that a filter value cannot be null and offers
`tag-key` for the any-value question.

An unrecognized filter name is **refused**, and one AWS documents that substrate cannot
evaluate (`encrypted`, `create-time`, `fast-restored` and six others) is accepted and
inert. Both follow the rule that now governs every EC2 describe — see
[One rule for an unrecognized filter name](#one-rule-for-an-unrecognized-filter-name),
which lists this operation's eleven evaluated names and nine inert ones.

### `CreateVolume` invents neither a size nor a zone

AWS puts two combination rules on this operation and marks every member involved
`Required: No`, because in each case the requirement is on a *pair*:

| Rule | AWS's words | Substrate's answer when it is broken |
|---|---|---|
| Size or snapshot | "You must specify either a snapshot ID or a volume size." | `InvalidParameterCombination`, 400 |
| Zone name or zone ID | "Either `AvailabilityZone` or `AvailabilityZoneId` must be specified, but not both." | `InvalidParameterCombination`, 400 — for **neither** and for **both** |
| A `Size` that is not a positive integer | — | `InvalidParameterValue`, 400 |

Substrate read neither rule before this. A request naming no size got a silent 8 GiB
volume, one naming `Size=-1` or `Size=eight` got the same, and one naming no zone got
`<region>a` — including a request that named the zone *by ID*, since
`AvailabilityZoneId` was ignored outright. Every one of those returned `200` with a
volume ID, which is the failure a refusal exists to prevent: the volume was real, at a
size and in a zone the caller never asked for, and the first visible symptom was an
attach failing later with nothing to point at.

`AvailabilityZoneId` now resolves to the zone's name, through the **same derivation**
`DescribeAvailabilityZones` renders — a zone ID read out of one operation is one the
other accepts. Two deliberate asymmetries:

- A zone **name** is recorded as given and is *not* checked against the three seeded
  zones; a zone **ID** must resolve, because it has to be translated before it can be
  stored at all. Validating names would be a wider change than the pair rule, and one
  substrate makes on no zone-taking operation.
- An unresolvable zone ID answers `InvalidParameterValue`, not `InvalidParameterCombination`.

The refusal **codes are substrate's reading**: `CreateVolume`'s Errors section is empty,
so it publishes no operation-specific error, and `InvalidParameterCombination`'s
client-error gloss is the only one whose shape fits — "The request includes an incorrect
combination of parameters, **or a missing parameter**."

Still not enforced, and stated here rather than left to be found: AWS's per-type size
ranges (`gp2` 1–16384, `io1` 4–16384, `st1`/`sc1` 125–16384, `standard` 1–1024, …).
`Iops` and `Throughput` also keep their tolerance — an unparseable value leaves the
field at zero and omits it from the response. That differs from `Size` because the
absences differ: a volume must have a size, so an unusable `Size` has no defensible
reading, while omitting `Iops` is the ordinary case for the five volume types that do
not take one. The 8 GiB `ec2DefaultVolumeSizeGiB` still backs the **launch** path, where
a mapping that omits a size is legal and AWS does document a default.

### A mapping AWS refuses is refused, with `InvalidBlockDeviceMapping`

A launch carrying a mapping real EC2 rejects fails here too, before anything is
written. Through the versions that first parsed these mappings, every mapping
substrate could parse was accepted and materialized, so a consumer whose IaC carried
an invalid mapping got a green test and a failure on real AWS — the same class of
defect an empty `ImageId` used to have.

Eight refusals:

| Refused | Provenance |
|---|---|
| An `Ebs` structure naming neither `Ebs.VolumeSize` nor `Ebs.SnapshotId` | Documented verbatim on `EbsBlockDevice.VolumeSize`: "You must specify either a snapshot ID or a volume size." |
| `Ebs.SnapshotId` naming a snapshot the account does not hold | `InvalidSnapshot.NotFound` (or `InvalidSnapshotID.Malformed` for the syntax) — both codes documented in EC2's client-error table |
| `Ebs.VolumeSize` smaller than the named snapshot's | Documented verbatim on `EbsBlockDevice.VolumeSize`: "You can specify a volume size that is equal to or larger than the snapshot size." |
| `Ebs.Throughput` on an explicitly named type that is not `gp3` | Documented verbatim: "This parameter is valid only for `gp3` volumes." |
| `Ebs.Iops` on an explicitly named `standard`, `st1` or `sc1` | The sibling launch-template shape — substrate's reading, see below |
| An unparseable numeric value for `Ebs.VolumeSize`, `Ebs.Iops` or `Ebs.Throughput` | Substrate's own |
| Two mappings naming one `DeviceName` | Substrate's own — AWS documents no rule |
| A `VirtualName` beside any `Ebs.*` member | Substrate's own — AWS documents no rule |

The two snapshot refusals carry different codes on purpose. A snapshot substrate cannot
find is an `InvalidSnapshot.NotFound` about the *ID* — which is what a caller naming a
snapshot from another account or a previous run has done — while a size below the
snapshot's is an `InvalidBlockDeviceMapping` about the *mapping*, like every other row
above.

The error code is documented — EC2's client-error table lists
`InvalidBlockDeviceMapping` as "A block device mapping parameter is not valid. The
returned message indicates the incorrect value." The `400` is a **class-level
inference**: the table says only that client errors are "accompanied by a 400-series
HTTP response code", and every message is substrate's own, since AWS publishes no
wording. Each message names the offending device, because a request can carry many
mappings and the code alone does not say which was refused.

**The scope is only what the API model states.** Per-type size and IOPS ranges are
deliberately *not* encoded even though the reference lists them: they change, and a
stale range is a false deny — worse than the silence it replaces, because a caller
cannot work around a refusal of a request AWS accepts. For the same reason both
type-scoped rules key off the **explicitly named** `VolumeType`, never the resolved
one. Substrate resolves an absent type to `gp2`, but on real EC2 it comes from the
AMI's own mapping, commonly `gp3`, so refusing `Throughput` on a mapping that named
no type would refuse a launch real EC2 accepts.

Three reading calls worth stating outright:

- The size-or-snapshot sentence lives on `EbsBlockDevice.VolumeSize`, a member *of*
  the `Ebs` structure. A mapping carrying **no `Ebs` structure at all** — a bare
  `DeviceName` — names no EBS block device for the requirement to have a subject, so
  it is still accepted and still takes substrate's 8 GiB default.
- `Ebs.Iops` is refused by a short **deny** list rather than AWS's `io1 | io2 | gp3`
  allow list. The "supported for `io1`, `io2`, and `gp3` volumes only" sentence is on
  `LaunchTemplateEbsBlockDeviceRequest.Iops`, not on the `EbsBlockDevice` shape
  `RunInstances` accepts, and the very same member's own paragraph — on both shapes —
  explains what `Iops` means "for `gp2` volumes". AWS contradicts itself inside one
  member, so substrate refuses only the three types that appear in neither list and
  takes the permissive reading of `gp2`.
- A mapping naming a snapshot and **no size** takes the snapshot's size, which AWS
  documents on the same member: "If you specify a snapshot, the default is the snapshot
  size." It took substrate's 8 GiB default instead until #689, so a restore from a
  30 GiB snapshot produced an 8 GiB volume. `CreateVolume` had the identical gap
  independently and applies the same two rules through the same comparison.

**A refusal writes nothing.** The validator runs after a launch template has been
merged in — so a mapping that reaches a launch through a template is refused at
`RunInstances` time, and one validator covers requests, templates, template versions
and fleet launches alike, since a fleet reaches mappings only through a template —
and before the default-VPC branch, which commits a VPC, subnet, security group,
internet gateway, route table and four index mutations. A refusal past that point
would leave state the next request in the same test could see.

**`CreateLaunchTemplate` does not refuse — it warns.** Its response carries a
documented `warning` member of type `ValidationWarning` that exists precisely for
"parameters or parameter combinations that are not valid", and its Errors section
lists none — so a `400` there would be substrate's invention. That an invalid *block
device mapping* belongs in that warning rather than in an error is substrate's
reading; AWS documents the member's purpose but never says which validations use it.

Since #693 both `CreateLaunchTemplate` and `CreateLaunchTemplateVersion` render it:
`warning` (singular) holding `errorSet>item` of `{code, message}`. The warning and
the refusal are the same diagnosis by construction — one collector produces the
problems and the refusal is a thin wrapper returning its first — so the code and
message a caller reads at create time are byte-identical to what the launch would
have refused with. The warning is wider in one respect: it reports **every** problem
rather than the first, because AWS documents one entry "for each issue that's found".
A valid template's response carries no `warning` element at all.

| Operation | On an invalid mapping |
|---|---|
| `CreateLaunchTemplate` | `200` + `warning`; the template is created |
| `CreateLaunchTemplateVersion` | `200` + `warning`; the version is created, including for a mapping inherited through `SourceVersion` |
| `RunInstances` | `400` `InvalidBlockDeviceMapping` (or `InvalidSnapshot.NotFound`); nothing is written |
| `CreateFleet` | as `RunInstances`, since a fleet reaches mappings only through a template |

The mapping a warning is about reads back through
`DescribeLaunchTemplateVersions`, which renders `blockDeviceMappingSet>item` —
`deviceName`, `virtualName`, `noDevice` and the full `ebs` structure. `noDevice` is
the present-and-empty element AWS documents, and `deleteOnTermination` keeps its
three states rather than collapsing an unstated value to `false`. Nothing rendered
the member before #693, so a template's mappings were write-only.

### An instance reports its own block devices

`DescribeInstances`, `RunInstances` and `DescribeInstanceAttribute` each render a
`blockDeviceMapping` set for an instance, so a caller no longer has to go to
`DescribeVolumes` and filter on `attachment.instance-id` to learn which volume is on
which device.

The set is **derived from the volume records**, not from the mappings the launch
recorded. That is what makes it track reality rather than intent: a volume attached
with `AttachVolume` after launch appears, one detached stops appearing, and an
instance store device never appears because it never became a volume. `EC2Instance`
carries no block-device field of its own, so the volumes are also the only available
source.

Each item is AWS's `InstanceBlockDeviceMapping` — `deviceName` plus an `ebs`
sub-element — and the `ebs` element carries the four members AWS's own sample shows:
`volumeId`, `status`, `attachTime` and `deleteOnTermination`. AWS's
`EbsInstanceBlockDevice` has eight members and **no size**, which is why this set does
not replace `DescribeVolumes` for the question above: the other four
(`associatedResource`, `ebsCardIndex`, `operator`, `volumeOwnerId`) are omitted rather
than defaulted, since substrate records nothing behind them and a fabricated
`volumeOwnerId` would be indistinguishable from a real one. `status` uses AWS's
four-value `AttachmentStatus` enum, which is not the five-value volume-side enum
`DescribeVolumes` renders.

Two substrate choices, where the API model does not decide:

- **`RunInstances` renders the set populated.** The reference's only `RunInstances`
  sample response emits `<blockDeviceMapping />` empty, on a `pending` instance whose
  request declared no mappings at all. Substrate's instances are running by the time
  `RunInstances` answers and their volumes already exist — the same ground
  `networkInterfaceSet` is rendered on.
- **The set is ordered by device name**, with the volume ID breaking a tie.
  `DescribeInstances` states outright that its own order may vary, so no fidelity
  claim rests on this; a deterministic emulator answering one request two ways would
  break the guarantee the project exists for.

## Every resource a request names is authorized

Three EC2 operations name more than one resource, and each is decided against all of
them: `RunInstances`, and the tagging pair `CreateTags`/`DeleteTags`. The rule is the
service-agnostic one Organizations'
[`MoveAccount` note](/reference/organizations#a-request-naming-several-resources-is-authorized-against-every-one)
states; these are EC2's instances of it. Every other EC2 operation still resolves to
one ARN.

### A launch is authorized against every resource it names

`RunInstances` is the EC2 action the Service Authorization Reference marks with the
most required resource types — **`image`, `instance`, `network-interface`,
`security-group` and `subnet`** — and the caller's policies are evaluated against
every one of them. A policy that allows `ec2:RunInstances` on the AMI and the
instance but not the subnet cannot launch into that subnet, and, in the other
direction, an ARN-scoped `Deny` on one subnet, AMI or security group fences off
every launch that reaches it. That is what a policy written to keep workloads out of
a shared or private subnet depends on.

A permission boundary is applied to every one of those resources too, since a
boundary checked against a subset is not a boundary. Each ARN is matched against the
tags of the resource *it* names, so an `aws:ResourceTag` condition written about the
subnet cannot be satisfied by a tag on the AMI. The denial names the first resource
the policies do not allow — resolved in a fixed order of image, subnet, security
groups, network interfaces, instance — which is the only place the missing ARN
surfaces, and so the ARN a caller has to add.

The AMI's ARN carries **no account ID** (`arn:aws:ec2:{region}::image/{ami}`),
which is the format the Service Authorization Reference gives, because an AMI is
shareable. The other four are `arn:aws:ec2:{region}:{account}:{type}/{id}`. Two of
them name resources that do not exist when the decision is made, so they are
wildcards: the instance is always `instance/*`, and an interface the launch creates
is `network-interface/*`, while one the request brings by ID is named. The cost of
that is a real limit — a policy scoped to `instance/i-*` matches on AWS and not
here, because the statement is the pattern and `*` is the value it is matched
against.

Resources a [launch template](#a-launch-template-merges-with-the-request-field-by-field)
supplies are authorized as if the request had named them, under the same
field-by-field precedence the launch itself uses, so a policy scoped to one subnet
fences off a launch that reaches another through a template. AWS states this only in
`CreateFleet`'s description: "Resource-level permissions for this action do not
include the resources specified in a launch template. To specify resource-level
permissions for resources specified in a launch template, you must include the
resources in the RunInstances action statement." A template that cannot be read
contributes nothing to the resource list rather than failing the request, so a
missing template still answers [its not-found code](#a-missing-launch-template-is-refused-by-the-identifier-named) rather than a
denial.

A resource the request does not name is skipped, not resolved to `*` — `*` for an
unknown resource would widen the policy the caller wrote instead of narrowing it.

#### A launch that omits `SubnetId` is authorized against the default VPC's resources

A launch that names no subnet does not run without one: substrate resolves the default
VPC's zone-a default subnet and its group named `default` — the VPC being the
`172.31.0.0/16` one [seeded on the account's first EC2 request](#the-default-vpc). Both are
part of the decision, resolved from state *before* it is made. The decision for an account's
very first EC2 request is made before the seed, which is the "No default VPC" row:

| State when the launch arrives | Subnet in the decision | Security group in the decision |
|---|---|---|
| A default VPC with a default subnet | that subnet's ARN | the default VPC's group, if the request named none |
| A default VPC with no default subnet | `subnet/*` | the default VPC's group, if the request named none |
| No default VPC | `subnet/*` | `security-group/*` |

A launch that names its own security groups is authorized against those and not
additionally against the default one, because that is what it attaches. The default
subnet applies only when nothing else supplied one — a request parameter, a nested
`NetworkInterface.N.SubnetId` and a launch template all take precedence, in the same
order the launch itself applies them.

The two wildcards are resources the launch is about to **create**, which is why they
are wildcards rather than skipped: the skip-don't-widen rule above is about a resource
the request omits. A `Deny` on `subnet/*` therefore still matches, and a
least-privilege `Allow` naming one specific subnet correctly refuses a launch that
will mint a different one.

**The resource ARN is substrate's reading, not AWS parity.** The Service Authorization
Reference's `RunInstances` scenario rows require `subnet*` only in the
`EC2-VPC-EBS-Subnet` and `EC2-VPC-InstanceStore-Subnet` scenarios, so a launch that
omits the subnet is, read straight, not authorized against one. Substrate diverges
because a guardrail a caller defeats by omitting a parameter is useless for the purpose
substrate exists for: a test that proves a policy keeps workloads out of a subnet has to
fail when the launch lands in it.

AWS's own recommended subnet guardrail is a condition key rather than a resource ARN —
`ec2:Subnet` on `network-interface/*` — and substrate populates that too, from the same
resolution, so **both** spellings of the guardrail hold for a launch that names no
subnet. See [the condition keys a launch's networking resources
carry](#a-launch-s-networking-resources-carry-ec2-subnet-and-ec2-vpc).

#### A launch's networking resources carry `ec2:Subnet` and `ec2:Vpc`

These are substrate's first `ec2:`-prefixed condition keys, and they are what AWS's own
example policies reach for to fence a launch into one subnet or one VPC. The subnet
example is a `Deny`: "you could create a policy that denies users permissions to launch
an instance into any other subnet. The statement does this by denying permission to
create a network interface, except where subnet `subnet-12345678` is specified."

```json
{
  "Effect": "Deny",
  "Action": "ec2:RunInstances",
  "Resource": "arn:aws:ec2:us-east-1:111122223333:network-interface/*",
  "Condition": {
    "ArnNotEquals": {
      "ec2:Subnet": "arn:aws:ec2:us-east-1:111122223333:subnet/subnet-12345678"
    }
  }
}
```

Both keys' values are **full ARNs**. AWS says so outright for the VPC — "To specify a
VPC for the `ec2:Vpc` condition key, you must specify the full ARN of the VPC" — and its
machine-readable service reference declares the `Type` of both keys as `ARN`. Its two
example policies nonetheless use different operator families, `ArnNotEquals` for
`ec2:Subnet` and `StringEquals` for `ec2:Vpc`; both work here, because a full ARN
satisfies either.

Each key is scoped to the resources the reference lists it on, not merged into the
request, so a condition written about the interface cannot be satisfied by the AMI:

| Resource in the decision | `ec2:Subnet` | `ec2:Vpc` |
|---|---|---|
| `network-interface/*` or `network-interface/{id}` | the launch's subnet | the launch's VPC |
| `subnet/{id}` or `subnet/*` | — | the launch's VPC |
| `security-group/{id}` | — | **that group's own** VPC |
| `image/{ami}`, `instance/*`, `security-group/*` | — | — |

A security group reports the VPC from *its own* record rather than the launch's, because
a group in another VPC is exactly the mismatch such a policy is written to catch. The
VPC itself comes from the default-VPC lookup when the launch names no subnet, and
otherwise from the resolved subnet's record — including a subnet a launch template
supplied, on the same field-by-field precedence the rest of the decision uses.

**A key with nothing to report is absent, not `"*"`.** A launch with no subnet and no
default VPC is about to create both, so there is no subnet and no VPC to name; a
wildcard *value* would be an ARN-shaped string no caller's `ArnEquals` could ever match.
Two consequences, both AWS's documented behaviour rather than substrate quirks:

- An `Allow` gated on an absent key does not match, so such a launch is **refused** —
  which is the safe direction, and the reason the omitted-not-wildcarded choice is not a
  loophole.
- A `Deny` gated on one with a positive operator does not fire, and a set qualifier
  (`ForAllValues:`) over it is **vacuously true** — which is why AWS says not to use set
  operators on single-valued keys.

Not covered: `ec2:Vpc` on any action other than `RunInstances` — the reference lists 28
action×resource pairs carrying `ec2:Subnet` service-wide, and this covers the launch
path only.

#### A fleet's launches are authorized, not exempted

`CreateFleet` launches its instances internally, through the same `RunInstances` path a
direct call takes, so the API's authorization pipeline sees only the `CreateFleet`
request. Each pool's launch is therefore authorized separately, against the resources
that pool resolves to — its launch template's AMI, the override's subnet, and the
instance and interface wildcards. A caller needs `ec2:RunInstances` on those resources
in addition to `ec2:CreateFleet`, which is what AWS requires too: "Resource-level
permissions for this action do not include the resources specified in a launch
template. To specify resource-level permissions for resources specified in a launch
template, you must include the resources in the RunInstances action statement."

Every pool is decided before the first one launches, so a refused fleet leaves no
instances and no fleet record behind.

Not covered:

- **`volume`.** Its asterisk appears only inside the reference's scenario rows,
  never on the action's own resource list, and the ID does not exist until the
  launch runs. Requiring it would refuse a policy naming exactly the five
  documented types.
- **The launch-template ARN**, which the reference does not mark required for
  `RunInstances` — so requiring it would refuse the same policy.
- **Most of the EC2 condition keys** (`ec2:InstanceType`,
  `ec2:IsLaunchTemplateResource` and the rest). Resource resolution is what this
  covers; the condition keys substrate populates are the `aws:`-prefixed ones,
  [`ec2:CreateAction`](#a-tagged-create-is-authorized-twice) on the tagging pass, and
  [`ec2:Subnet`/`ec2:Vpc`](#a-launch-s-networking-resources-carry-ec2-subnet-and-ec2-vpc)
  on a launch's networking resources.

### Tagging is authorized against every resource it names

`CreateTags` and `DeleteTags` accept up to 1000 resource IDs in `ResourceId.N`, of
mixed types, and the caller's policies are evaluated against every one of them. A
`Deny` naming any single resource refuses the whole call, and the denial names that
resource — so a policy written to keep a shared VPC or a production volume out of
reach of a pipeline that re-tags everything it can see is a boundary. In the other
direction, a least-privilege `Allow` listing the ARNs a call touches permits it; a
policy missing one refuses the call and says which ARN to add. A permission boundary
applies to every resource named, and each ARN is matched against the tags of the
resource *it* names.

Both actions have **no required resource type at all** — the Service Authorization
Reference marks zero of the 105 it lists for either one. Authorizing against every
resource named is therefore substrate's reading of AWS's general rule for an action
naming several resources, supported by AWS's own scoped tagging examples, which write
a resource ARN as the `Resource` of an `ec2:CreateTags` statement and would be
pointless if only one resource of a batch were evaluated. The two actions resolve
identically. They differ in condition-key surface, and the difference that matters is
modelled: **neither carries `ec2:CreateAction`**, because neither is a create. See [a
tagged create is authorized twice](#a-tagged-create-is-authorized-twice).

The ARN a caller writes uses the resource type the reference documents, which for five
of the nine taggable types is *not* the abbreviation substrate's own IDs suggest:

| ID prefix | ARN resource type |
|---|---|
| `i-` | `instance` |
| `vol-` | `volume` |
| `vpc-` | `vpc` |
| `subnet-` | `subnet` |
| `sg-` | `security-group` |
| `igw-` | `internet-gateway` |
| `rtb-` | `route-table` |
| `eipalloc-` | `elastic-ip` |
| `nat-` | `natgateway` |

An ID whose prefix names none of those is **skipped, not denied**: substrate's handler
treats such an ID as a no-op, and AWS answers an unparseable tagging ID with `InvalidID`
("The specified ID for the resource you are trying to tag is not valid") rather than
`AccessDenied`. Skipping is also the only safe direction — widening it to `*` would hand
the caller the one resource a broad `Allow` matches. A call naming *only* such IDs is
decided against a single `*`, as an EC2 operation naming no resource substrate can resolve
is.

`aws:RequestTag/{key}` is populated from the tags a tagging call carries
(`Tag.N.Key`/`Tag.N.Value`), so the "tag only with the keys and values we prescribe"
condition AWS's tagging guardrails are written around evaluates. `DeleteTags` treats a
tag's value as optional, and a request naming only a key records the empty string —
indistinguishable from an absent key to every condition operator, including `Null`.

The refusal is whole: authorization runs before the handler, so a call that names one
resource the policy denies tags **none** of them — the same all-or-nothing shape the
[reserved-key](#reserved-tag-keys) and [tag-limit](#the-50-tag-per-resource-limit)
checks already have.

`aws:TagKeys` is populated too — the sorted list of keys the request names — so the
`ForAllValues:StringEquals` form several of AWS's `DeleteTags` examples are written in now
evaluates. See [multivalued condition
keys](/reference/iam#multivalued-condition-keys-forallvalues-and-foranyvalue) for the set-qualifier rules
those examples depend on, including why AWS pairs them with `Null`.

### An operation naming resources by ID is decided against every one of them

Four request parameters resolve to the resources they name, so the operations carrying them
are authorized against real ARNs and against those resources' own tags rather than against
the literal `*`:

| Parameter | Resolves to | The bundled actions that need it |
|---|---|---|
| `InstanceId.N` | `instance/<id>` | — (it is the one parameter that already resolved) |
| `GroupId.N` | `security-group/<id>` | `ec2:DeleteSecurityGroup` |
| `RouteTableId.N` | `route-table/<id>` | `ec2:DeleteRouteTable`, `ec2:DeleteRoute` |
| `InternetGatewayId.N` | `internet-gateway/<id>` | `ec2:DeleteInternetGateway` |

The list is short for a reason: each entry is a parameter one of the two `ec2:ResourceTag`
statements in the bundled AWS managed policies names its target with. `DeleteRoute` appears
under `RouteTableId` because the route table is the resource AWS authorizes a route change
against. The indexed and un-indexed spellings are the same request, matching what the
handlers themselves accept.

**Every ID a request names is authorized, not the first alone**
([#744](https://github.com/scttfrdmn/substrate/issues/744)). A `TerminateInstances` naming
three instances was decided against `InstanceId.1`'s ARN and `InstanceId.1`'s tags, so a
policy allowing instances tagged `Env=dev` and denying the rest permitted a call naming one
dev instance and two production ones — provided the dev one came first. Each named resource
now carries its own ARN and its own tags into its own decision, which is the same reading
[tagging](#tagging-is-authorized-against-every-resource-it-names) already applies to
`ResourceId.N` and AWS applies to any action naming several resources. Two consequences a
consumer's test will see:

- **The denial names the first resource the policy does not allow**, in fixed parameter order
  and then request-index order. That is deterministic and replay-stable, and it is the
  resource a caller can act on: drop it from the batch and the call proceeds.
- **A permission boundary sees the whole batch too.** It is loaded once and evaluated against
  each resource, so a boundary naming only the first instance refuses the rest.

<a id="the-operation-decides-whether-a-request-has-a-resource"></a>**The operation decides
whether a request has a resource at all**
([#762](https://github.com/scttfrdmn/substrate/issues/762)). An ID is resolved only when AWS
documents the operation as supporting that resource type. Whether a request has a resource is
not a property of its parameters: AWS publishes, per action, which resource types the action
supports, and an action supporting none is authorized against `*` however many identifiers
the request carries. `ec2:DescribeInstances` is exactly that action, so a policy scoping it
to an instance ARN grants nothing on AWS — and granted precisely those instances here, which
is the direction that matters, because a test written against substrate passed while the
deployment behind it failed.

The classification is **not a hand-written operation list**. It comes from AWS's
[Service Reference Information](https://servicereference.us-east-1.amazonaws.com/), whose
per-service JSON names the resource types each action supports and where an action with no
`Resources` list supports none. Snapshots of `ec2` and `iam` are vendored under
`emulator/authzref`, generated into a Go table, and `make authz-reference-check` fails if the
table and its snapshots disagree — so a refreshed snapshot cannot silently widen a decision.
The Service Authorization Reference HTML pages carry the same data but render their tables in
JavaScript and cannot be read by a fetch — the dead end recorded for both ELB pages under
[ELB v2](/reference/elasticloadbalancing), which is why ELB's `Names.member.N` is still decided against `*`.

An operation **absent** from that table is also decided against `*` — the same answer AWS
gives an action with no published resource types, and the safe direction of the two: a
resource narrower than the one AWS would use is a grant substrate would be inventing, and
inventing a grant is worse than inventing a refusal.

Two consequences worth stating. `GroupId` is overloaded — `DescribePlacementGroups` reads
`pg-` IDs through it — and that operation supports no resource-level permissions, so it is
decided against `*` and the placement-group ARN is never built for it; the name-form ARN
translation is still exercised by `CreateTags`, which AWS does scope to `placement-group`.
And an `ec2:ResourceTag/<key>` condition on a describe can no longer match, because with the
request resource `*` there is no resource whose tags to read. That is AWS's behavior too, and
it is why the bundled policies put those conditions on the mutating operations.

`aws:ResourceTag/<key>` and `ec2:ResourceTag/<key>` both report the resolved resource's
tags. <a id="ec2-reports-a-resource-s-tags-under-both-prefixes"></a>**EC2 reports a
resource's tags under both prefixes** because AWS publishes both — `aws:ResourceTag` as a
global key and `ec2:ResourceTag` as EC2's own — and AWS's own bundled policies use the
service-specific one. The two prefixes are not folded together: they are two keys, not one
key compared case-insensitively, and only EC2 gets a second prefix. Reporting, say, an S3
bucket's tags under an `s3:ResourceTag/` key AWS does not publish would honor a policy real
AWS ignores, which is a divergence in the granting direction.

The bundled statement that motivated the prefix conditions on
`ec2:ResourceTag/aws:cloudformation:stack-name`, and that tag is not one a caller can set —
`CreateTags` refuses a key beginning with `aws:`, as AWS does. <a id="cloudformation-stamps-its-own-tags-on-the-resources-it-creates"></a>**CloudFormation
stamps its own tags on the resources it creates**
([#746](https://github.com/scttfrdmn/substrate/issues/746) for EC2,
[#765](https://github.com/scttfrdmn/substrate/issues/765) for everything else), which is what makes
`ManagedCloudformationResourcesCleanupPolicy`'s statement satisfiable rather than inert: a
resource a stack creates carries `aws:cloudformation:stack-name`,
`aws:cloudformation:stack-id` and `aws:cloudformation:logical-id`, so the bundled statement's
`StringLike` on `EC2ContainerService-*` now grants the four EC2 deletes for a resource an
`EC2ContainerService-…` stack created and still refuses them elsewhere.

The stamp is written to state by the deployer rather than sent as a `TagSpecification`, for
two reasons a consumer can observe: a synthesized `CreateTags` would refuse the `aws:` keys it
is carrying, and every synthesized request is authorized, so the parameter route would newly
require `ec2:CreateTags` of every principal whose deployment succeeds today. The three values
are the stack's own — the stack ID is the same ARN `AWS::StackId` resolves to, not a second
derivation of it — and the write is an upsert, so re-deploying a stack rewrites the three
rather than accumulating them. `aws:` keys are already excluded from the 50-tag limit, so the
stamp cannot push a caller's own tags over it.

### What the stamp reaches

Forty CFN resource types are stamped, across twenty-one services, and each tag is readable
through that service's **own** tag call rather than only out of state:

| Service | CFN types stamped | Read back with |
|---|---|---|
| EC2 | VPC, Subnet, SecurityGroup, InternetGateway, RouteTable, EIP, NatGateway, LaunchTemplate, Instance | `DescribeTags` |
| S3 | `AWS::S3::Bucket` | `GetBucketTagging` |
| Lambda | `AWS::Lambda::Function` | `ListTags` |
| SQS | `AWS::SQS::Queue` | `ListQueueTags` |
| DynamoDB | `AWS::DynamoDB::Table` | `ListTagsOfResource` |
| ELBv2 | LoadBalancer, TargetGroup, Listener, ListenerRule | `DescribeTags` |
| Step Functions | `AWS::StepFunctions::StateMachine`, `AWS::StepFunctions::Activity` | `ListTagsForResource` |
| ECR | `AWS::ECR::Repository` | `ListTagsForResource` |
| ECS | `AWS::ECS::Cluster`, `AWS::ECS::Service`, `AWS::ECS::TaskDefinition` | `ListTagsForResource` |
| EFS | `AWS::EFS::FileSystem`, `AWS::EFS::AccessPoint` | `ListTagsForResource` |
| ElastiCache | `AWS::ElastiCache::CacheCluster` | `ListTagsForResource` |
| RDS | `AWS::RDS::DBInstance`, `AWS::RDS::DBCluster`, `AWS::RDS::DBSubnetGroup` | `ListTagsForResource` |
| Kinesis | `AWS::Kinesis::Stream` | `ListTagsForStream` |
| Glue | `AWS::Glue::Database` | `GetTags` |
| KMS | `AWS::KMS::Key`, `AWS::KMS::ReplicaKey` | `ListResourceTags` |
| Secrets Manager | `AWS::SecretsManager::Secret` | `DescribeSecret` |
| SNS | `AWS::SNS::Topic` | `ListTagsForResource` |
| SSM | `AWS::SSM::Parameter` | `ListTagsForResource` |
| ACM | `AWS::CertificateManager::Certificate` | `ListTagsForCertificate` |
| CloudFront | `AWS::CloudFront::Distribution` | `ListTagsForResource` |
| AWS Config | `AWS::Config::ConfigRule`, `AWS::Config::ConfigurationRecorder` | `ListTagsForResource` |

The last fifteen services carry
[#819](https://github.com/scttfrdmn/substrate/issues/819)'s twenty-three types, added in three parts.
Two conditions decided that cut, both checked against the owning plugin rather than assumed: the
service's tag record has a merge arm behind substrate's one tag writer — the same writer the Resource
Groups Tagging API uses, so a stamp and a `TagResources` call cannot merge differently — and the
physical ID CloudFormation records is already exactly the identifier that plugin keys its record by.
A service that fails the second condition needs the key re-derived, which is where a stamp lands
somewhere nothing reads.

Twelve of the twenty-three waited on the first condition — their services kept tag state no merge arm
reached, so there was nowhere for a stamp to land that the owning service would read, and
`TagResources` could not reach them either: one defect with two symptoms, closed as
[#835](https://github.com/scttfrdmn/substrate/issues/835). Seven of those twelve then failed the
*second* condition and are resolved individually rather than from the type table, and the reasons are
worth naming because each is a way the two identifiers can silently differ: a KMS key's, a secret's
and a topic's physical ID is an **ARN** where the record's key holds a bare ID or name, so each is
resolved through its own service's ARN resolver; a distribution's key carries **no Region**, because
CloudFront is global and its ARN's Region field is empty; a parameter's key carries the leading `/`
that `PutParameter` adds and the physical ID does not; and an ECS service's and task definition's key
has **four** segments, one of which — the cluster, and the revision — appears only in the ARN.

Read-back is through whatever the owning service calls its tag-reading operation, which is not always
`ListTagsForResource`: ACM publishes `ListTagsForCertificate`, and Secrets Manager publishes no
tag-reading operation at all, so a secret's tags are read where a caller reads them — off
`DescribeSecret`.

Two resolvers sit behind the one writer. EC2's keys on the physical ID's prefix, because an EC2
ID carries its type; every other service's keys on the **CloudFormation resource type**, because
outside EC2 a physical ID is a bare name — a bucket named `orders` and a queue named `orders` are
the same string, so there is nothing in the ID to switch on. EC2's resolver is tried first, and a
type neither claims is skipped.

**A resource whose service models no tags is skipped silently, and that is deliberate rather
than an omission.** AWS declines to publish an exhaustive propagation list of its own: "The
propagation of stack-level tags to resources, including tags with the `aws:` prefix, varies by
resource type. For example, tags aren't propagated to Amazon EBS volumes that are created from
block device mappings." So substrate states its rule instead — a resource is stamped when
substrate models tags for its service *and* a caller can read the tag back through that
service's own API — and names what that leaves out, in three groups. There is no log line per
skipped resource: a stack creates far more of those than of the kinds that can be stamped, so a
warning each would bury a real one.

**Tag state but no tagging operation: out of scope.** An IAM user or role, an API Gateway v1 or
v2 API and a Cognito user pool each carry a tag field in substrate's records, but no operation
reads or writes it. Stamping them would write a tag **no API call could observe**, and an
observation a caller cannot make is outside substrate's emulation boundary — the boundary is
what an AWS API call can see, not what a record happens to hold (see the *Scope* section of
`CLAUDE.md`). IAM has a second, independent reason: no AWS page states that an IAM entity
receives the stamp, and substrate's `TagRole` refuses an `aws:`-prefixed key, so a stamped IAM
tag would be one no caller could ever set or remove. These three are therefore **decided out**
rather than deferred. Should one of them gain a tagging surface, the stamp becomes observable
and the decision is worth revisiting on that ground alone.

**No tag state at all: deferred, on AWS's own licence.** Roughly twenty services the deployer
can create resources in keep no tag state whatsoever, among them CloudWatch Logs, EventBridge,
Route 53, Athena, CodeBuild, CodePipeline, CodeDeploy, CloudTrail, OpenSearch, WAFv2, Backup,
Budgets, Firehose, MSK, Transfer, SES v2 and AppSync. Each needs a tag store **and** a tagging
API before a stamp could be observed at all, which is a per-service piece of work rather than a
line here. The omission is licensed by the sentence quoted above: propagation "varies by
resource type", so a partial cut is what AWS itself describes rather than a shortfall against a
published list. [#819](https://github.com/scttfrdmn/substrate/issues/819) keeps the list in one
place; it is split per service when one is picked up.

**Every type that had a tagging surface and no stamp now has both.** The missing piece for twelve of
them was never a resolver arm alone but an arm in that shared *writer*, so the same gap also meant the
Resource Groups Tagging API could not tag them: one defect with two symptoms, closed as
[#835](https://github.com/scttfrdmn/substrate/issues/835) a row at a time — ECS's service and task
definition, an RDS DB cluster and DB subnet group, a Step Functions activity, an ACM certificate, a
CloudFront distribution, a KMS key, an SNS topic, a Secrets Manager secret and an SSM parameter —
each keying through the one state-key builder the owning service's own tag operation uses, and each
merging its record as raw JSON so a member the writer does not model is preserved rather than
dropped. The last piece of that was the ECS namespace's **scanner** half: `GetResources` enumerated
ECS clusters only, so a tag `TagResources` had written to a service, a task or a task definition was
readable through ECS's own `ListTagsForResource` and invisible to the tagging API's own inventory
call ([#935](https://github.com/scttfrdmn/substrate/issues/935)). With the writer in place, the
CloudFormation half followed: all twelve are in the table above.

**AWS Config was the thirteenth, and it needed a writer of its own rather than an arm in the shared
one.** Config keeps a resource's tags in a **side-car** state record keyed by ARN whose whole document
*is* the tag map, rather than on the resource — which is deliberate, since neither AWS's
`ConfigurationRecorder` shape nor its `ConfigRule` shape has a `Tags` member and a tag field on either
would emit a member AWS never emits. The shared writer's two merge helpers both look for a tag member
*inside* a record, and here there is none to merge into. Three consequences shape the arm, and each is
observable:

- The stamp **creates** the side-car, where every other arm refuses a record that is absent — a
  resource created with no tags has no side-car at all.
- Because the record proving the resource exists is a different key from the one holding its tags,
  the existence check is made against the **resource**, through the same lookup
  `ListTagsForResource` uses rather than a second copy of it. Reading the side-car alone cannot
  tell "this rule has no tags" from "there is no such rule".
- The key is the **ARN**, not the physical ID. A rule's physical ID is its name while its ARN names
  it by a hashed `ConfigRuleId`, and a recorder's ARN carries a minted `RecorderId` no API member
  holds, so both are read back from the service at deploy time; an ARN that could not be read leaves
  the resource unstamped rather than stamped under a guess.

`AWS::Config::DeliveryChannel` is the one type the deployer creates that stays out, and not for want
of a writer. `TagResource`'s `ResourceArn` enumerates the nine resources Config can tag — a
configuration recorder, a Config rule, an organization Config rule, a conformance pack, an
organization conformance pack, a configuration aggregator, an aggregation authorization, a stored
query and a connector — and a delivery channel is **not among them**. So no `TagResource` call can
name one, the `DeliveryChannel` shape has no `arn` member to be named by, and substrate's deploy
records none. It deploys and is skipped in silence.

One further limit, stated because the side-car makes it look like a gap. Config's writer deletes the
side-car outright when the last tag leaves it, so an untagged resource stays distinguishable from one
holding `{}`. A stack's tag reconciliation cannot reach that case: the three `aws:cloudformation:*`
keys are stamped before the stack tags are reconciled, so a resource a stack touched always carries at
least three tags. The rule therefore lives with Config's own `UntagResource`, which can reach it,
rather than being restated at the CloudFormation caller where nothing could exercise it.

Three further limits, each named because a policy or an assertion written against the stamp will
otherwise assume more:

- **A resource that failed to deploy is not stamped**, and neither is one with no physical ID.
  Tagging it would put a stack's bookkeeping on something the stack does not own.
- **Caller-supplied stack tags reach the same resources, by a different rule.** `CreateStack`'s
  `Tags.member.N` is propagated as well ([#764](https://github.com/scttfrdmn/substrate/issues/764)),
  to exactly the services in the table above — but the three keys here are substrate's own and are
  upserted unconditionally, where a stack tag yields to a value the caller set directly. See [A
  stack tag reaches the resources the stack
  creates](/reference/cloudformation#a-stack-tag-reaches-the-resources-the-stack-creates).
- **A physical ID that merely looks like an EC2 ID is stamped as one.** EC2's resolver is tried
  first and keys on the prefix alone, so an S3 bucket a template names `i-something` resolves as
  an instance. No AWS naming rule prevents it and substrate does not check for it.

Provenance: **all three keys are documented**, on the Template Reference's *Resource tag* page —
"CloudFormation automatically creates the following stack-level tags with the `aws:` prefix:
`aws:cloudformation:{{logical-id}}`, `aws:cloudformation:{{stack-id}}`,
`aws:cloudformation:{{stack-name}}`". This corrects an earlier note here which said that only
`aws:cloudformation:stack-name` was reachable and that the triple was observed behaviour: the
pages documenting the set returned empty bodies when the stamp was first implemented, and the
Template Reference page settles it. The same page supplies the "varies by resource type"
sentence and the EBS carve-out quoted above.

What still resolves to `*`, each named because a policy written against it will not behave
as AWS would:

- **`NetworkInterfaceId`.** Substrate stores no standalone taggable ENI record, so there are
  no tags to compare and `AmazonEKSClusterPolicy`'s `ec2:DeleteNetworkInterface` statement
  stays inert. Fabricating an empty tag set would be worse: it would turn "substrate cannot
  tell" into "the tag is absent", deciding the condition rather than admitting it cannot.
- **`VpcId` and `SubnetId`.** Several creates carry one as the *container* the new resource
  goes into rather than as the resource acted on, and authorizing a create against its
  parent's ARN is not what AWS evaluates it against.

An ID whose prefix names no type substrate can tag, or a `pg-`/`key-` ID naming no record, is
**skipped**: it contributes no resource to the decision, so a batch of one resolvable and one
unparseable ID is authorized as the batch of one. The refusal such an ID is owed is the
handler's, as `Malformed` or `NotFound`, not an `AccessDenied` naming a resource that does not
exist — and for the operation where it would matter most it cannot launder a state change
past a policy, because AWS documents `TerminateInstances` as all-or-nothing ("If you specify
multiple instances and the request fails (for example, because of a single incorrect instance
ID), none of the instances are terminated") and substrate resolves every ID before it
terminates any. `StopInstances` and `StartInstances` are not atomic, so there the batch is
simply authorized as the resources that exist. Substrate states no ordering between the 403
and the 404: no AWS page publishes one.

The case is narrower than it sounds, and worth stating so a policy is not written against a
wrong reading of it: a *well-formed* ID naming no record is not skipped. An instance's ARN is
built from its ID rather than looked up, so `i-0123…` naming nothing still gets its own ARN
with an empty tag set, and a least-privilege policy still has to name it.

A request naming no resolvable ID at all falls back to `*`. That fallback is safe in one
direction only, which is why it is the fallback: `*` as the *request* resource is not a
wildcard, because `resourceMatches` uses the statement's own `Resource` as the pattern, so
`*` matches only a statement whose `Resource` begins with `*`. Replacing it with concrete
ARNs can therefore narrow a grant but never widen one.

### A tagged create is authorized twice

A create that carries tags is two authorization decisions, not one. AWS: "If tags are
specified in the resource-creating action, Amazon performs additional authorization on the
`ec2:CreateTags` action to verify if users have permissions to create tags. Therefore, users
must also have explicit permissions to use the `ec2:CreateTags` action."

So `ec2:RunInstances` alone launches an untagged instance and **refuses a tagged one** —
which is what real EC2 does, and the point of the rule: a policy that withholds
`ec2:CreateTags` is how AWS expects you to stop a caller writing tags at all.

The second pass runs only when the request actually carries tags, per AWS: "The
`ec2:CreateTags` action is only evaluated if tags are applied during the resource-creating
action." An untagged create is decided exactly as before, as is a `TagSpecification.N` that
names a `ResourceType` but no `Tag.M`. It also runs *after* the primary decision succeeds, so
a caller missing both permissions is told about the create first.

**What it is authorized against** is the resources the create will *make*, one wildcard per
distinct `TagSpecification.N.ResourceType`:

| Request | Second pass authorizes `ec2:CreateTags` on |
|---|---|
| `RunInstances` tagging `instance` | `arn:aws:ec2:<region>:<account>:instance/*` |
| `RunInstances` tagging `instance` and `volume` | both `instance/*` and `volume/*` |
| `CreateVolume` with `TagSpecification` | `arn:aws:ec2:<region>:<account>:volume/*` |
| `CreateSubnet` with `TagSpecification` | `arn:aws:ec2:<region>:<account>:subnet/*` |

Not the resources the create *reads*. A launch is authorized against its image, subnet and
security group; its tags land on an instance and a volume, and AWS's own example turns on that
distinction — it scopes the grant to `instance/*` and says "Users cannot tag existing
resources, and users cannot tag volumes using the `RunInstances` request." A launch that tags
both scopes therefore needs the grant on both types; one written for `instance/*` alone refuses
the volume half, and the denial names `volume/*` so it is clear which ARN to add.

The `<type>/*` wildcard is **substrate's reading**, not a documented rule — the resources have
no IDs when the decision is made, so no concrete ARN exists to authorize against. It is the
shape all four of AWS's example policies for this key are written against (`*/*`,
`instance/*`), which is why it is the one chosen. A `ResourceType` substrate does not otherwise
model is passed through as written rather than filtered out; filtering could only ever produce
a false *allow*.

**`ec2:CreateAction`** carries the creating operation's name — `RunInstances`, `CreateVolume` —
so AWS's documented shape works verbatim:

```json
{"Effect": "Allow", "Action": "ec2:CreateTags",
 "Resource": "arn:aws:ec2:us-east-1:111122223333:instance/*",
 "Condition": {"StringEquals": {"ec2:CreateAction": "RunInstances"}}}
```

The key is **absent** on a direct `CreateTags` or `DeleteTags`, which is what makes that
statement mean "tag during a launch, and not otherwise" — AWS's "Users cannot tag existing
resources". A policy wanting to refuse only standalone tagging gates on `"Null":
{"ec2:CreateAction": "false"}`, the one construct that tells an absent key apart from one
present with a different value. The value is case-sensitive, so a grant written for
`CreateVolume` does not admit a `RunInstances`; the key *name* is not, so a policy writing
`ec2:createaction` is evaluated exactly as one writing `ec2:CreateAction` — see [condition
key names are matched
case-insensitively](/reference/iam#condition-key-names-are-matched-case-insensitively), which is global
to the evaluator rather than specific to this key.

**Tags a launch template supplies are authorized the same way**, per AWS: "The
`ec2:CreateTags` action is also evaluated if tags are provided in a launch template." The
template is resolved through the same lookup the launch's *resource* authorization uses, so one
launch's two decisions cannot read different template versions. Precedence is per scope and
mirrors the handler: the template's instance tags apply only when the request named none of its
own, and its volume tags only when the request named no volume tags — so a request that
overrides a scope authorizes the tags it actually sends for that scope.

`aws:RequestTag/{key}` and `aws:TagKeys` in the second pass are the first pass's values plus
whatever the template contributed, so the two decisions cannot disagree about what the request
asked for. No `aws:ResourceTag/*` is populated: the resources do not exist yet, so they have no
tags to match.

A permission boundary applies to the second pass as it does to the first — a boundary that
omits `ec2:CreateTags` refuses a tagged create even when the identity policy permits it.

## Instance attributes

`DescribeInstanceAttribute` reads one attribute off an instance. It is the only way
to read an instance's **user data** back: `RunInstances` recorded `UserData` and
nothing could observe it, so a consumer could not assert that the user data their IaC
intended reached the instance — including the value a
[launch template supplied](#a-launch-template-merges-with-the-request-field-by-field).

Five attributes are readable, being the ones that correspond to state substrate holds:

| `Attribute` | Reports |
|---|---|
| `userData` | The value as stored, still base64-encoded |
| `instanceType` | |
| `disableApiTermination` | `true` or `false`; recorded at launch — from the request or its [launch template](#what-a-launch-template-stores) — and by `ModifyInstanceAttribute` |
| `groupSet` | `groupSet>item` with `groupId`/`groupName`, the same shape [`DescribeInstances` reports](#security-groups-on-an-instance) |
| `blockDeviceMapping` | `blockDeviceMapping>item`, the same set [an instance reports](#an-instance-reports-its-own-block-devices) |

Scalar values are **wrapped in a `<value>` element**, which all three of the
reference's worked examples show and which matches the `AttributeValue` type the
response elements carry:

```xml
<DescribeInstanceAttributeResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
  <instanceId>i-0123456789abcdef0</instanceId>
  <instanceType><value>t3.micro</value></instanceType>
</DescribeInstanceAttributeResponse>
```

Exactly one attribute appears per response — the one asked for. `groupSet` and
`blockDeviceMapping` are the exceptions to the wrapper: they are arrays, not
`AttributeValue`s. Both are rendered as a **present-but-empty** element when the
instance has none, rather than an omitted one, because an SDK maps a present-but-empty
element to an empty slice and an omitted one to nil.

`Attribute` is **Required: Yes**, so an absent one fails with `MissingParameter`
(`The request must contain the parameter Attribute`). An unknown instance ID fails
with `InvalidInstanceID.NotFound`, and a malformed one with `InvalidInstanceID.Malformed`,
as [everywhere else](#explicit-resource-ids).

### Unmodelled attributes are refused, not defaulted

Every other name in the documented valid-values list — `kernel`, `ramdisk`,
`sourceDestCheck`, `productCodes`, `ebsOptimized`, `rootDeviceName`,
`sriovNetSupport`, `enaSupport`, `enclaveOptions`,
`instanceInitiatedShutdownBehavior`, `disableApiStop` — is rejected:

```
InvalidParameterValue: Value (enaSupport) for parameter attribute is invalid. Unknown attribute.
```

The status is `400`, and the offending value is interpolated. Refusing is deliberate
rather than a gap: answering `sourceDestCheck` with a default `false` would be
indistinguishable from a real instance that has it disabled, and a consumer asserting
on it would get a green test built on a value substrate invented.

This message has the strongest provenance of any in the EC2 plugin. It is **captured
from real AWS** in [aws/aws-cli#4273](https://github.com/aws/aws-cli/issues/4273),
where `aws ec2 describe-instance-attribute --attribute enaSupport` returns exactly it,
and it is byte-identical to [moto](https://github.com/getmoto/moto)'s string — a
capture and an independent reimplementation agreeing. The reference could not have
supplied it: `DescribeInstanceAttribute`'s Errors section is empty.

`enaSupport` is the case that makes the boundary concrete. It is *in* AWS's own valid-values
list, and the same reference says "Note that the `enaSupport` attribute is not supported."
Real AWS rejects a value its documentation lists, and #4273 is the capture of that
rejection — so substrate rejecting it is fidelity rather than a shortfall.

Attribute names are matched **case-sensitively**, as AWS's valid values are: `InstanceType`
is rejected, `instanceType` is not.

### An attribute that was never set

An unset attribute is reported as a **present but empty element** — `<userData></userData>`
— rather than an omitted one. An empty `groupSet` likewise appears, empty.

This is the one shape here the reference cannot settle: all three of its worked examples
show an attribute that *has* a value. It ships from moto's `test_describe_instance_attribute`,
which asserts `response["UserData"] == {}` — an empty mapping, which is what an SDK
produces from a present element with no children. That is **weaker provenance than a
capture**, and worth stating, because the two shapes are not interchangeable to a caller:
an SDK maps a present-but-empty element to an empty struct and an omitted one to nil, so
`resp.UserData.Value` panics under one and not the other.

### Modifying an attribute requires a stopped instance

`ModifyInstanceAttribute` writes `InstanceType.Value`, `UserData.Value` and
`DisableApiTermination.Value`. The first two require the instance to be `stopped`:

```
IncorrectInstanceState: The instance 'userData' attribute cannot be modified while the
instance is in the 'running' state; stop the instance first
```

The status is `400`. The **code** is documented — EC2's client-error table lists
`IncorrectInstanceState` as "some instance attributes, such as user data, can only be
modified if the instance is in a 'stopped' state" — while the **message text is
substrate's own**, since the table describes the condition rather than quoting the
string AWS sends, and no capture of this rejection was found.

This is a behaviour change for `instanceType`, which substrate previously changed on a
running instance. `ModifyInstanceAttribute`'s Example 1 states the requirement plainly:
"The instance must be in the `stopped` state." A test that asserted the old behaviour
will now see a `400`.

`disableApiTermination` is deliberately **exempt** from the gate, because `RunInstances`'
reference says so: "You can enable termination protection when you launch an instance,
while the instance is running, or while the instance is stopped." Gating it would refuse
a call real EC2 accepts — the same class of defect as accepting one real EC2 refuses,
just harder to notice, since it looks like extra rigor.

`UserData.Value` is read with a presence check rather than a non-empty one, so clearing
an instance's user data is expressible: `UserData.Value=` on a stopped instance empties
it, and the attribute then reads back as the empty element above.

### Termination protection is honored, one Availability Zone at a time

`TerminateInstances` refuses a protected instance with `OperationNotPermitted`, HTTP
`400`, and the instance stays `running`. The **code** is documented — EC2's client-error
table lists `OperationNotPermitted` as "The specified operation is not allowed" and names
this case first among its examples, "you might be trying to terminate an instance that has
termination protection enabled" — while the **message text is substrate's own**, since no
capture of the string AWS sends was found. It interpolates the instance ID and names the
attribute to clear, which is what a caller acts on:

```
OperationNotPermitted: The instance 'i-0123456789abcdef0' may not be terminated. Modify
its 'disableApiTermination' instance attribute and try again.
```

A request naming both protected and unprotected instances is where this gets
counter-intuitive, and it is worth reading the reference's own words, because the answer is
neither "the whole request is refused" nor "the unprotected instances are terminated":

> If you terminate multiple instances across multiple Availability Zones, and one or more
> of the specified instances are enabled for termination protection, the request fails with
> the following results:
>
> - The specified instances that are in the same Availability Zone as the protected
>   instance are not terminated.
> - The specified instances that are in different Availability Zones, where no other
>   specified instances are protected, are successfully terminated.

Partial failure is scoped to the **Availability Zone**. So for the reference's own worked
example — A and B unprotected in `us-east-1a`, C protected and D unprotected in
`us-east-1b`, all four named in one request:

| Instance | Zone | Protected | Outcome |
|---|---|---|---|
| A | `us-east-1a` | no | **terminated** |
| B | `us-east-1a` | no | **terminated** |
| C | `us-east-1b` | yes | still `running` |
| D | `us-east-1b` | no | still `running` — it shares C's zone |

The request itself reports `OperationNotPermitted`, naming C, **after** the terminations in
`us-east-1a` have been persisted. An unprotected instance sharing a zone with a protected
one survives; an unprotected instance in another zone does not.

Because the grouping key is the zone, every instance carries one. It is resolved at launch
from `Placement.AvailabilityZone`, or from the subnet's zone when the launch named only a
`SubnetId`, or from the region's first zone (`<region>a`) when it named neither — matching
the reference's "EC2 automatically selects an Availability Zone for you". `AvailabilityZoneId`
is not modelled. The zone is reported by `RunInstances` and `DescribeInstances` as
`<placement><availabilityZone>`, and `DescribeInstances` accepts an `availability-zone`
filter, so a caller can work out in advance which of their instances a terminate would
spare. (The filter is named `availability-zone`, not `placement.availability-zone` — the
placement family's filter names are spelled out individually in the reference's list.)

An instance unmarshalled from an **event log recorded before this field existed** reads back
with an empty zone, which groups all such instances together. That is the conservative
reading, being what a single-zone account looks like.

A bad instance ID still fails the whole request before anything is written, per "If you
specify multiple instances and the request fails (for example, because of a single incorrect
instance ID), none of the instances are terminated." The protection scan runs as a second
pre-flight pass, after every named instance resolves, so no state is written for a zone that
is about to be refused. Terminating an already-terminated instance still succeeds; the
operation is idempotent and the protection check does not change that.

`DeleteFleets --terminate-instances` goes through the same handler, so the rule applies
there too: a protected fleet instance **survives** its fleet's deletion, an unprotected
sibling in another zone does not, and `DeleteFleets` propagates the `OperationNotPermitted`
rather than folding it into `unsuccessfulFleetDeletionSet`. `DeleteFleetError`'s documented
codes are exactly `fleetIdDoesNotExist`, `fleetIdMalformed`, `fleetNotInDeletableState` and
`unexpectedError`, none of which covers termination protection, so folding it in would mean
answering `unexpectedError` and losing the code the caller acts on.

One divergence remains, unrelated to protection and pre-existing: substrate reports a
terminated instance as code `48` `terminated` immediately, where real EC2 reports code `32`
`shutting-down` first and settles to `48`. A consumer polling for `shutting-down` will never
observe it. That is tracked separately.

## Spot Instance requests

**`RunInstances` launches a Spot Instance, and the three Spot request operations are routed**
([#1436](https://github.com/scttfrdmn/substrate/issues/1436)). Before it,
`RequestSpotInstances`, `DescribeSpotInstanceRequests` and `CancelSpotInstanceRequests` answered
`InvalidAction`, and `RunInstances` ignored `InstanceMarketOptions` — a launch asking for Spot got
an On-Demand instance with no request behind it.

`InstanceMarketOptions.MarketType=spot` on `RunInstances` — or a `RequestSpotInstances` — launches
Spot Instances, each with its own `sir-` request, linked both ways: the instance reports
`instanceLifecycle=spot` and `spotInstanceRequestId`, and the request reports `instanceId`. A
request is `active` / `fulfilled` at once, where AWS passes through `pending-evaluation`; substrate
models no evaluation delay. `TagSpecification.N` with `ResourceType=spot-instances-request` tags the
request, and the instance takes only its own `instance`-scoped tags. `DescribeInstances` evaluates
`instance-lifecycle` and `spot-instance-request-id`.

The `SpotMarketOptions` rules `RunInstances` states are enforced: `SpotInstanceType` is `one-time`
or `persistent`, `InstanceInterruptionBehavior` is `hibernate`, `stop` or `terminate`, a `MaxPrice`
must be more than USD $0.001 (`InvalidParameterValue`), "persistent Spot Instance requests are only
supported when the instance interruption behavior is either hibernate or stop", and `ValidUntil` is
"Supported only for persistent requests" (both `InvalidParameterCombination`, a code substrate
chose — the page publishes none). `RequestSpotInstances` allows a persistent `terminate` request, as
its own page does, and defaults a one-time request's `ValidUntil` to seven days from creation.

**What each request goes through**, using the status codes of the EC2 User Guide's "Spot request
status" list:

| Event | Request state | Status code |
|---|---|---|
| Launched | `active` | `fulfilled` |
| `CancelSpotInstanceRequests`, instance running | `cancelled` | `request-canceled-and-instance-running` |
| `CancelSpotInstanceRequests`, no instance | `cancelled` | `canceled-before-fulfillment` |
| Its instance terminated — a one-time request, or a persistent one past `ValidUntil` | `closed` | `instance-terminated-by-user` |
| Its instance terminated — a cancelled request | `cancelled` | `instance-terminated-by-user` |
| Its instance terminated — an active persistent request | `active`, with a **replacement** | `fulfilled` |

"Canceling a Spot Instance request does not terminate running Spot Instances associated with the
request." The last row is the behaviour a cancel-before-terminate path exists for: terminating the
instance of an active persistent request launches a replacement from the request's own launch —
**with no tags**, since a launch specification carries none — and moves the request's `instanceId`
to it, inside the same `TerminateInstances` call. A cancelled request launches nothing. A
replacement the launch refuses (its AMI deregistered since, say) leaves the request `open` with
`bad-parameters`.

**An open request is re-evaluated on observation.** Each `DescribeSpotInstanceRequests` looks at
every request a [seeded outcome](#seeding-a-spot-request-outcome) left `open`: once no seed blocks
its instance type it is fulfilled — an instance launched from its recorded launch, the request
`active`/`fulfilled` — and once its `ValidUntil` has passed it is `closed` with `schedule-expired`.
So a wait-for-fulfillment loop terminates when the seed is cleared, and a replay of the same
describe launches the same instance.

**CreateFleet's Spot pools launch Spot Instances** — `instanceLifecycle=spot`, so
`instance-lifecycle=spot` selects them — without a `sir-` request: CreateFleet publishes no Spot
Instance request, and the fleet record is what tracks the capacity. The request-outcome seed does
not apply to them; a fleet's shortfall has [its own](#seeding-ec2-fleet-partial-fulfillment).

`DescribeSpotInstanceRequests` asserts existence on `SpotInstanceRequestId.N`
(`InvalidSpotInstanceRequestID.NotFound`, `.Malformed`), evaluates twenty-two of its forty
filters (see [What is refused, and what is merely inert](#what-is-refused-and-what-is-merely-inert)),
and paginates on `MaxResults`/`NextToken` — with no ceiling, because the page publishes no Valid
Range, only a recommendation. `CancelSpotInstanceRequests` resolves every ID before cancelling any,
so an unknown one cancels nothing. `CreateTags` reaches a request by its `sir-` ID.

Not modeled: Spot pricing, interruption and rebalance notices — an instance runs until something
terminates it — `ValidFrom`, `BlockDurationMinutes`, launch-group and Availability-Zone-group
enforcement (both are recorded and filterable), and the deletion of a request "four hours after
they are canceled and their instances are terminated". A capacity shortfall is
[seedable](#seeding-a-spot-request-outcome) instead.

## MinCount and MaxCount

A count that is **present but invalid** fails with `InvalidParameterValue`,
HTTP 400. A count that is **absent** still defaults.

| Request | Result |
|---|---|
| Neither given | 1 instance |
| `MinCount=2` alone | 2 instances — an absent `MaxCount` defaults to `MinCount`, not to 1 |
| `MaxCount=4` alone | 4 instances |
| `MinCount=1&MaxCount=3` | 3 instances |
| `MinCount=0`, or either count `< 1` | `Invalid value '0' for parameter minCount. It must be at least 1.` |
| Either count unparseable | `Invalid value 'abc' for parameter minCount. It must be an integer.` |
| `MinCount=3&MaxCount=1` | `Invalid value '1' for parameter maxCount. The maxCount must be equal to or greater than the minCount '3'.` |

A successful launch always creates `MaxCount` instances. AWS "launches the largest
possible number of instances above the specified minimum count", and substrate
models no capacity ceiling, so the largest possible number is always the maximum
asked for and `MinCount` can only ever be satisfied.

Absence defaults rather than erroring even though AWS marks both **Required: Yes**,
because in every typed SDK they are required members that fail client-side — so a
consumer bug there cannot reach the wire, while requiring presence here would break
hand-built form-encoded requests. A value that is present and invalid *is*
reachable: the query protocol carries these as strings, and neither botocore's
`ParamValidator` nor `aws-sdk-go-v2` range-checks them.

The error code is the common-error `InvalidParameterValue` because the RunInstances
reference documents no action-specific error for these. `MinCount > MaxCount` uses
it too, rather than `InvalidParameterCombination` — that code is defined as
"Parameters that must not be used together were used together", which cannot
describe two parameters AWS documents as used together; the defect is the *value*.

The upper bound is a per-account, per-instance-type quota substrate does not model,
so it is not enforced: any count at or above 1 is accepted.

## Instance types are a seeded catalog

`DescribeInstanceTypes`, `DescribeInstanceTypeOfferings` and
`DescribeSpotPriceHistory` all answer from one seeded catalog. It is **not
exhaustive** — EC2 offers some 800 types — but it is **complete per family**:

| Family | Sizes |
|---|---|
| `t3`, `t3a` | `nano`, `micro`, `small`, `medium`, `large`, `xlarge`, `2xlarge` |
| `m5`, `m5a`, `r5`, `c5a` | `large`, `xlarge`, `2xlarge`, `4xlarge`, `8xlarge`, `12xlarge`, `16xlarge`, `24xlarge` |
| `c5` | `large`, `xlarge`, `2xlarge`, `4xlarge`, `9xlarge`, `12xlarge`, `18xlarge`, `24xlarge` — note the ladder is **not** the same as `c5a`'s |
| `p3` | `2xlarge`, `8xlarge`, `16xlarge` |
| `p4d`, `p4de` | `24xlarge` — AWS publishes these as two families, not two sizes of one |
| `p5` | `4xlarge`, `48xlarge` |
| `g4dn` | `xlarge`, `2xlarge`, `4xlarge`, `8xlarge`, `12xlarge`, `16xlarge` |
| `g5`, `g6` | `xlarge`, `2xlarge`, `4xlarge`, `8xlarge`, `12xlarge`, `16xlarge`, `24xlarge`, `48xlarge` |
| `inf1` | `xlarge`, `2xlarge`, `6xlarge`, `24xlarge` |
| `inf2` | `xlarge`, `8xlarge`, `24xlarge`, `48xlarge` |
| `trn1` | `2xlarge`, `32xlarge` |
| `trn2` | `3xlarge`, `48xlarge` |
| `t4g` | `nano`, `micro`, `small`, `medium`, `large`, `xlarge`, `2xlarge` — AWS Graviton2, burstable |
| `m6g`, `c6g`, `r6g`, `m7g`, `c7g`, `r7g` | `medium`, `large`, `xlarge`, `2xlarge`, `4xlarge`, `8xlarge`, `12xlarge`, `16xlarge` — AWS Graviton2 and Graviton3 |
| `m8g`, `c8g`, `r8g`, `m9g`, `c9g`, `r9g` | `medium`, `large`, `xlarge`, `2xlarge`, `4xlarge`, `8xlarge`, `12xlarge`, `16xlarge`, `24xlarge`, `48xlarge` — AWS Graviton4 and Graviton5 |
| `m6gd`, `c6gd`, `r6gd`, `m7gd`, `c7gd`, `r7gd`, `c6gn`, `c7gn` | `medium`, `large`, `xlarge`, `2xlarge`, `4xlarge`, `8xlarge`, `12xlarge`, `16xlarge` — Graviton2 and Graviton3 (Graviton3E for `c7gn`), instance store (`gd`) or network optimized (`gn`) |
| `m8gd`, `c8gd`, `r8gd`, `m9gd`, `c9gd`, `r9gd`, `c8gn` | `medium`, `large`, `xlarge`, `2xlarge`, `4xlarge`, `8xlarge`, `12xlarge`, `16xlarge`, `24xlarge`, `48xlarge` — Graviton4 and Graviton5 |
| `g5g` | `xlarge`, `2xlarge`, `4xlarge`, `8xlarge` (one NVIDIA T4G each), `16xlarge` (two) — AWS Graviton2 |

That is 349 types in forty-seven families. **The twenty-nine Graviton families report `arm64`** in
`processorInfo.supportedArchitectures`, and every other type `x86_64`, so the
`processor-info.supported-architecture` filter selects one set or the other
([#1497](https://github.com/scttfrdmn/substrate/issues/1497)). Auto Scaling attributes them to the
`amazon-web-services` CPU manufacturer, and selects among them by the launch template's AMI
architecture — see [EC2 Auto Scaling](/reference/autoscaling)'s mixed instances.

The `gd` and `gn` variants and `g5g` arrived in
[#1509](https://github.com/scttfrdmn/substrate/issues/1509). Each variant's vCPU and memory figures
equal its base family's size for size — the guide pages publish them that way, `gn` included. The
`gd` families carry NVMe SSD instance store on every size, so Auto Scaling's `LocalStorage`
attribute counts them as having it; the `gn` families and `g5g` are EBS-only. The catalog does not
report `instanceStorageInfo`, so the storage itself is not described
([#1535](https://github.com/scttfrdmn/substrate/issues/1535)). Their `.metal`, `.metal-24xl`
and `.metal-48xl` sizes are not carried, as with every family.

Whole families rather than a sample, because an absent type is *refused* (below) —
a catalog stopping at `c5.xlarge` would answer `InvalidInstanceType` for
`c5.large`, which is the right code for a bogus type and the wrong one for a real
one. Bare-metal sizes (`m5.metal`, `g4dn.metal` and friends) are deliberately
excluded: they are real types, but nothing else in the plugin models their
behaviour, so returning them would advertise fidelity that is not there. vCPU and
memory figures come from the AWS instance-type guides — general purpose, compute
optimized, memory optimized, and, for the accelerated families,
[accelerated computing](https://docs.aws.amazon.com/ec2/latest/instancetypes/ac.html)
(`p3` from the [previous generation](https://docs.aws.amazon.com/ec2/latest/instancetypes/pg.html)
page).

A family absent from the table above is refused outright, so the ones deliberately
left out are worth naming: `p3dn`, `trn1n`, `p5e` and `p5en` are single-size
families AWS publishes beside the ones here, and `trn2u.48xlarge` is published with
**no** accelerator count at all — almost certainly a documentation gap rather than a
zero-accelerator instance, and inferring `16` from `trn2.48xlarge` would be substrate
inventing a spec. Widening later is additive (#896).

### Which member reports which family's accelerator count

`InstanceTypeInfo` splits accelerators across five members — `gpuInfo`,
`neuronInfo`, `inferenceAcceleratorInfo`, `fpgaInfo` and `mediaAcceleratorInfo`.
Substrate models **two** of them, and a type reports its count through exactly one:

| Families | Member | Element |
|---|---|---|
| `p3`, `p4d`, `p4de`, `p5`, `g4dn`, `g5`, `g5g`, `g6` | `gpuInfo` | `gpuInfo>gpus>item>count` |
| `inf1`, `inf2`, `trn1`, `trn2` | `neuronInfo` | `neuronInfo>neuronDevices>item>count` |

So an Inferentia or Trainium type renders **no `gpuInfo` element at all** — not a zero
count — and an NVIDIA type renders no `neuronInfo`. A non-accelerated type renders
neither.

The `gpuInfo` half of that predates #896 and matches real EC2. The `neuronInfo` half is
#1029, and it *replaces* an earlier reading: substrate previously held the Inferentia
and Trainium counts internally and reported them nowhere, on the ground that no
reference page states which family populates which member. Half of that still stands —
neither `NeuronInfo` nor `InferenceAcceleratorInfo` names a family — but the conclusion
does not, because `InferenceAcceleratorInfo`'s own page carries *"Amazon Elastic
Inference is no longer available"*, which removes it as a candidate and leaves
`neuronInfo` as the one live member for a device the Neuron SDK drives. Reporting a
count substrate already had is a smaller reading than silently dropping it.

**Only `count` is reported**, and the omissions are deliberate. `NeuronDeviceInfo`
publishes `count`, `name`, `coreInfo` and `memoryInfo`, and `NeuronInfo` publishes
`totalNeuronDeviceMemoryInMiB`; all are `Required: No` and **not one of the four besides
`count` carries a valid-values list or an example**, so substrate would be inventing a
device name and two memory figures per type. It reports the count it can source and
omits the rest rather than reporting a placeholder (#1013's rule: a member substrate
does not model is absent, not empty). One `neuronDevices` item is rendered per type, the way `gpuInfo` renders one `gpus`
item: the catalog carries a single count per type, so the list is that count.

Accelerator counts are **not monotonic in size** and AWS publishes them that way, so
do not derive one from the size: `g4dn`, `g5` and `g6` each have a `12xlarge` carrying
four accelerators and a `16xlarge` carrying one, `g5`/`g6` have a `24xlarge` carrying
four below a `48xlarge` carrying eight, and `inf2`'s counts run 1, 1, 6, 12. Read the
count from `DescribeInstanceTypes`.

### `currentGeneration` is per family, and `p3` is the one that is `false`

`DescribeInstanceTypes` reports `currentGeneration` from the family, not as a constant:
**207 of the 210 catalogued types report `true` and `p3`'s three report
`false`**. The `current-generation` filter is evaluated to match, so
`Name=current-generation,Values=false` selects exactly `p3.2xlarge`, `p3.8xlarge` and
`p3.16xlarge`, and `Values=true` selects the other 207.

The source is the EC2 Instance Types guide's
[Specifications for Amazon EC2 previous generation instances](https://docs.aws.amazon.com/ec2/latest/instancetypes/pg.html),
whose `Instance family` table publishes fifteen families — A1, C1, C3, C4, G3, I2, M1, M2,
M3, M4, **P3**, **P3dn**, R3, R4, T1 — and spells P3 as `p3.2xlarge | p3.8xlarge |
p3.16xlarge`. `API_DescribeInstanceTypes` describes the member only as *"Indicates whether
the instance type is current generation"* and names no family, so the guide is where the
enumeration comes from. Substrate transcribes all fifteen rather than special-casing `p3`,
so a family added to the catalog later is classified by AWS's answer; fourteen of the
fifteen are families the catalog does not carry at all.

**There is a second AWS list and it is not the one used here.**
`https://aws.amazon.com/ec2/previous-generation/` omits P3 and P3dn entirely, names G2 where
the guide names G3, adds C2, CR1 and HS1, and lists M4, R4 and D2 as *upgrade targets* — i.e.
current. Taken as authority it would make every catalogued family current generation. It is a
marketing page about hardware AWS is steering customers off rather than a statement about what
the API reports, so the documentation page governs.

Until [#1028](https://github.com/scttfrdmn/substrate/issues/1028) this value was a hardcoded
`true` and the filter was inert, which hid each other: a constant cannot be narrowed on, and a
filter that narrows nothing cannot contradict a constant. Do not use `currentGeneration` to
decide whether substrate models a type — the catalog carries current and previous generations
alike, and a type it does not carry is refused with `InvalidInstanceType` regardless of
generation. Use the family table above.

### A type outside the catalog: refused, or empty?

Both — and which one depends on whether the parameter is an assertion or a filter.
This asymmetry is deliberate and matches real AWS; #485 diffed all three
operations against `us-east-1`.

| Request | Answer |
|---|---|
| `DescribeInstanceTypes --instance-types zz9.bogus` | `InvalidInstanceType`, HTTP 400 — `InstanceType.N` asserts the types exist |
| `DescribeInstanceTypeOfferings --filters Name=instance-type,Values=zz9.bogus` | **0 offerings, HTTP 200** — a filter that matches nothing is a legitimate empty answer |
| `DescribeSpotPriceHistory --instance-types zz9.bogus` | **Empty history, HTTP 200** — the reference describes this parameter as filtering the results |

Every unknown type in one `DescribeInstanceTypes` request is collected into a
single error, in request order:

```
InvalidInstanceType: The following supplied instance types do not exist: [zz9.bogus, aa1.nope]
```

One bad type fails the whole request; the known types are not returned. The
message is verbatim from a real `us-east-1` capture for the single-type case; the
`", "` separator for a list is substrate's choice, so dispatch on the code.

#### `RunInstances` accepts a type `DescribeInstanceTypes` refuses

`RunInstances` does **not** validate `InstanceType` against the catalog. It stores
whatever string it is given, and `DescribeInstances` reports it back, so:

```
aws ec2 run-instances --instance-type m7i.large ...   # succeeds
aws ec2 describe-instance-types --instance-types m7i.large   # InvalidInstanceType, HTTP 400
```

Two operations therefore disagree about whether a type exists, and the permissive one
is the one that creates state. Real EC2 refuses at launch. Substrate does not, and the
reason is the catalog's deliberate non-exhaustiveness: refusing here would move the
very failure the completeness invariant exists to prevent onto the operation that
creates state, and it would refuse types AWS plainly offers. That is not hypothetical —
`m7i.large`, `t2.micro`, `c7i.xlarge`, `t4g.nano`, `m7i.xlarge` and `c6a.xlarge` are all
launched by fixtures in this repository and none of them is in the catalog.

The consequence for a caller: **an instance's `instanceType` is recorded intent, not an
assertion that substrate models the type.** A launch tells you nothing about whether
`DescribeInstanceTypes`, `DescribeInstanceTypeOfferings` or `DescribeSpotPriceHistory`
will report that type. Closing the divergence means widening the catalog to every type
a consumer launches, which is the direction #896 took rather than tightening the launch
path.

`DescribeInstanceTypes` applies **six** of the fifty-six filter names its reference
documents — `current-generation`, `instance-type`, `memory-info.size-in-mib`,
`processor-info.supported-architecture`, `supported-usage-class` and
`vcpu-info.default-vcpus`. The other fifty are over response fields the seeded catalog does
not carry, so they are accepted and inert, and an undocumented name is refused. That split is
what closed [#495](https://github.com/scttfrdmn/substrate/issues/495)'s filter half: the concern
was never that the answerable handful should go unapplied, but that dropping the rest
*silently* is indistinguishable from applying them — which the
[evaluated/inert table](#what-is-refused-and-what-is-merely-inert) resolves by naming every
inert one.

**Fifty-six, corrected from fifty-seven.** Every count site said fifty-seven from
[#695](https://github.com/scttfrdmn/substrate/issues/695) until
[#1028](https://github.com/scttfrdmn/substrate/issues/1028). The names substrate accepts were
re-diffed against `API_DescribeInstanceTypes` one by
one on 2026-09-19 — nothing missing, nothing extra — and they come to **fifty-six**. There is no
tag filter to account for the difference: an instance type is not a taggable resource and the page
lists no `tag:` entry. Whether the page once published a fifty-seventh that has since been
withdrawn cannot be recovered from it, so the number states what it publishes now.

The numeric filters compare as **strings**, because AWS supports no
greater-than or less-than in a filter value: `memory-info.size-in-mib=4096` selects the types
with exactly that much memory, and `4097` selects none rather than "more than 4096".
`current-generation` compares the same way, against the literals `true` and `false` — see
[`currentGeneration` is per family](#currentgeneration-is-per-family-and-p3-is-the-one-that-is-false).

### Offerings filters and wildcards

`DescribeInstanceTypeOfferings` accepts exactly the two filter names its reference
documents — `instance-type` and `location` — and there is nothing it accepts but cannot
answer. **Any other name is refused** with `InvalidParameterValue`, which is now what
[every EC2 describe does](#one-rule-for-an-unrecognized-filter-name); this operation is where
that rule started. Multiple `Filter.N.Value.M` values are an OR; separate `Filter.N`
entries AND together.

Filter values honour EC2's documented wildcards and are case-sensitive, per
[the rules that apply to every EC2 filter](#wildcards-in-filter-values). This operation is
where that matcher started, which is why the examples there use instance types.

`LocationType` is a top-level **parameter**, not a filter name (`location-type` is
refused as a filter). `availability-zone` is the default, `region` returns one
offering per type located at the region, and `availability-zone-id` returns one per
type per zone located at the zone's **AZ ID** — the same `zoneId`
`DescribeAvailabilityZones` reports for that zone, since both read one derivation
(#893). A `location` filter under this `locationType` therefore matches an AZ ID:
`use1-az1` selects, `us-east-1a` selects nothing. Read the ID out of
`DescribeAvailabilityZones` rather than hardcoding it — the name→ID pairing substrate
reports is stable and a real account's is not, per the note below.

`outpost` is the one valid AWS value substrate does **not** model, and it is refused
with a message naming substrate — its location is an Outpost ARN, substrate seeds no
Outpost, and answering with zone *names* under a `locationType` claiming they are
Outpost ARNs is what a caller matching the two would silently mis-read.

The three zones `DescribeAvailabilityZones` reports are the same three the
offerings and spot-price operations use, so filtering an offerings query by a zone
you just enumerated always returns an answer.

#### Zone IDs take AWS's published shape, and always map zone `a` to `-az1`

`zoneId` is derived from the region: `us-east-1a` is `use1-az1`, `eu-west-1b` is
`euw1-az2`, `ap-southeast-2c` is `apse2-az3`. Every prefix reproduces a row of AWS's
[Availability Zones reference](https://docs.aws.amazon.com/global-infrastructure/latest/regions/aws-availability-zones.html).
Substrate emitted a different shape entirely until this release — `us-east-1` produced
`ue11`, with a doubled digit — which nothing caught because zone IDs were only ever
*emitted*: a test that read one out of a response and filtered on it was self-consistent
whatever the string was. `CreateVolume`'s `AvailabilityZoneId` made it an input, and a
consumer's fixture carrying the real `use1-az1` would have been refused.

The derivation is **one letter per compass word**, not AWS's own summarising sentence
("the first three letters of the Region code, followed by the number at the end"). That
sentence is refuted by the table it introduces: `ap-southeast-2` is `apse2`, not `aps2`,
and `ap-northeast-1` is `apne1`. The published table wins.

What substrate does **not** model is the per-account shuffle. AWS: "we independently map
Availability Zones to codes for each AWS account", so a real `us-east-1a` is `use1-az1`
in one account and `use1-az3` in another — which is the whole reason AZ IDs exist.
Substrate maps zone `a` to `-az1` in every account, because a deterministic emulator
cannot hold a per-account secret and a test asserting the pairing has to be able to pass.
Do not use substrate to verify that code correctly treats the name→ID mapping as
account-specific; it will agree with an assumption real AWS breaks.

### Spot prices are stubs

The `spotPrice` values are **deterministic stubs, not AWS prices**: substrate has
no price feed, and the numbers exist so a spot-price response has a plausible,
stable figure in it. Within a family they are a fixed rate per GiB, so they stay
monotonic in size. Assert on the *shape* of a spot-price response, never on the
amount. Every catalog type has a price — the two are generated together, so a type
cannot appear in `DescribeInstanceTypes` and be missing from
`DescribeSpotPriceHistory`.

## Explicit resource IDs

Naming a resource ID explicitly is an assertion that the ID exists, and EC2
answers it with an error rather than an empty result. `DescribeVpcs()` with no
arguments legitimately returns `[]`; `DescribeVpcs(VpcIds=["vpc-…"])` where that
VPC is absent fails.

- An ID that resolves to nothing → `Invalid<Type>.NotFound`, HTTP 400.
- A syntactically invalid ID → `Invalid<Type>.Malformed`, HTTP 400. Syntax is
  checked before existence, so a request naming both a malformed and an absent ID
  reports `Malformed`.
- One present plus one absent ID fails the whole call — EC2 does not return the
  partial set.
- An ID excluded by a `Filter` rather than by absence still counts as resolved:
  an existing ID plus a non-matching filter returns 200 and an empty set.
- No explicit IDs → every resource matches, and an empty account returns 200 and
  an empty set.

AWS's casing is inconsistent across these codes and SDK callers match the literal
string, so substrate mirrors each pair exactly:

| Resource | NotFound | Malformed |
|----------|----------|-----------|
| Instance | `InvalidInstanceID.NotFound` | `InvalidInstanceID.Malformed` |
| VPC | `InvalidVpcID.NotFound` | `InvalidVpcID.Malformed` |
| Subnet | `InvalidSubnetID.NotFound` | `InvalidSubnetID.Malformed` |
| Security group | `InvalidGroup.NotFound` | `InvalidGroupId.Malformed` |
| Internet gateway | `InvalidInternetGatewayID.NotFound` | `InvalidInternetGatewayId.Malformed` |
| Route table | `InvalidRouteTableID.NotFound` | `InvalidRouteTableId.Malformed` |
| Snapshot | `InvalidSnapshot.NotFound` | `InvalidSnapshotID.Malformed` |
| Volume | `InvalidVolume.NotFound` | `InvalidVolumeID.Malformed` |
| Image (AMI) | `InvalidAMIID.NotFound` | `InvalidAMIID.Malformed` |
| Elastic IP allocation | `InvalidAllocationID.NotFound` | — |
| NAT gateway | `InvalidNatGatewayID.NotFound` | `NatGatewayMalformed` |

AMIs are the one family whose absence code names no ID — `InvalidAMIID.NotFound` is
"The specified AMI doesn't exist" — the mirror image of the cross-naming snapshots
carry. Which AMIs an ID can resolve to is
[its own question](#which-amis-resolve); AWS's third AMI code,
`InvalidAMIID.Unavailable`, is raised nowhere, because substrate models no
deregistered-but-extant image.

EC2 publishes no `Malformed` variant for allocation IDs; a malformed allocation ID
surfaces as `InvalidAllocationID.NotFound`. NAT gateways are the one family whose
malformed code sits outside the `Invalid*ID.Malformed` naming entirely — the reference
publishes it as `NatGatewayMalformed`, "The specified NAT gateway ID is not formed
correctly. Ensure that you specify the NAT gateway ID in the form
nat-xxxxxxxxxxxxxxxxx."

AWS publishes two absence codes for NAT gateways — `NatGatewayNotFound` ("The
specified NAT gateway does not exist.") and `InvalidNatGatewayID.NotFound` — and no
per-operation page says which operation raises which; `DeleteNatGateway`'s `Errors`
section is empty, as EC2's pages generally are. Substrate answers
`InvalidNatGatewayID.NotFound` everywhere, because that is the spelling every other
row above follows and the one `DescribeNatGateways` has always published.

**Mutations answer from the same table.** Every operation that names a resource of one
of these families — a `Describe*` reading `<Type>Id.N`, or a mutation reading a single
`<Type>Id` — produces its code, message and status from the one entry, so the two
halves of the API cannot drift apart:

- An absent ID → the row's `NotFound`, with the message `The <noun> ID '<id>' does not
  exist`.
- A syntactically invalid ID → the row's `Malformed`, with the message
  `Invalid id: "<id>"`. This is checked before existence on a mutation exactly as it is
  on a describe, which matters because the two branches mean different things to a
  caller: `Invalid*.NotFound` can be retried after creating the resource, and
  `*.Malformed` never can.
- An **omitted** single ID parameter → `MissingParameter`, "The request must contain the
  parameter `<Name>`" — not a `Malformed` naming a parameter the caller never sent.
  `AttachVolume` names its two required IDs separately, so a caller who sent one of the
  two learns which is missing.

Four codes are outside the table and stay hand-written, because AWS gives their ID
families no prefix rule of the shape above: `InvalidRoute.NotFound`,
`InvalidAssociationID.NotFound`, `InvalidLaunchTemplateId.NotFound` and
`InvalidLaunchTemplateName.NotFoundException`.

One refusal reuses a code above for a different condition. A `RunInstances` naming a
security group that exists but lives in another VPC answers `InvalidGroup.NotFound`
with `does not belong to VPC …` — AWS's own gloss for that code is "the specified
security group does not exist", and from the target VPC's point of view it does not.
That is membership rather than absence, so it does not share the table's message.

**A refused mutation writes nothing.** An operation naming two resources resolves both
before its first write. `ReplaceRouteTableAssociation` is the one that had this wrong: it
removed the source association and committed it before resolving the target
`RouteTableId`, so a request naming an absent or malformed route table deleted the
association it was asked to move, left the subnet with no route table at all, and then
reported a failure — and a retry with the ID corrected answered
`InvalidAssociationID.NotFound` for the association the first call had eaten.

An ID is well formed when it has the resource's prefix followed by at least one
lowercase hex digit. Length is deliberately not checked: substrate's generators
emit 16 hex characters where AWS emits 8 or 17, and AWS itself still accepts the
legacy 8-character form for several resources.

### Which selectors assert existence

Every `Describe*` whose ID family has a row above resolves the IDs a caller names.
Twelve do: `DescribeInstances`, `DescribeInstanceStatus`, `DescribeVpcs`,
`DescribeSubnets`, `DescribeSecurityGroups`, `DescribeInternetGateways`,
`DescribeRouteTables`, `DescribeSnapshots`, `DescribeAddresses`,
`DescribeNatGateways`, `DescribeVolumes` and `DescribeImages`. The last two joined with
[#731](https://github.com/scttfrdmn/substrate/issues/731); before that each answered a
superset rather than an error, and
`DescribeImages` did not read `ImageId.N` **at all** — a caller naming one AMI was
answered with every AMI the account owned. A superset is the worse failure of the two,
because an error is visible and a superset reads as a successful narrowing.

`DescribeInstanceTypes`' `InstanceType.N` also asserts existence, answering
`InvalidInstanceType`. So does `DescribeLaunchTemplates`, as of
[#1469](https://github.com/scttfrdmn/substrate/issues/1469): an unknown `LaunchTemplateId.N`
answers `InvalidLaunchTemplateId.NotFound` and an unknown `LaunchTemplateName.N`
`InvalidLaunchTemplateName.NotFoundException`, both with real EC2's observed "At least one of
the launch templates specified in the request does not exist." The IDs are checked before the
names, each list in request order, and the check runs before `Filter.N`, so a named template
that exists and is filtered out is still an empty answer. It answered an empty set before,
which code treating the exception as "not created yet" could not tell from success.

**Eight selector families deliberately do not**, and answer an empty set where AWS
answers `NotFound`. Four have reasons that would not change if a kind were registered
for them:

| Selector | Why not |
|---|---|
| `DescribeFleets`' `FleetId.N` | AWS publishes **no** `InvalidFleetId.NotFound`. The only fleet-ID absence code in the reference is `InvalidSpotFleetRequestId.*`, which is a `sfr-` request, not a `fleet-` fleet. |
| `DescribeCapacityReservations`' `CapacityReservationId.N` | AWS **does** publish `InvalidCapacityReservationId.NotFound`, and `CancelCapacityReservation` answers it — but nothing says whether a *describe* raises it, since the operation's Errors section is the common-types boilerplate. Narrowing is what keeps a sweep over a list of IDs from failing because one reservation had already been cancelled and swept. A **malformed** ID is refused here, because that is a mistake in the request rather than an absent resource. |
| `DescribeRegions`' `RegionName.N` | The parameter explicitly permits naming any Region, enabled for the account or not, so "this Region is not in your answer" is not absence. |
| `DescribeSecurityGroups`' `GroupName.N` | The kind **is** registered and its `GroupId.N` asserts, but the code is `InvalidGroup.NotFound` and both AWS's client-error table and substrate's message for it describe a missing security group *ID*. This operation's own Errors section is empty, so a name-shaped refusal would be invented wording. |

Four more have a published code but no registered `ec2IDKind`, so the assertion is
unimplemented rather than declined: `DescribeKeyPairs`' `KeyName.N`/`KeyPairId.N`
(`InvalidKeyPair.NotFound`), `DescribePlacementGroups`' `GroupName.N`/`GroupId.N`
(`InvalidPlacementGroup.Unknown`), `DescribeAvailabilityZones`' `ZoneName.N`/`ZoneId.N`,
and `DescribeAddresses`' `PublicIp.N` (its `AllocationId.N` **does** assert).

`DescribeSecurityGroups`' `GroupName.N` selects as of
[#749](https://github.com/scttfrdmn/substrate/issues/749) — it was read by nothing before, so a
caller naming one group by name was answered about **every** group in the account — and it
unions with `GroupId.N` like the [other paired identity
parameters](#twelve-describes-gained-filters). A name is matched in the [default
VPC](#the-default-vpc) only, as AWS scopes the parameter ("[Default VPC] The names of the
security groups"), as of [#1491](https://github.com/scttfrdmn/substrate/issues/1491); before it, a
name matched account-wide and could select several groups. One thing about it is substrate's
reading:

- **A name matching nothing answers an empty set**, not `InvalidGroup.NotFound`. This
  operation's Errors section is empty, and EC2's client-error table describes that code as a
  missing security group *ID* — which is what substrate's own message for it says — so refusing
  here would mean inventing wording AWS does not publish for this operation. The ID half keeps
  its full contract either way: a malformed `GroupId.N` is refused before the walk and an
  unresolved one after it, whether or not a name selected something.

`DescribeImages` reads `Owner.N` as of #1432 — `self` and the caller's account ID select its
own images, `amazon` the bundled catalog, Canonical's account its Ubuntu entries, anything else
nothing — and `ExecutableBy.N` as of #1494; see [Which AMIs resolve](#which-amis-resolve).

Both ID lists are read in every form AWS accepts — `VolumeId.1`, `VolumeId.2`, … and the
un-indexed `VolumeId` — and an explicitly empty value is an ID the caller sent, so it is
reported as `Malformed` rather than truncating the list.

## Finding a fleet's instances

Every instance `CreateFleet` launches is tagged `aws:ec2:fleet-id` with the fleet
that created it, so the fleet's instances are reachable with an ordinary
`DescribeInstances` tag filter:

```bash
aws ec2 describe-instances \
  --filters "Name=tag:aws:ec2:fleet-id,Values=fleet-12a34b56-7890-1cde-2f34-abcdef567890"
```

For an `instant` fleet this is the only route from a fleet back to its live
instances. `DescribeFleetInstances` rejects instant fleets outright, and the
`fleetInstanceSet` in a `CreateFleet`/`DescribeFleets` response is a record of what
was launched — it never drops instances that have since terminated. Without the tag
a fully-running fleet is indistinguishable from an empty one.

This tag is modelled from observed behaviour on real AWS rather than from a
documented API contract: it appears in neither the EC2 API reference nor the fleet
tagging and describe pages. It is applied to every fleet type, and — unlike a
caller's own `TagSpecification` entries — it is not scoped by `ResourceType`.

A caller cannot delete this tag — see
[reserved tag keys](#reserved-tag-keys) — which matches the rule AWS attaches to the
`aws:` prefix.

## Reserved tag keys

Every path that assigns a tag rejects any key beginning with `aws:`, the prefix EC2
reserves for its own use — `CreateTags`, `DeleteTags`, and tag-on-create through
`RunInstances`, `CreateFleet`, `CreateImage` and `CreateNatGateway`:

```
InvalidParameterValue: Tag keys starting with 'aws:' are reserved for internal use
```

The status is `400`. The whole request is refused before any resource is modified,
so a request that mixes a legal tag with a reserved one leaves every resource it
named untouched — `CreateTags` accepts up to 1000 resource IDs, and a partial
application is a state real EC2 never produces.

The match is **case-sensitive**. AWS documents tag keys and values as
case-sensitive, so `AWS:foo` and `Aws:foo` are ordinary user tags and are accepted;
only the lowercase `aws:` prefix is reserved.

On a tag-on-create path the rejection happens **before the resource is created**, so
a refused `RunInstances` launches no instance, a refused `CreateImage` leaves behind
neither the AMI nor its backing snapshot, and a refused `CreateNatGateway` creates no
gateway. This follows the tagging documentation directly: "If tags cannot be applied
during resource creation, we roll back the resource creation process. This ensures
that resources are either created with tags or not created at all."

Provenance: the `CreateTags` API reference has an empty Errors section, so neither
the code nor the message above is derivable from the API model. Both come from
observed real-AWS responses — and both captures are in fact of `RunInstances`
tag-on-create, so that path has the strongest claim to this wording. The
`DeleteTags` rejection is a step weaker — substrate found no captured `DeleteTags`
error and inherits the wording from the same capture. What the tagging documentation
does state plainly is the outcome: such a tag "can't be edited or deleted" by a
caller.

### How substrate's own fleet tag is exempt

Substrate stamps [`aws:ec2:fleet-id`](#finding-a-fleet-s-instances) on every fleet
instance, which is a reserved key on a tag-on-create path — the reason this check
was previously left off that path entirely.

It is exempt structurally rather than by a flag. `CreateFleet` parses the caller's
`TagSpecification.N` tags and checks them exactly as `RunInstances` does; the
fleet-ID tag is appended to the resulting value *after* that check, on an internal
launch entry point that takes already-parsed tags. There is no param a request could
set to reach it. A validation-skipping flag would have made the outcome depend on
internal state a consumer cannot observe, which is the opposite of the
deterministic-replay property substrate exists for.

So a caller naming `aws:` anything in a `CreateFleet` request is rejected —
instance- and fleet-scoped alike — while the fleet's own stamp is still applied, and
both coexist with the caller's legal tags on the same instance.

One limit of the current scope, stated rather than implied: only tags scoped to a
resource substrate tags are checked. A `TagSpecification` naming `network-interface`
or `spot-instances-request` on `RunInstances`, or inside a launch template's
`LaunchTemplateData`, is skipped, because substrate does not tag those resources at
all; real EC2 would reject a reserved key there too. The `volume` scope used to be
skipped for the same reason and no longer is: now that a launch tags its volumes, a
reserved key there refuses the launch, and the refusal happens before the launch loop
so no instance is left behind by it.

A launch template's own tags *are* checked, on both modelled scopes, at
`CreateLaunchTemplate` and `CreateLaunchTemplateVersion` as well as at every launch
that names the template — so a template cannot serve as an unchecked second path to a
reserved key. Each scope is counted against the 50-tag limit on its own, because the
limit is per resource and an instance and its volumes are different resources. See
[A launch template merges with the request, field by field](#a-launch-template-merges-with-the-request-field-by-field).

## The 50-tag-per-resource limit

A resource carrying more than 50 user tags is refused, on `CreateTags` and on every
tag-on-create path:

```
TagLimitExceeded: The maximum number of Tags for a resource has been reached.
```

The status is `400`. From the tagging documentation's restrictions: "Maximum number of
tags per resource – 50".

Two rules make this less arithmetic than it looks, and both are modelled.

**Tags with the `aws:` prefix do not count.** The documentation says so directly:
"Tags with the `aws:` prefix do not count against your tags per resource limit." This
is load-bearing rather than pedantry, because substrate stamps
[`aws:ec2:fleet-id`](#finding-a-fleet-s-instances) on every fleet instance — a counter
that included reserved keys would refuse a fleet launch whose template names the full
50 user tags, which real EC2 accepts. A fleet instance therefore holds 51 tags legally:
50 of the caller's and one of substrate's.

**Overwriting an existing key at the limit succeeds.** The count is over the
*post-merge key set*, so a key already on the resource adds nothing. `CreateTags` on a
50-tag instance changing the value of `key7` is accepted and the value changes; adding
a new `key51` to the same instance is refused. Written as
`len(existing) + len(incoming)` both would fail, and real AWS permits the first —
[getmoto/moto#8151](https://github.com/getmoto/moto/issues/8151) reports exactly that
case.

As with reserved keys, the whole request is refused before anything is modified.
`CreateTags` naming two instances — one with room, one at the limit — tags neither.
A resource ID that names nothing is not counted against, because the apply step ignores
it: checking it would refuse a request real EC2 accepts as a no-op.

Provenance is split, and the weaker half is the message. The **code** is documented:
EC2's client-error table lists `TagLimitExceeded` as "You've reached the limit on the
number of tags that you can assign to the specified resource." The wire **message** is
published nowhere, so the wording above is [moto](https://github.com/getmoto/moto)'s,
from a reimplementation rather than a captured response. That is a weaker claim than the
code's, and is a distinction worth stating: SDKs dispatch on `Error.Code`, so the code
is the part a consumer's error branch turns on.

A launch template's instance-scoped tags are counted the same way, at
`CreateLaunchTemplate` and `CreateLaunchTemplateVersion` as well as at every launch
that names the template. Exactly 50 template tags launch; 51 are refused at template
creation.

The third restriction from the same table — the key and value **length** limits — is
enforced too, with one deliberate difference in how reserved keys are treated. See
[Tag key and value length limits](#tag-key-and-value-length-limits).

## Tag key and value length limits

A tag key longer than **128 characters** or a value longer than **256** is refused, on
`CreateTags`, `DeleteTags` and every tag-on-create path:

```
InvalidParameterValue: Tag key must be no more than 128 Unicode characters in UTF-8; the supplied key is 129
```

The status is `400`. The message names which of the two limits was exceeded and by how
much, because that is the only place a caller learns it. From the same restrictions list
that gives the 50-tag count: "Maximum key length – 128 Unicode characters in UTF-8" and
"Maximum value length – 256 Unicode characters in UTF-8".

**The unit is Unicode characters, not bytes.** A key of 128 emoji is 128 characters and
512 bytes, and it is legal; a byte-counting check would refuse it, and refuse it while
reporting a length the caller never sent. The two counts agree on ASCII, so a suite that
only tests ASCII keys cannot tell them apart.

There is **no lower bound**. The documentation states that "You can set the value of a
tag to an empty string, but you can't set the value of a tag to null", so an empty value
is legal and the check is an upper bound only. That is also what makes `DeleteTags` work
unremarkably: it names keys and treats the value as optional, so a request with no
`Tag.N.Value` supplies the empty string and passes. A key is required by the query
encoding rather than by this check — the tag walk ends on an absent or empty
`Tag.N.Key` — so an empty key is not expressible in the first place.

As with the other two tag restrictions, the whole request is refused before anything is
modified, and on a tag-on-create path before the resource is created: a refused
`RunInstances` launches no instance, a refused `CreateImage` creates neither the AMI nor
its snapshot, a refused `CreateNatGateway` creates no gateway, and a refused
`CreateLaunchTemplate` creates no template. A launch template is checked at
`CreateLaunchTemplate` and `CreateLaunchTemplateVersion` as well as at every launch that
names it, so a consumer hears about an over-long template tag once, at the operation that
named it.

**Reserved keys are not exempt from the lengths, though they are from the count.** The
exemption in the restrictions list is scoped to the count alone — "Tags with the `aws:`
prefix do not count against your tags per resource limit" — and nothing in it exempts a
reserved key from either length, so substrate checks them. In practice that decides no
observation: the reserved-key check runs first, so a caller's `aws:`-prefixed key is
refused for being reserved before its length is measured. It matters for the code rather
than the wire, and it is recorded here because the adjacent count check does the opposite.
Where the length check does bite is the case-sensitive edge: `AWS:` is an ordinary user
tag, so an over-long `AWS:`-prefixed key is refused for its length.

Provenance is the weakest of the three tag restrictions, and is marked as such
deliberately. The **code** `InvalidParameterValue` with `400` is by analogy with the
reserved-key rejection — the other tag-restriction violation on the same operations, and
`CreateTags`' Errors section is empty so the API model supplies nothing. The **message
text is substrate's own**: no captured real-AWS length rejection was found, in
[moto](https://github.com/getmoto/moto) or LocalStack either. It follows the
`SendMessage` size-limit message's precedent of interpolating the limit and the actual
value. A consumer's error branch should dispatch on the code, which is the part that
rests on something.

### Tag scoping on `CreateImage`

`CreateImage` accepts two tag scopes, and substrate now honours the distinction:
`ResourceType=image` tags the AMI, and `ResourceType=snapshot` tags the backing
snapshot substrate materializes for the AMI's root device. Per the reference, "the
same tag is applied to all of the snapshots that are created."

This is a behaviour change. `CreateImage` previously read `TagSpecification.1`'s tags
whatever they were scoped to, so a request that tagged only its snapshots put those
tags on the AMI instead. A caller asserting on `DescribeImages` tags that were
actually snapshot-scoped will now see them on `DescribeSnapshots`, which is where
real EC2 puts them.

### `RegisterImage` records the whole block device mapping

`RegisterImage` read exactly one thing out of the mapping it was sent — the first
`BlockDeviceMapping.N.Ebs.SnapshotId`, found by a hand walk of indexes 1 to 32 — and
discarded the rest of every entry: device names, sizes, volume types, and every mapping
after the first that named a snapshot. AWS's own third example for this operation registers
three volumes (two snapshots and an empty 100 GiB volume), so a caller sending AWS's
documented request got one volume back, on a `/dev/sda1` the request need never have
mentioned.

| Request | Answer |
|---|---|
| Every `BlockDeviceMapping.N` entry | Stored as sent and rendered back by `DescribeImages` in request order |
| A mapping naming a snapshot that is malformed or names nothing | `InvalidSnapshotID.Malformed` / `InvalidSnapshot.NotFound`, before anything is written |
| `Ebs.VolumeSize` below the snapshot's size | `InvalidBlockDeviceMapping`, naming the device |
| A mapping with a size and **no** snapshot | Registered — AWS's third example volume is exactly that |
| A mapping absent a size but naming a snapshot | The snapshot's size, per `EbsBlockDevice.VolumeSize`: "If you specify a snapshot, the default is the snapshot size" |
| `RootDeviceName` | Read, to decide which mapping is the root device's; absent, the first mapping naming a snapshot is |
| `TagSpecification.N` with `ResourceType=image` | Tags the AMI, through the same walk and the same [tag rules](#reserved-tag-keys) as every other tag-on-create |
| `TagSpecification.N` with any other `ResourceType` | `InvalidParameterValue`. AWS: "If you specify another value for `ResourceType`, the request fails" |
| `Name` | Still the only required parameter, as AWS marks it |

The mapping is now read by the same parser `RunInstances` and a launch template use, which
brings two changes a caller can notice beyond getting its volumes back. The walk is
**unbounded**, so a request with more than 32 mappings no longer has the remainder dropped;
and it **stops at the first absent index** instead of tolerating a gap. AWS's query protocol
indexes contiguously and every other indexed walk in substrate assumes it, so a sparse
request is not one an SDK produces — but a hand-built request that skipped an index used to
have its later mappings read, and no longer does.

Three mapping shapes render differently, because they are different things: an EBS volume
gets an `ebs` element, an instance store device gets a `virtualName` and **no** `ebs`, and a
suppressed device gets a `noDevice` element whose value is empty. Giving every mapping an
`ebs` element would report a phantom 0 GiB volume for the latter two.

`block-device-mapping.snapshot-id` widened with this: it matches on **every** snapshot the
AMI's mapping names, not only the root device's. Consulting the root alone meant a filter
naming a volume `DescribeImages` had just rendered found no AMI — two operations
contradicting each other about one record.

Two things this does **not** do. It applies only the snapshot rule from the shared mapping
validator, not the rest of it (duplicate device names, `virtualName` spellings, gp3-only
`Throughput`): none of those is a rule AWS states for this operation, and arriving on a
published path unannounced is how a consumer's working request starts failing. And
`Name`'s documented character constraints ("3-128 alphanumeric characters, parentheses…")
stay unenforced — only an empty `Name` is refused.

Provenance: `API_RegisterImage.html`'s Errors section is empty, so no code here is quoted
from the operation's own page. The snapshot codes come from EC2's client-error table and
predate this change. The `ResourceType` code is **substrate's reading** — AWS says only that
"the request fails" — chosen because `InvalidParameterValue` is EC2's gloss for "A value
specified in a parameter is not valid, is unsupported, or cannot be used".

### An AMI reports its architecture, platform and root device

`DescribeImages` rendered six members — `imageId`, `name`, `description`, `imageState`,
`imageOwnerId` and `creationDate` — and nothing about the image itself. So a caller that
branched on architecture to pick an instance type, or on the platform to decide whether to
send PowerShell or bash user data, or on the root device name to build a block device
mapping, read an empty string out of an AMI substrate knew the answer for. Twelve members
now render ([#750](https://github.com/scttfrdmn/substrate/issues/750)):

| Member | Bundled AMI | Caller's AMI |
|---|---|---|
| `architecture` | from the catalog — `x86_64` or `arm64` | `RegisterImage`'s `Architecture`, or inherited by `CreateImage` |
| `platform` | `windows` on the Windows entry, **omitted** on the eight Linux ones | omitted; `RegisterImage` takes no such parameter |
| `platformDetails`, `usageOperation` | `Linux/UNIX`+`RunInstances`, or `Windows`+`RunInstances:0002` | omitted, unless inherited by `CreateImage` |
| `rootDeviceName` | `/dev/xvda` on the Amazon Linux and ECS entries, `/dev/sda1` on Windows and Ubuntu | `RegisterImage`'s `RootDeviceName`, or inherited |
| `rootDeviceType` | `ebs` | `instance-store` when `ImageLocation` was sent, otherwise `ebs` |
| `virtualizationType` | `hvm` | `RegisterImage`'s `VirtualizationType`, or inherited |
| `hypervisor` | `xen` | `xen` |
| `imageType` | `machine` | `machine` |
| `imageOwnerAlias` | `amazon` on the nine AWS-published entries, **omitted** on the four Canonical ones | omitted |
| `imageOwnerId` | `099720109477`, Canonical's account, on the four Canonical entries; **omitted** on the nine AWS-published ones | the caller's account |
| `isPublic` | `true` | `false` |
| `publicSsmParameterName` | the parameter the AMI was discovered through | omitted |

Four of those cells are the interesting ones, because they are where a default written for
the bundled catalog's benefit would have been wrong for a registered image — both passes
render through one closure, so a value written once attaches to both:

- **`imageOwnerAlias` is not on the Ubuntu entries.** The alias is "an Amazon-maintained
  list", and Canonical is not on it.
- **`publicSsmParameterName` is not on a caller's AMI.** No public parameter names it, and
  the member exists precisely to let a caller round-trip the parameter it resolved an AMI
  through — which is now assertable in both directions, since substrate's SSM and EC2
  plugins answer from the same catalog key.
- **`platformDetails` and `usageOperation` are absent on a registered AMI.** Both are billing
  facts AWS derives from the image's product codes, which substrate does not model, so
  reporting `Linux/UNIX` for an AMI a caller registered would be a guess about what it
  contains. `CreateImage` inherits them, because an image of a running instance runs the same
  operating system its parent AMI does.
- **`imageOwnerId` is now omitted rather than rendered empty** for a bundled AMI. The member
  had no `omitempty`, so a named bundled image answered with an empty `<imageOwnerId>` — a
  member present and blank, which an SDK reads as "the owner is the empty string" rather than
  as "there is no owner". Amazon's AMI-owning account varies by Region and AWS publishes no
  stable mapping, so the alias is rendered and the account is left out. Canonical's is published,
  so the Ubuntu entries render it (#1494).

`RegisterImage`'s defaults are **AWS's documented ones**: `Architecture` defaults to `i386`
("Default: For Amazon EBS-backed AMIs, i386") and `VirtualizationType` to `paravirtual`. No
AMI a caller would register today is either, and both will look wrong. They are used anyway,
because inventing `x86_64`/`hvm` would mean a request that omits the parameter reports one
thing here and another on AWS — a divergence in the direction that makes a passing test
meaningless. Neither parameter's value is validated against AWS's enum, following the same
reasoning the mapping rules do: a new refusal on a published path arrives unannounced.

Each entry's root device name is **substrate's reading**, not AWS's published text. AWS's
device naming reference says only "Differs by AMI — `/dev/sda1` or `/dev/xvda`" and publishes
no per-AMI table, so the split above follows each publisher's own convention: Amazon Linux
and the ECS-optimized AMIs use `/dev/xvda`, Windows and Canonical's Ubuntu images use
`/dev/sda1`. The launch path still accepts either spelling as a root device, as it did before.

Two places where AWS contradicts itself, and the side substrate took:

- **`platform`'s casing.** The `Image` type page lists the valid value as `Windows` and in
  the same entry says the member "is set to `windows` for Windows AMIs"; `DescribeImages`'
  second example response renders `<platform>windows</platform>` and its `platform` filter
  says "The only supported value is `windows`". Substrate renders lowercase — three
  statements against one.
- **`imageOwnerId`/`isPublic` against `ownerId`/`public`.** `DescribeImages`' first example
  uses the short spellings; its second and third examples and the `Image` type page use the
  long ones. Substrate follows the type page, which is the spelling it already used for the
  owner.

`CreateImage` inherits `architecture`, `platform`, `platformDetails`, `usageOperation`,
`virtualizationType` and `rootDeviceName` from the AMI its source instance runs, rather than
leaving them empty: an AMI made from an arm64 Amazon Linux instance runs arm64 Amazon Linux,
and reporting nothing while its parent reported `arm64` would be two of substrate's own
answers disagreeing about one lineage. What it does **not** inherit is ownership —
`imageOwnerAlias`, `publicSsmParameterName` and `isPublic` are the new AMI's own, and it is
the caller's private image whatever it was made from. The fabricated root mapping follows the
inherited device name, so an AMI's `rootDeviceName` and its `blockDeviceMapping` cannot
disagree.

One thing this makes newly visible rather than fixes: a bundled AMI now names a
`rootDeviceName` while its `blockDeviceMapping` stays empty. Bundled images live outside
state, so no snapshot backs one, and fabricating a mapping would mean rendering an `ebs`
element naming a snapshot ID that resolves to nothing — a worse answer than none. A caller
that needs a mapping to read should register or image its own AMI.

### `DescribeImages` filters

Eighteen filter names are applied: `image-id`, `block-device-mapping.snapshot-id`,
`tag:<key>`, `tag-key`, and — with the members above, because a rendered member a caller
cannot filter on is half an answer — `architecture`, `description`, `hypervisor`,
`image-type`, `is-public`, `name`, `owner-alias`, `owner-id`, `platform`,
`public-ssm-parameter-name`, `root-device-name`, `root-device-type`, `state` and
`virtualization-type`. Fourteen of those were previously *accepted and inert*, which is worse
than the alternatives in both directions: a filter on `architecture` returned every AMI in
the account, and a caller reads a non-empty answer as a match.

AWS documents twenty-five more, which need state substrate
does not keep: those are accepted and inert, and anything outside AWS's list is refused —
see [One rule for an unrecognized filter name](#one-rule-for-an-unrecognized-filter-name).
The remainder are the `product-code*`, `image-watermark.*`, `source-image*` and
`state-reason-*` families, the `block-device-mapping.*` names other than `snapshot-id`, the
ENA/sriov and kernel/ramdisk names, `manifest-location`, the Allowed-AMIs and Free-Tier
markers, and `creation-date`, whose documented rule is an ISO-8601 prefix match with
wildcards rather than the equality the others use.

`owner-id` compares against the account that owns the AMI, so it matches nothing for an
AWS-published bundled image — which reports no owner — exactly as `Owners=self` does not, and
Canonical's account for the Ubuntu entries. `is-public` is
compared as the rendered `true`/`false`, so `is-public=false` selects the caller's own AMIs
and excludes every bundled one.

Two behaviour changes came with `tag-key`, both of which bring this operation into line
with `DescribeInstances` and `DescribeVolumes` rather than leaving it with its own rules:

- A **`tag:<key>` filter with no value now matches nothing**, where it previously matched
  any value. That any-value question is what `tag-key` spells, which is why the filter had
  to arrive in the same change — otherwise the correction would have removed the only way
  to ask it. AWS settles neither shape: its filtering guide says only that a filter value
  cannot be null, so matching nothing is substrate's reading, and it is now the same
  reading everywhere.
- An **explicitly empty filter value is a value**. `Filter.1.Value.1=` asks for an image
  whose tag is the empty string; it previously arrived indistinguishable from naming no
  values at all, because this operation parsed `Filter.N` itself and stopped at the first
  empty string.

Repeated filter names changed for **every** EC2 describe, not only this one: two
`Filter.N` entries sharing a name now **OR their values** instead of the second silently
replacing the first. AWS documents that filters are ANDed and the values within a filter
ORed, and says nothing about a name appearing twice, so the merge is substrate's reading.
What it replaces was not a reading of anything — the earlier entry was discarded with no
indication. Differently-named filters are ANDed, as documented, and always were.

### A subnet reports its tags, and filters on them

`EC2Subnet` has carried a `Tags` field since it existed, and `CreateTags` on a `subnet-` ID
has always written to it. Only the reader was missing: `DescribeSubnets` rendered six
members and no `tagSet`, and it parsed **no `Filter.N` at all** — so a request for "the
subnets of `vpc-x`" answered with every subnet in the region, with nothing in the response
to say the filter had not been applied. That is the worst shape of an ignored filter: a
consumer walking a VPC's subnets silently got its neighbours' as well.

Three things changed together, because each is useless without the others:

- **`CreateSubnet` honours `TagSpecification.N`** scoped to `subnet`, under the same two
  rules every other tag path enforces — the reserved `aws:` prefix and the 50-tag limit —
  and a refused request creates no subnet. This is how CDK and Terraform set the tags a
  caller then filters on, so without it the filters below had nothing to find.
- **`CreateSubnet` and `DescribeSubnets` render one `Subnet` element**, which is what AWS
  documents. The two previously rendered different subsets of it: `CreateSubnet` omitted
  `mapPublicIpOnLaunch`, so a caller reading the create response saw a different subnet from
  the one it could then describe. A regression test asserts the two are equal.
- **The element gained `tagSet`, `subnetArn`, `ownerId` and `defaultForAz`.** The ARN and
  the owner are what the `subnet-arn` and `owner-id` filters compare against, so rendering
  them is what lets a caller round-trip a value it read.

`tagSet` is **absent** on an untagged subnet rather than present-and-empty, which is what
both of AWS's `DescribeSubnets` samples show. That deliberately differs from
`DescribeSnapshots`, whose own page shows the empty element — each follows its own
operation's samples rather than one house rule. No sample on either page shows a *tagged*
subnet, so `tagSet`'s position last in the element is substrate's choice.

Five members AWS's samples carry stay absent: `availableIpAddressCount`,
`availabilityZoneId`, `assignIpv6AddressOnCreation`, `ipv6CidrBlockAssociationSet` and the
`blockPublicAccessStates`/`privateDnsNameOptionsOnLaunch` structures. Nothing in state backs
them, and deriving an address count from the CIDR would be fabrication — the real count
depends on how many addresses AWS reserves and on every interface in the subnet.

Eleven filter names are applied, plus the four alias spellings the reference documents
inline ("You can also use `availabilityZone` as the filter name"); the aliases are separate
names on the wire, so each is answered beside its canonical form rather than normalized
away. The fourteen names AWS documents that substrate keeps no state for are accepted and
inert, and anything outside the twenty-five is refused —
[the same rule as everywhere else](#one-rule-for-an-unrecognized-filter-name), which lists
both sets.

`cidr-block` is **exact**, per AWS: "The CIDR block you specify must exactly match the
subnet's CIDR block for information to be returned for the subnet." A caller asking for
`10.0.0.0/16` does not get `10.0.1.0/24`.

### A snapshot filters on its own members, and scopes by account

`DescribeSnapshots` evaluated exactly one filter — `snapshot-id` — and silently dropped the
other thirteen, so `status=pending` or `tag:Env=prod` returned every snapshot in the
account. Eleven are now applied: `description`, `encrypted`, `owner-id`, `progress`,
`snapshot-id`, `start-time`, `status`, `tag-key`, `tag:<key>`, `volume-id` and `volume-size`.
The remaining three — `owner-alias`, `storage-tier` and `transfer-type` — name members
substrate does not render, so there is nothing to compare a value against; they are
accepted and inert.

`progress` was the fourth inert name until substrate had a progress to render (see
[Seeding a snapshot progression](#seeding-a-snapshot-progression)). It compares against the
percent-suffixed string the response carries, which is the form AWS's own filter text shows
("for example, `80%`"): a caller filters on `100%`, not on `100`.

**`status` and `progress` compare against what *this* request reports, not against the stored
record.** The two differ only under a seeded progression, and that is the one case a poll loop
cares about — filtering on the record would make `status=pending` select nothing at the very
moment the caller is being told the snapshot is pending. It also means the CLI's own
`aws ec2 wait snapshot-completed`, which polls
`--filters Name=status,Values=completed`, terminates: an observation is taken before the
filters run, so a snapshot the filter excludes has still advanced its countdown.

**`Owner.N` and `RestorableBy.N`** were read by neither the ID selection nor the filters,
and are now honored. Both sit outside `Filter.N` and both accept `self`, an account ID, or
one of AWS's aliases. Substrate is single-account, so `self` and the requesting account's ID
match everything, and **anything else — including `amazon` — matches nothing**: answering
"snapshots owned by `amazon`" with the account's own snapshots would claim they were public.
Values within either parameter OR, and the two AND with the filters and with the ID list.

A snapshot a *filter* excluded still counts as resolved, so naming an existing snapshot and
filtering it out is an empty HTTP 200 rather than `InvalidSnapshot.NotFound` — the rule
[Explicit resource IDs](#explicit-resource-ids) states, and the distinction between "not
yet" and "never" that a consumer's wait loop turns on. `DescribeSubnets` follows it too.

### A snapshot has a real size

Until [#689](https://github.com/scttfrdmn/substrate/issues/689) every snapshot in substrate
reported `volumeSize` as the literal `8`,
because `CreateImage` was the only thing that wrote one and it wrote the constant. So the
`volume-size` filter above compared against a constant, the mapping rule "a size must not be
smaller than its snapshot's" had nothing to test, and `DescribeImages` rendered a *second*,
independent `8` for the same snapshot — two constants that agreed only for as long as neither
knew a real size.

`CreateSnapshot` closes it at the source:

| Behaviour | Answer |
|---|---|
| `VolumeId` | Required, and checked against state. Absent is `MissingParameter`; a syntactically invalid ID is `InvalidVolumeID.Malformed`; one that names nothing is `InvalidVolume.NotFound` — so a snapshot cannot exist for a volume that does not |
| `volumeSize` | The source volume's size, which is what AWS documents the member as: "the size of the volume, in GiB" |
| `encrypted` | The source volume's. "Snapshots that are taken from encrypted volumes are automatically encrypted" |
| `status` | `completed` at once by default, **not** the `pending` AWS's own sample response shows — unless a [progression is seeded](#seeding-a-snapshot-progression), in which case the snapshot is born in the seeded state |
| `progress` | Rendered, since AWS documents it as a member of the `Snapshot` that `CreateSnapshot` returns. `100%` for an unseeded snapshot; the ramp's current position under a seed |
| `statusMessage` | Not rendered, because AWS scopes it: "this parameter is only returned by `DescribeSnapshots`" — where an SDK reads the same element as `StateMessage`, the third of these name splits after `status`/`State` and `state`/`State` |
| `Description`, `TagSpecification.N` | Read; the tags go through the same walk and the same [tag rules](#reserved-tag-keys) as every other tag-on-create, checked before anything is written |
| `Location`, `OutpostArn` | Not read. Both are documented as supported only for a Local Zone or an Outpost, neither of which substrate models, so honoring them would place the snapshot somewhere substrate cannot describe it from |

`status` being `completed` immediately is the deliberate divergence. Substrate advances no
snapshot asynchronously, so a caller's waiter succeeds on its first poll rather than
depending on wall-clock time — which is the point of a deterministic emulator. The waiting
path is exercised instead by seeding it: see
[Seeding a snapshot progression](#seeding-a-snapshot-progression).

`CreateImage`'s own snapshot now reads the instance's root volume too, recording both its
size and its ID — so an AMI made from a 40 GiB root volume reports 40 GiB through
`DescribeSnapshots` *and* through `DescribeImages`, which reads the snapshot record rather
than rendering its own constant. An AMI whose snapshot is missing falls back to the 8 GiB
default rather than reporting `0`, since a caller sizing a volume off that member can act on
the default. Two things reach that state, and `RegisterImage` naming a snapshot that does not
exist is no longer one of them — that request is refused, see
[`RegisterImage` records the whole block device mapping](#registerimage-records-the-whole-block-device-mapping).
What remains is deleting the snapshot a **non-root** mapping names, which `DeleteSnapshot`
permits because AWS scopes its refusal to the root device (below), and an AMI record written
directly into state.

### The rest of the snapshot family

`CreateSnapshots`, `CopySnapshot` and the three attribute operations answered `InvalidAction`
until [#709](https://github.com/scttfrdmn/substrate/issues/709) — the answer that tells a
caller the endpoint does not speak EC2, rather than that substrate has a gap. All five are
implemented. None of the five publishes an operation-specific error: every Errors section
points only at the common types, and `CreateSnapshots` has no Examples section at all. So each
refusal below is either a code from EC2's client-error table (which describes a condition
rather than quoting a wire message) or substrate's reading of a rule AWS states in prose; the
table says which.

**`CreateSnapshots`** snapshots every EBS volume attached to one instance in a single call.

| Behaviour | Answer |
|---|---|
| `InstanceSpecification.InstanceId` | Required; absent is `MissingParameter`. Resolved against state, so an absent instance is `InvalidInstanceID.NotFound` rather than an empty `snapshotSet` — an empty set is the true answer for an instance with nothing left to snapshot, and a consumer's wait loop turns on the difference |
| Order | `snapshotSet` is ordered by device name. State keys come back in map order, so anything else would answer differently on each run and `snapshotSet[0]` would be a coin toss |
| `ExcludeBootVolume` | Drops the volume attached at a root device name |
| `ExcludeDataVolumeId.N` | Singular member, indexed — not a plural. Up to 40 per request, per AWS; a 41st is `InvalidParameterValue`. An ID that is not attached excludes nothing, which is what the parameter asks for |
| The root volume named in `ExcludeDataVolumeId.N` | Refused, naming `ExcludeBootVolume`. AWS: "If you specify the ID of the root volume, the request fails. To exclude the root volume, use `ExcludeBootVolume`." The code is substrate's; AWS says only that the request fails |
| `CopyTagsFromSource` | Valid value `volume`, and any other value is refused rather than treated as "do not copy". Each snapshot inherits **its own** source volume's tags |
| `Description`, `TagSpecification.N` | Applied to every snapshot the call creates. Where a request tag collides with a copied volume tag, the request wins — AWS settles neither the precedence nor the ordering, so both are substrate's |
| `state` | `completed` at once, for the reason `CreateSnapshot`'s `status` is — and seedable the same way. A `"*"` seed governs every snapshot in the set, each with its own countdown, so snapshotting five volumes under a two-poll seed leaves each of the five with its own two polls |
| `progress` | Rendered, as `CreateSnapshot`'s is |
| `Location`, `OutpostArn` | Not read, as on `CreateSnapshot` |

The response element is `snapshotSet` of `SnapshotInfo`, which names the state member
**`state`** where `CreateSnapshot` and `DescribeSnapshots` both name the same thing `status`.
A caller unmarshalling `SnapshotInfo` reads `State`. `availabilityZone`, `outpostArn` and
`sseType` are not rendered: `availabilityZone` is the Local-Zone placement member — the
singular `CreateSnapshot` response has no such member — so rendering the volume's AZ would
claim a local snapshot, and neither Outposts nor a specific server-side encryption type is
modelled. `SnapshotInfo` has no `statusMessage` member at all; AWS publishes that one on
`Snapshot` alone.

A refused request writes nothing: every snapshot is built and its tags checked before the
first write, so a five-volume instance whose fourth volume carries a reserved tag key does not
leave three snapshots behind.

**`CopySnapshot`** copies a snapshot within the region, which AWS's own text makes coherent
for a single-region emulator twice over. "If the source snapshot is in a Region, you can copy
it within that Region" makes a same-region copy legal rather than degenerate, and AWS's Example
1 is one. And `DestinationRegion` is not routing — "this parameter is only valid for specifying
the destination Region in a `PresignedUrl` parameter … the snapshot copy is sent to the
regional endpoint that you sent the HTTP request to" — so the destination is the endpoint, and
the endpoint is the one region substrate serves.

| Behaviour | Answer |
|---|---|
| `SourceRegion`, `SourceSnapshotId` | Both required; absent is `MissingParameter`. A malformed source is `InvalidSnapshotID.Malformed`, one naming nothing `InvalidSnapshot.NotFound` |
| A `SourceRegion` that is not the request's region | `SnapshotCopyUnsupported.InterRegion`, whose published description — "inter-region snapshot copy is not supported for this AWS Region" — is precisely substrate's situation. The code is AWS's; the message is substrate's |
| `Encrypted` | Accepted only as `true`. A copy of an encrypted snapshot is encrypted whatever the request says; `Encrypted=false` is refused rather than silently honored or silently dropped |
| `KmsKeyId` | Validated, not stored: "if `KmsKeyId` is specified, the encrypted state must be true", so naming a key for a copy that will not be encrypted is refused. Substrate models no KMS key on a snapshot, so the rule is observable only as that refusal |
| `Description` | The request's, empty when absent. AWS documents nothing for an omitted one, so substrate invents no "Copied from …" string a consumer might assert on |
| `TagSpecification.N` | The only source of the copy's tags. `CopySnapshot` has no `CopyTagsFromSource` — that is `CreateSnapshots`' parameter — so the source's tags are deliberately not carried over |
| `volumeId` | A freshly generated ID that names no volume, which is what AWS produces: "snapshots copies have an arbitrary source volume ID. Do not use this volume ID for any purpose." Rendering the source's would hand a caller a reference that resolves, inviting exactly that use |
| `CompletionDurationMinutes`, `DestinationAvailabilityZone`, `DestinationOutpostArn`, `DestinationRegion`, `PresignedUrl` | Not read. A time-based copy has nothing to slow down here, the next two place the copy somewhere substrate does not model, and the last two are artifacts of signing a cross-region request |

The response carries `snapshotId` and `tagSet` and nothing else — no status, no size — so a
caller polls `DescribeSnapshots` for the rest.

**The attribute trio** — `DescribeSnapshotAttribute`, `ModifySnapshotAttribute` and
`ResetSnapshotAttribute` — follows
[Instance attributes](#instance-attributes) in four respects:
the attribute name is validated *before* the snapshot is resolved (a bad name is a defect no
retry fixes; an absent snapshot may be one the caller is still waiting on), exactly one
attribute element marshals, a present-but-empty element is not the same observation as an
omitted one, and an attribute substrate will not answer is refused rather than answered with an
invented default.

| Behaviour | Answer |
|---|---|
| `Attribute` | `Required: Yes` on the describe and the reset — "you can specify only one attribute at a time" — and `Required: No` on the modify, where the wire form carries the selection. Valid values `createVolumePermission` and `productCodes`; anything else is `InvalidParameterValue` |
| `productCodes` on the describe | Answered, as a **present but empty** element. Nothing in substrate assigns a product code, so "none" is a fact about every snapshot it can produce rather than an invented default — and an SDK reads a present-but-empty element as an empty list where it reads an omitted one as `nil` |
| `productCodes` on the modify | Refused: "only volume creation permissions can be modified" |
| `productCodes` on the reset | Refused: "only the attribute for permission to create volumes can be reset" |
| `createVolumePermission` on a fresh or copied snapshot | Present and empty. AWS describes a reset snapshot as "a private snapshot that can only be used by the account that created it", which is what a created one already is |
| The two wire forms of a modification | Both read. Structured is `CreateVolumePermission.Add.N.{UserId,Group}` and `…Remove.N.…`, which is what AWS's own examples send; flat is `OperationType` (`add`/`remove`) with `UserId.N` and **`UserGroup.N`** — the wire member, where the CLI spells the same thing `--group-names` and an SDK spells it `GroupNames`. A request using both simply concatenates; AWS documents no precedence |
| `Group` | Only `all`. Any other group is refused rather than stored, since a stored group nothing can grant is a permission a caller reads back and cannot act on |
| Adding *and* removing an **account ID** in one request | Refused. AWS: "You may add or remove specified AWS account IDs … but you cannot do both in a single operation" |
| Adding a group while removing an account | **Allowed** — that is AWS's own Example 2, which adds `all` while removing `111122223333`. The sentence above is scoped to account IDs, so the refusal is too; read as a blanket rule it would reject AWS's example |
| Sharing an **encrypted** snapshot publicly | Refused: "you can share only unencrypted snapshots publicly". Sharing an encrypted snapshot with a named account is not refused — the rule is about the group. The companion rule, that a snapshot carrying a Marketplace product code cannot be made public, is unreachable rather than unmodelled: substrate assigns no product codes |
| More than 500 modifications | Refused: "you can make up to 500 modifications to a snapshot in a single operation" |
| A flat `UserId.N`/`UserGroup.N` with no `OperationType` | `MissingParameter`. The values name no list to join, and accepting the request would silently discard the permission |
| A modify naming no modification at all | Succeeds and changes nothing. Every parameter but `SnapshotId` is optional and AWS publishes no error for the empty case |
| Adding a permission already present, or removing one that is not | Neither is an error, and neither duplicates. AWS documents no refusal for either, and both are idempotent in the direction a cleanup loop needs |
| The reset | Clears the list rather than restoring a remembered default, since an empty list is what a snapshot is created with |

Substrate is single-account, so a permission recorded here grants nothing. It is recorded
intent a caller can read back, which is the observable half of sharing — and the observable
half is the whole of what substrate models.

### Deleting a snapshot refuses what AWS refuses

`DeleteSnapshot` did delete the record — it has since
[#325](https://github.com/scttfrdmn/substrate/issues/325), whatever its doc comment claimed —
and validated nothing whatever. **Every** well-formed ID answered HTTP 200 and
`<return>true</return>`, which is the answer a caller reads as "deleted": a cleanup loop
deleting a typoed ID, an ID from another region, or the same ID twice was told each time that
it had removed a snapshot.

| Request | Answer |
|---|---|
| `SnapshotId` omitted | `MissingParameter` — AWS marks it `Required: Yes` |
| Not a snapshot ID (`vol-…`, `ami-…`, no prefix, non-hex, uppercase) | `InvalidSnapshotID.Malformed` |
| Well-formed, names nothing — **including a second delete of the same ID** | `InvalidSnapshot.NotFound` |
| A registered AMI's **root device** snapshot | `InvalidSnapshot.InUse`, naming both the snapshot and the AMI |
| Anything else | Deleted; a subsequent `DescribeSnapshots` does not report it |

The in-use rule is AWS's: "You cannot delete a snapshot of the root device of an EBS volume
used by a registered AMI. You must first deregister the AMI before you can delete the
snapshot." Its scoping — the *root* device specifically — is load-bearing, not incidental. An
AMI records the whole mapping it was registered with, so it can reference snapshots that are
not its root device's, and **those are not protected**: deleting one succeeds and leaves the
mapping pointing at a snapshot that no longer exists, which `DescribeImages` then renders at
the 8 GiB default. That is what AWS's sentence says, and following the API model rather than a
wider guess is the standing rule here. Which mapping is the root device's is decided by the
request's `RootDeviceName`, falling back to the first mapping that names a snapshot; a
`CreateImage`-minted AMI has one snapshot and it is the instance's root volume's.

Two consequences worth planning for:

- **The operation is not idempotent**, and against AWS it never was. A consumer whose teardown
  is written to be re-runnable has to tolerate `InvalidSnapshot.NotFound`, which is what it has
  to tolerate against AWS. Substrate answering 200 was hiding that requirement.
- **Order matters**: deregister the AMI, then delete its snapshot. Substrate previously let the
  reverse order succeed, leaving an AMI pointing at a snapshot that no longer existed — a state
  real EC2 cannot be put into.

When two AMIs reference one snapshot — reachable since
[#328](https://github.com/scttfrdmn/substrate/issues/328), because `RegisterImage` against an
existing snapshot shares it — the refusal names the **lowest** image ID. That tie-break exists
so identical inputs produce an identical response body on replay; without it the message would
follow Go's map iteration order.

Provenance: none of these codes is on `API_DeleteSnapshot.html`, whose Errors section is
empty. All three come from EC2's client-error table, and its `InvalidSnapshot.InUse` entry
*describes* the condition ("The snapshot that you are trying to delete is in use by one or
more AMIs") rather than quoting a wire message, so the message substrate returns is its own
wording of AWS's description — match on the code, not the string.

### Every taggable ID prefix is reachable

`CreateTags` resolved ten ID prefixes and **silently ignored the rest**: a request naming an
`ami-`, `lt-`, `fleet-`, `pg-` or `key-` ID answered `<return>true</return>` and wrote
nothing. Every one of the five was well-formed and named a resource substrate stores, so
there was no way for a caller to tell — the answer a consumer's tag-everything step reads is
the same answer it gets when the tags land.

Seventeen prefixes now resolve, on `CreateTags` and `DeleteTags` alike — the fifteen the
resolution below covers, `cr-` with the Capacity Reservation operations (#891), and `sir-` with
the Spot Instance request operations (#1436):

| Prefix | Resource type | Prefix | Resource type |
|---|---|---|---|
| `i-` | `instance` | `snap-` | `snapshot` |
| `vpc-` | `vpc` | `ami-` | `image` |
| `subnet-` | `subnet` | `lt-` | `launch-template` |
| `sg-` | `security-group` | `fleet-` | `fleet` |
| `igw-` | `internet-gateway` | `pg-` | `placement-group` |
| `rtb-` | `route-table` | `key-` | `key-pair` |
| `eipalloc-` | `elastic-ip` | `vol-` | `volume` |
| `nat-` | `natgateway` | `cr-` | `capacity-reservation` |
| `sir-` | `spot-instances-request` | | |

**A prefix naming no taggable type is now refused** rather than ignored, before anything is
written:

```
InvalidID: The ID 'tgw-0abc11112222333d' for the resource you are trying to tag is not
valid. Ensure that you provide the full resource ID; for example, ami-2bb65342 for an AMI.
```

The status is `400`, and the check runs over every `ResourceId.N` before the first tag is
applied — so a request naming a good instance ID first and a bad ID second tags neither, the
same all-or-nothing rule [the reserved-key check](#reserved-tag-keys) follows.

A **well-formed prefix naming nothing** stays a no-op at HTTP 200, and is not counted against
the [50-tag limit](#the-50-tag-per-resource-limit): there is nothing to apply the tags to, so
refusing would reject a request real EC2 accepts.

Two of the sixteen are keyed by **name** rather than by ID in substrate's state, and by name
in AWS's ARN as well — `arn:aws:ec2:${Region}:${Account}:placement-group/${PlacementGroupName}`
and `…:key-pair/${KeyPairName}`. `CreateTags` takes the `pg-`/`key-` form and translates by
scanning the namespace for the ID inside each record; `DescribeTags` reports the `resourceId`
member the record carries, not the name its key ends in, so what comes back is what a caller
can pass in again.

Whether real `CreateTags` takes a placement group by ID or by name **is not settled by AWS's
documentation**: the ARN is by name and `DescribePlacementGroups` publishes no `group-id`
filter, but the client-error table publishes `InvalidPlacementGroupId.Malformed` "in the form
`pg-xxxxxxxxxxxxxxxxx`", which only an ID-taking operation can raise. Substrate accepts the
`pg-` form.

Three things had to change alongside the resolution:

- **A launch template's own tags were settable by no path at all.** The `TagSpecification` in
  `CreateLaunchTemplate`'s `LaunchTemplateData` is scoped to the resources a *launch* creates
  — "for the resources that are created when an instance is launched" — which is a different
  parameter from the top-level `TagSpecification.N`, "the tags to apply to the launch
  template on creation. To tag the launch template, the resource type must be
  `launch-template`". Substrate read only the inner one. It now reads both, each on its own
  scope.
- **`CreateKeyPair`, `ImportKeyPair` and `CreatePlacementGroup` honour `TagSpecification.N`**
  and echo the result, and `DescribeKeyPairs`/`DescribePlacementGroups` render `tagSet`.
  `EC2KeyPair` had no tags field to write to.
- **An `image` and a `snapshot` ARN have an empty account field** —
  `arn:${Partition}:ec2:${Region}::image/${ImageId}` — where the other fourteen carry
  `${Account}`. The authorizer stamped the account on unconditionally, which makes an
  ARN-scoped `Deny` naming an AMI inert and a least-privilege `Allow` unable to grant the
  call. The `snap-` arm shipped with that defect in
  [#689](https://github.com/scttfrdmn/substrate/issues/689) and is fixed here too.

#### An untagged resource omits `tagSet`

Seven renderers previously answered `<tagSet></tagSet>` for a resource with no tags and now
omit the element: `CreateKeyPair`, `ImportKeyPair`, `DescribeKeyPairs`,
`CreatePlacementGroup`, `DescribePlacementGroups`, `CreateLaunchTemplate` and
`DescribeLaunchTemplates`. This is a **wire change**, and it follows AWS's own examples —
`CreateLaunchTemplate`'s Example 1 and `CreatePlacementGroup`'s Example 2 each create an
untagged resource and neither response carries a `tagSet` at all, while `CreateKeyPair`'s
tagged example does. An SDK distinguishes an absent list from an empty one, which is the same
distinction `DescribeTags` keeps `<value/>` present-but-empty for.

`DescribeFleets` is the one place the old shape stays. Its API reference page publishes **no
example response**, so there is no untagged sample to follow and changing it would be
substrate guessing rather than substrate reading. `DescribeSnapshots` keeps its
present-but-empty element for the opposite reason: its own page shows it. Each response
answers to the page that documents it.

### Finding a resource by tag

`DescribeTags` — the operation whose whole job is this question — did not exist until
[#688](https://github.com/scttfrdmn/substrate/issues/688), and answered
`InvalidAction` / HTTP 400, while four of substrate's bundled managed policies *granted*
`ec2:DescribeTags`: `AmazonVPCFullAccess` and `AmazonVPCReadOnlyAccess` name it outright, and
`AmazonEC2FullAccess` and `AmazonEC2ReadOnlyAccess` reach it through `ec2:*` and
`ec2:Describe*`. A policy permitted an operation nothing served, and the only routes left were
the per-operation `tag:<key>` filters (five operations) and Resource Groups Tagging.

It reports every tag stored in the request's account and region. Every EC2 record key is
`<namespace>:<account>/<region>/<id>`, so the scan is regional by construction, which is also
real EC2's scope for this operation.

**The scan and what `CreateTags` can write are the same sixteen types.** They were not:
`DescribeTags` read thirteen and `CreateTags` reached ten, so an `image`, `launch-template`
or `fleet` tag could be *reported* and not *changed*, while a `placement-group` or `key-pair`
tag could be neither — both were absent from the scan as well. See
[every taggable ID prefix is reachable](#every-taggable-id-prefix-is-reachable) for the
prefixes and for what a request naming something else now answers.

| Behaviour | Answer |
|---|---|
| Filters | `key`, `resource-id`, `resource-type`, `value`, `tag:<key>` — every name AWS documents, all evaluated, none inert |
| `tag-key` | **Refused.** This operation documents no such filter, alone in the describe family, because `key` already matches a key whatever its value |
| Wildcards | `*` and `?` work in every filter value, which AWS's Example 4 states outright ("specify the value as `?ebserver` to find tags with the key `webserver` or `Webserver`"). Since #697 that is true of [every EC2 filter](#wildcards-in-filter-values), not just this one |
| An empty value | A tag whose value is the empty string renders `<value/>` and is matched by `Filter.N.Value.1=` — AWS's Example 1 and Example 6 respectively |
| `MaxResults` | 5–1000. A value outside the range is **refused** with `InvalidParameterValue`, not clamped: a caller who asked for 2000 asked for something the operation cannot do |
| `NextToken` | A decimal offset. A malformed token is refused; an offset past the end is clamped to an empty last page, so resuming after a tag was deleted is not an error |
| Order | **Sorted by `resourceId`, then `resourceType`, then `key`** |

The sort is *stricter than AWS*, which says its own order "might vary" and that applications
should not rely on it. Substrate must be stricter: `StateManager.List` promises no ordering
either, so an offset-based token over an unordered list could skip or repeat a tag between
pages, and two replays of one recorded request could answer differently. A deterministic
emulator cannot answer one request two ways.

Authorization needs nothing special: the Service Authorization Reference gives
`DescribeTags` resource type "—", and substrate authorizes it against `*`.

### One offset paginator, shared

`DescribeTags` and `DescribeLaunchTemplateVersions` each carried their own copy of the same
three rules — read `MaxResults`, decode `NextToken`, cut the page — while sixteen other routed
describes published both parameters and implemented neither. Those answered the **whole listing
with no token**, which is the one divergence a paginating caller cannot see: the loop terminates
on the first page against substrate and finds a second page in production. #917 replaced the two
copies with one shared paginator and converted seven of the sixteen onto it, so the count of
implementations went down rather than up. #1024 converted the remaining nine, in three parts
grouped by the range each page publishes: the first is the three publishing **no** range —
`DescribeInstanceStatus`, `DescribeSpotPriceHistory` and `DescribeFleets` — the second is the five
publishing a floor of five, `DescribeInternetGateways`, `DescribeNatGateways`,
`DescribeRouteTables`, `DescribeInstanceTypes` and `DescribeInstanceTypeOfferings`, and
`DescribeLaunchTemplates` alone is the third, because its is the one page in the set whose
published floor is **not** five. **No routed describe publishing both parameters implements neither
any longer.**

The eighteen that page are therefore `DescribeTags` and `DescribeLaunchTemplateVersions`, which
already did, plus `DescribeVolumes`, `DescribeSnapshots`, `DescribeImages`, `DescribeVpcs`,
`DescribeSubnets`, `DescribeSecurityGroups` and `DescribeInstances` from #917, plus
`DescribeInstanceStatus`, `DescribeSpotPriceHistory` and `DescribeFleets` from #1024's first part,
`DescribeInternetGateways`, `DescribeNatGateways`, `DescribeRouteTables`,
`DescribeInstanceTypes` and `DescribeInstanceTypeOfferings` from its second, and
`DescribeLaunchTemplates` from its third. Wire behaviour for a caller that sends neither parameter
is unchanged at every one of them: the count is exact and audited, rather than the "roughly twenty"
this paragraph used to estimate.
`DescribeCapacityReservations` joined the paginating set later (#891) rather than being converted,
so it is a nineteenth: its published range is 1–1000, and it reads both parameters from the start.

AWS publishes the mechanism **once for the whole service**, in the Query Requests page's
*Pagination* section rather than per operation, and two of its sentences decide the design:

> With pagination, you continue to call the action until `nextToken` is null, even if you receive
> less than `MaxResults` items, including zero items.

> If you call a describe API action with both a list of IDs and `MaxResults`, the request fails
> with the error `InvalidParameterCombination`.

So a **short page is not the end of the listing** — the token is emitted from whether a further
record exists, not from whether the page filled up, which is also why a listing whose size is an
exact multiple of the page size costs no extra round trip to an empty page. And the **last page
carries no token at all**, because a caller told to keep calling until the token is null would
otherwise loop forever.

| Behaviour | Answer |
|---|---|
| An absent `MaxResults` | The whole listing, with no `nextToken` element. `API_DescribeSecurityGroups` is the one page that states this outright — "If this parameter is not specified, then all items are returned" — and it is what every converted operation answered before it paginated. `DescribeTags` and `DescribeLaunchTemplateVersions` keep their own defaults of 1000 and 200, since neither page publishes the sentence and both defaults shipped deliberately — which is why `DescribeLaunchTemplates` and `DescribeLaunchTemplateVersions` share a published range and not a default: the first reports the whole listing, the second pages at 200 |
| `MaxResults` out of range | **Refused** with `InvalidParameterValue` / 400, never clamped: a caller who asked for 2000 items asked for something the operation cannot do, and silently answering 1000 hides that |
| `NextToken` | A plain decimal offset, and a token that is not a non-negative integer is refused with `InvalidParameterValue` / 400. It is validated **before any state is read**, so the refusal does not depend on how many resources happen to exist |
| An offset past the end | Clamped to an empty last page rather than refused — a caller resuming a walk after a record was deleted holds a token that was valid when it was issued |
| An ID list **and** `MaxResults` | `InvalidParameterCombination` / 400. Checked before the ID list's own syntax: whether two parameters may appear together does not depend on either being well formed. Which refusal AWS answers first is not published, so the ordering is substrate's |

**The range is per operation, and seven of the eighteen pages publish none.** `API_DescribeVpcs`,
`API_DescribeSubnets`, `API_DescribeSecurityGroups`, `API_DescribeInternetGateways`,
`API_DescribeNatGateways` and `API_DescribeInstanceTypeOfferings` publish `Valid Range: Minimum
value of 5. Maximum value of 1000.`; `API_DescribeRouteTables` and `API_DescribeInstanceTypes`
publish the same floor with `Maximum value of 100.`; `API_DescribeLaunchTemplates` publishes
`Minimum value of 1. Maximum value of 200.` and repeats it in prose; `API_DescribeTags` (5–1000) and
`API_DescribeLaunchTemplateVersions` (1–200) state theirs in prose only; `API_DescribeInstances`,
`API_DescribeImages`, `API_DescribeVolumes`, `API_DescribeSnapshots`,
`API_DescribeInstanceStatus`, `API_DescribeSpotPriceHistory` and `API_DescribeFleets` publish **no
bound at all** — only "the maximum number of items to return for this request", type `Integer`.
Substrate does not borrow 5–1000 from the siblings at those seven, per the scope rule that only
what the API model states is modelled: `MaxResults=5000` is accepted on all seven, and
`MaxResults=1` is accepted there and refused on the eight that publish a floor of five. A floor of
one is therefore reached two different ways — *published* at the two launch-template pages and
*substrate's reading* at those seven — and the distinction is why they are separate constants even
though the number is the same.

**The ceiling is per operation too, and two neighbouring pages disagree about the same value.**
`MaxResults=1000` is accepted on `DescribeNatGateways` and **refused** on `DescribeRouteTables`,
which publishes 100 — two operations a caller reaches in the same breath while wiring a VPC, whose
requests substrate must answer differently because their pages do. `DescribeInstanceTypes` (100)
sits beside `DescribeInstanceTypeOfferings` (1000) the same way, and `DescribeLaunchTemplates` (200)
refuses it too. Harmonising any of those pairs would be a one-character edit and would make
substrate accept a request AWS rejects, which is the direction of divergence that matters: the code
would work here and fail in production.

The floor of **one** at those seven pages is *substrate's reading*, and it is the single bound the
published pagination rule forces. `MaxResults=0` under "you continue to call the action until
`nextToken` is null, even if you receive less than `MaxResults` items, including zero items"
describes a walk that can never advance — every call answers nothing and hands back a token — so
refusing it is the only answer that does not invite an infinite loop.

The token is a **decimal** offset rather than the base64 form the CloudWatch, Systems Manager and
S3 listings use (see [A pagination token substrate never
issued](/reference/concepts/listing-order#a-pagination-token-substrate-never-issued-is-refused-not-answered-with-page-one)). That
is the shape both original EC2 operations already issued, so it is part of every run already
recorded against them; and the property base64 buys — that a token substrate never issued is
detectable — is worth less here, because an invented decimal offset resumes the walk from that
offset instead of silently restarting it.

Paging is a cut of an already-assembled answer rather than an early exit from the scan, which is
load-bearing at `DescribeSnapshots`: a [seeded status progression](#seeding-a-snapshot-progression)
advances once per observation, so stopping the scan at the page boundary would make a countdown
advance by an amount that depended on the caller's page size.

**`DescribeInstances` counts instances, not reservations** — *substrate's reading*, because its
answer is the one nested listing in the set: `reservationSet > item > instancesSet`. The page says
only "the maximum number of items" and never states which of the two lists an item is. Counting
reservations would leave `MaxResults` unable to bound a response at all, since one `RunInstances`
with `MinCount=500` is a single reservation and a page of five could hold five hundred instances;
and `NextToken`'s own text — "Pagination continues from the end of the items returned by the
previous request" — describes a position in a flat sequence rather than in the grouping.

The visible consequence, also substrate's reading and also unpublished, is that **a reservation
whose instances straddle a page boundary is reported on both pages**, each carrying only the
instances belonging to that page. The alternative — never splitting a reservation — would have to
answer either more instances than `MaxResults` asked for or fewer than are available alongside a
token, each of which contradicts the parameter more visibly than a repeated reservation ID does. An
*instance* is still reported on exactly one page, which is what a caller assembling a walk relies
on.

**`GroupName.N` does not conflict with `MaxResults`** on `DescribeSecurityGroups`, where `GroupId.N`
does — substrate's reading again. The service-wide rule is stated against "a list of IDs", and a
group name is not an ID; it is not what `InvalidGroup.NotFound` is about either (see
[Which selectors assert existence](#which-selectors-assert-existence)). AWS publishes nothing about
the combination, so refusing the name form would mean extending a published rule to a parameter it
does not name. `DescribeLaunchTemplates` is the second operation that reading applies to, and the
only one where the two selectors *union*: `LaunchTemplateId.N` forbids `MaxResults` and
`LaunchTemplateName.N` pages alongside it, so a request naming both lists and `MaxResults` is still
refused — the rule is about the ID list appearing, not about it being the only selector.

**Three paginating describes carry no `InvalidParameterCombination` refusal at all**, because the
service-wide rule is stated against "a list of IDs" and none of the three has one.
`DescribeInstanceTypeOfferings` is the plainest: its whole request is `DryRun`, `Filter.N`,
`LocationType`, `MaxResults` and `NextToken`, so there is no candidate parameter even to argue
about. `DescribeSpotPriceHistory`'s `InstanceType.N` is documented as "Filters the results by the
specified instance types" — a filter, and an instance type is not a resource ID, which is the same
reading that makes an unknown type an empty history there rather than `InvalidInstanceType`.

`DescribeInstanceTypes` is the interesting one, and it lands the same way. Its `InstanceType.N` is a
*stronger* parameter than the spot-price namesake — it asserts the types exist, so an unknown one is
refused with `InvalidInstanceType` — but what it names are catalog members rather than resources the
account holds, and the published rule is about resource IDs. So `MaxResults` and `InstanceType.N` are
read together at all three, which is asserted rather than assumed, because a sweep is exactly where a
published rule gets applied one operation too far.

The opposite case is **`DescribeInstanceStatus`, where the prohibition is published twice over**:
the service-wide sentence, and its own page repeating it against its own parameter in the same
words `API_DescribeInstances` uses — "You cannot specify this parameter and the instance IDs
parameter in the same request."

**An instant fleet can never appear on a paginated `DescribeFleets` page.** That is AWS's
arithmetic from two published rules rather than substrate's choice: a fleet of type `instant` is
reported only when its ID is named (see
[Seeding EC2 Fleet partial fulfillment](#seeding-ec2-fleet-partial-fulfillment) for the fleet
types substrate models), and naming an ID list forbids `MaxResults`. So the two conditions cannot
hold at once, and substrate's own pagination tests build `maintain` fleets for that reason.

**One published shape substrate does not take:** `API_DescribeSpotPriceHistory`'s Example Response
shows `<nextToken/>` on a last page, while the same member is documented as "an empty string ("")
or null when there are no more items". Both shapes are published, and substrate omits the element
— the answer every other converted describe gives, and one a caller decoding into a string reads
as `""` either way.

**`DescribeLaunchTemplates` was the last of the sixteen**, and it is a part of its own rather than a
sixth row of the five above because `API_DescribeLaunchTemplates` publishes **1–200** — the only page
in the whole set whose floor is 1 rather than 5, stated as a `Valid Range` line and repeated in prose.
`API_DescribeLaunchTemplateVersions` states the same range in prose alone, so the two share one pair
of constants without either borrowing the other's bound; what they do **not** share is the default,
since neither page publishes one and each keeps the behaviour it shipped with.

Two further readings are recorded there. Its offset counts positions in the account's launch-template
index, which is kept **sorted by ID** rather than in creation order, so the same offset names the same
template on two calls without an explicit sort — the one converted describe whose ordering comes from
an index rather than from `StateManager.List`. And `IncludeManagedResources` is published and read
nowhere: substrate models no launch template owned by another service, so the parameter has nothing to
include or hide, and it is recorded here rather than refused, because refusing a published parameter
is the larger divergence.

`GetSpotPlacementScores` is the sharpest illustration of the range being per operation, and it is
not part of that count because it reads both parameters already: its published floor is **10**,
higher than any `Describe*` in the service. A shared floor would accept `MaxResults=1` there, which
AWS refuses.

Seven further routed `Describe*` operations publish **neither** parameter and so are not part of
that count: `DescribeKeyPairs`, `DescribePlacementGroups`, `DescribeAvailabilityZones`,
`DescribeAddresses`, `DescribeRegions`, and the two single-attribute reads
`DescribeInstanceAttribute` and `DescribeSnapshotAttribute`. Answering everything in one page is
what AWS describes at each, so there is nothing there to convert.

## Key pair fingerprints

A key pair's fingerprint follows the published rule for its type and for where it came from
([#1296](https://github.com/scttfrdmn/substrate/issues/1296) for a created key, [#1190](https://github.com/scttfrdmn/substrate/issues/1190) for an imported one). The
EC2 User Guide's "Verify the fingerprint of your key pair" tabulates them:

| Key pair | Fingerprint | Form |
|---|---|---|
| RSA, created by `CreateKeyPair` | SHA-1 of the DER (PKCS#8) private key — "the SHA-1 digest of the DER encoded private key" | colon-separated hex, 59 characters |
| RSA, imported by `ImportKeyPair` | MD5 of the DER public key | colon-separated hex, 47 characters |
| ED25519, either way | SHA-256 of the SSH-encoded public key — "the base64-encoded SHA-256 digest, which is the default for OpenSSH" | padded base64, 44 characters |

The imported-RSA row follows the User Guide's verification command, `openssl rsa -in … -pubout
-outform DER | openssl md5 -c`, which digests the DER public key. `API_ImportKeyPair` glosses the same
value as "the MD5 public key fingerprint as specified in section 4 of RFC 4716", which would digest
the SSH encoding instead; substrate follows the command, because it is what a caller runs to check
the value. An ED25519 key created by `CreateKeyPair` and then imported keeps its fingerprint, as the
User Guide says it does. Before #1190 every import answered a colon-separated SHA-256 of the raw
material, which matched neither rule.

`CreateKeyPair` generates both types for real: RSA as the 2048-bit PKCS#1 `RSA PRIVATE KEY` PEM the
page publishes, and ED25519 in OpenSSH format. `KeyFormat=ppk` is refused rather than generated:
PuTTY's format carries a MAC substrate has no reference output to check against.

**Compatibility.** `ImportKeyPair` parses the key, so material that is not an RSA or ED25519 public
key in a supported format, placeholder strings included, is now `InvalidKeyPair.Format`, as AWS
answers it; so is an RSA key of a length other than 1024, 2048 or 4096 bits. Neither `CreateKeyPair` nor `ImportKeyPair` renders `keyType` any longer;
read it from `DescribeKeyPairs`.

## Request validation for key pairs and placement groups

EC2's key-pair and placement-group operations — `CreateKeyPair`, `ImportKeyPair`,
`DeleteKeyPair`, `DescribeKeyPairs`, `CreatePlacementGroup`, `DeletePlacementGroup` and
`DescribePlacementGroups` — are validated against the service model before any plugin sees a
client's request (#1612; see [Request validation](/reference/concepts/validation)). The rest of
EC2 is not yet. Of the seven, three have a member the model requires — `KeyName` on
`CreateKeyPair` and `ImportKeyPair`, `PublicKeyMaterial` on `ImportKeyPair`, and `GroupName` on
`DeletePlacementGroup` — and a request missing one is `MissingParameter`/400, `The request must
contain the parameter KeyName`; the plugin answers a blank one the same way. The parameter is named
by the model's `locationName` where it has one (`publicKeyMaterial`, `groupName`), as EC2's
observed answers name `instanceId` and `ebs`, and by the member name otherwise (`KeyName`); that
rule is substrate's reading of those two answers. `DeleteKeyPair`'s and `CreatePlacementGroup`'s
own checks are the plugin's, not the model's: `CreatePlacementGroup`'s `GroupName` is `Required:
No` in the model and the reference. The EC2 model publishes no length or range on these
operations, and enum checks are off.

## Every response carries its requestId

**Every EC2 success document leads with `requestId`** ([#1189](https://github.com/scttfrdmn/substrate/issues/1189)). Each operation
page lists it among its Response Elements — "requestId: The ID of the request" — and every sample
response opens with it:

```xml
<AllocateAddressResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
   <requestId>59dbff89-35bd-4eac-99ed-be587EXAMPLE</requestId>
   ...
```

Its value is the request's own ID, the one the event log records and a replay is dispatched with,
never one minted per call, so it can be asserted on and a replayed call renders the body it recorded.
An internal dispatch carrying no request ID — a hand-built request context — renders no element.

The element is inserted once, at the plugin's `HandleRequest`, after the root element's start tag of
any 2xx `…Response` document, rather than declared on each of the roughly seventy response structs.
That trades type checking for a single place no handler can forget — the trade IAM's request-ID
stamp made — and it is the reason a response struct does not carry the member. Error documents are
not touched: they already carry the ID as `<RequestID>` after `<Errors>`, which is the shape the
error-code reference's "Example error response" shows.

**Compatibility.** Every EC2 success body gains the element, so a test comparing a whole body sees
it, and two calls over unchanged state differ in it. An event log recorded before this change
replays with one body difference per EC2 call, at `requestId`.

## Seeding EC2 Fleet partial fulfillment

`CreateFleet` fulfills its whole `TotalTargetCapacity` by default. Partial
fulfillment — the case callers most often get wrong, since a fleet that asks for
12 and receives 8 still returns a fleet ID and echoes the *request* in
`TotalTargetCapacity` — is reachable by seeding a shortfall:

```bash
# Fulfill 8 instances and report the remainder as a capacity failure.
curl -X POST http://localhost:4566/v1/ec2/fleet-shortfall \
  -d '{"launchTemplate":"lt-0abc123","fulfill":8,
       "errorCode":"InsufficientInstanceCapacity","lifecycle":"spot"}'

# Clear one seed, or all of them.
curl -X DELETE 'http://localhost:4566/v1/ec2/fleet-shortfall?launchTemplate=lt-0abc123'
curl -X DELETE http://localhost:4566/v1/ec2/fleet-shortfall
```

`launchTemplate` matches a launch template ID or name, or `*` (the default) for
any. The shortfall is spread across the request's capacity pools, so `errorSet`
reports one item per pool that came up short, and `DescribeFleets` reports the
result in `fulfilledCapacity`.

## Seeding a snapshot progression

Every snapshot substrate writes is born `completed`, so the one loop callers actually write
around this API — poll `DescribeSnapshots` until `status` is `completed`, which is what CDK's
custom resources, Terraform's `aws_ebs_snapshot` and `aws ec2 wait snapshot-completed` all do —
exits on its first iteration. The retry, timeout and error branches such a loop carries are
never taken, so a consumer whose polling is broken, or which treats `error` as retryable
forever, passes against substrate and fails against AWS.

```bash
# The next four observations report pending; the fifth reports completed.
curl -X POST http://localhost:4566/v1/ec2/snapshot-status \
  -d '{"snapshotId":"*","pendingObservations":4}'

# Fail immediately, with the diagnostic AWS publishes for a failed copy. DescribeSnapshots
# renders it as `statusMessage`, which an SDK reads as `StateMessage`.
curl -X POST http://localhost:4566/v1/ec2/snapshot-status \
  -d '{"snapshotId":"snap-0abc123","pendingObservations":0,
       "finalState":"error","stateMessage":"Given key ID is not accessible"}'

# Clear one seed, or all of them.
curl -X DELETE 'http://localhost:4566/v1/ec2/snapshot-status?snapshotId=snap-0abc123'
curl -X DELETE http://localhost:4566/v1/ec2/snapshot-status
```

A seeded `error` is what makes the CLI waiter's own failure path reachable: botocore defines
`SnapshotCompleted` with a `pathAny Snapshots[].State == error` acceptor alongside the success
one — a fact the CLI's own documentation page omits — so `aws ec2 wait snapshot-completed`
exits 255 with "encountered a terminal failure state" rather than retrying for ten minutes.
That is the branch a consumer's error handling exists for, and it was unreachable before.

`snapshotId` matches one snapshot ID or `*` (the default) for any; an ID-scoped seed wins over
the wildcard. `state` defaults to `pending` and `finalState` to `completed`, and both accept any
of the five values AWS publishes for the member — `pending`, `completed`, `error`,
`recoverable`, `recovering`. A value outside those five is refused rather than stored: it is one
no SDK can map and no consumer can branch on, so the seed would look accepted and produce a
response the caller's own model rejects. Seeding again restarts the countdown, so a test that
seeds twice gets two full progressions rather than the remainder of the first.

**The progression is counted in observations, not measured as a duration.** The simulated clock
advances with wall time from its baseline, so a duration seed would expire partway through a
test and make every "still pending" assertion depend on how long the rest of the test took —
which no test here may be. This follows the reasoning the SQS consistency seed records. A count
is also the more useful unit: "the next two polls see pending" is what a test of a poll loop
wants to say.

The count is **per snapshot** even under a `"*"` seed. Without that, one `DescribeSnapshots`
over five snapshots would burn five observations off a single shared countdown, and the
snapshot a test was actually watching would complete early.

`progress` is the fraction of the countdown already spent, so a four-observation seed reports
`0%`, `25%`, `50%`, `75%` and then `100%`. AWS documents the member only as "the progress of the
snapshot, as a percentage", so the *schedule* is substrate's; it is deterministic, which is the
property that matters, and a consumer must no more assert on an exact intermediate value than it
may against AWS — whose own `CreateSnapshot` sample response implausibly shows a freshly created
snapshot at `60%`. Progress reaches `100%` whatever the terminal state, rather than freezing
below it for a failure: AWS's one observable data point says so, its
`restore-snapshot-from-recycle-bin` example showing `"Progress": "100%"` beside
`"State": "recovering"`. So progress measures how far the progression ran, not whether it
succeeded.

Several operations read the state without consuming an observation, since none of them is a
poll:

- **`CreateVolume`** refuses a snapshot that is not `completed` with `IncorrectState`. The rule
  is AWS's — its snapshot-states table says "a snapshot can't be used while it is in the
  `pending` state" and "a snapshot can't be used if it is in the `error` state" — though
  `CreateVolume`'s own page publishes no error for it, so the code is substrate's choice from
  EC2's client-error table, the one it already answers with for a volume in the wrong state.
- **A block device mapping** naming such a snapshot is refused the same way, with the same code,
  wherever the mapping is *consumed*: on the launch path (`RunInstances`, and `CreateFleet`
  through it) and in `RegisterImage`. One rule, one place, two doors — because a launch that
  restores a volume from a snapshot is doing what `CreateVolume` does, and the two disagreeing
  meant a consumer restoring through a launch never reached its own error branch
  ([#732](https://github.com/scttfrdmn/substrate/issues/732)). AWS's snapshot-states table covers
  all four non-`completed` states: a `recoverable` snapshot "must first [be recovered] from the
  Recycle Bin", and a `recovering` one is "ready for use" only once it reaches `completed`. So
  the rule is `completed`-or-refused, not pending-and-error-only. `RegisterImage` publishes no
  error for it either — its Errors section is empty — so again the code is substrate's choice.
  This is the one of these reads a single request can make more than once, one mapping each, so
  spending an observation on it would make `pendingObservations: 2` mean something different
  depending on how many volumes a launch declared.
- **The two `CreateLaunchTemplate` operations are exempt, deliberately.** A template *records* a
  mapping rather than consuming one, both report mapping problems through AWS's documented
  `warning` member and refuse nothing, and a snapshot that is `pending` when a template is
  written may legitimately be `completed` by the time the template is used — so refusing at
  write time would forbid an ordering AWS permits. A launch *from* such a template is refused,
  which is where the mapping is used.
- **`DeleteSnapshot`** does *not* refuse one, because AWS permits it in so many words:
  "although you can delete a snapshot that is still in progress, the snapshot must complete
  before the deletion takes effect." The deferred-effect half is not modelled — AWS publishes
  nothing observable about the interval, since a caller cannot see the snapshot after the
  request returns either way.

A seed governs what an *observation* reports and never rewrites the snapshot record, whose
`state` stays `completed`. So clearing a seed — or `POST /v1/state/reset`, which clears the
whole namespace — makes every snapshot read `completed` again, and a snapshot with no seed
against it is untouched.

A seed survives a replay: the `POST` above is recorded as an event and re-applied where it was
written, so a stream whose four `pending` observations preceded a `completed` replays as exactly
that rather than as five `completed`s. The countdown restarts from zero, because the reset still
wipes the `observed:` counters and the recorded `POST` re-arms the seed. See
[How a seed survives a replay](/reference/concepts/seeds-and-replay#how-a-seed-survives-a-replay) for the mechanism and for the one
thing a programmatic replay has to pass.

There is no Python helper for this endpoint: `pytest_substrate`'s `seed_result` and `clear_seeds`
address only the `/v1/{service}/results` endpoints — Athena and Redshift Data, which key a seed by
`sql`, and Timestream (`"timestream-query"`), for which the helper sends `queryString`, the field
that endpoint reads. Before #1200 the helper sent `sql` to Timestream too, which the endpoint
ignored, so every Timestream seed the helper installed became the `"*"` wildcard. Drive this one
with raw HTTP, as the fleet seed above is driven.

## Seeding a NAT gateway progression

A NAT gateway is one of the slowest resources in a VPC to come up, which is why every CDK,
CloudFormation and Terraform run waits on it, and `API_CreateNatGateway`'s two published sample
responses both show `pending`. Substrate settles the record `available` at once, so a consumer's
wait loop never runs its body. A seed makes the transition observable, on the rules every
progression shares ([How a progression is seeded](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded)):

```bash
# A gateway created now is born pending; the next two DescribeNatGateways report pending, then available.
curl -X POST http://localhost:4566/v1/ec2/nat-gateway-state \
  -d '{"natGatewayId":"*","pendingObservations":2}'

# Fail after one pending observation, with a published failure code. The failureMessage defaults
# to the page's message for the code, with the gateway's own allocation ID substituted.
curl -X POST http://localhost:4566/v1/ec2/nat-gateway-state \
  -d '{"natGatewayId":"*","pendingObservations":1,"finalState":"failed",
       "failureCode":"InvalidAllocationID.NotFound"}'

curl -X DELETE 'http://localhost:4566/v1/ec2/nat-gateway-state?natGatewayId=nat-0abc123'
curl -X DELETE http://localhost:4566/v1/ec2/nat-gateway-state
```

- **The default count is zero.** An unseeded gateway reads as it always has, `available` from the
  create on, so every existing fixture and recorded event log replays byte-identically (#1188).
- **`state` and `finalState`** must be values of `NatGateway.state`
  (`pending | failed | available | deleting | deleted`); they default to `pending` and `available`.
- **`failureCode`** must be one of the six codes the page publishes (`InsufficientFreeAddressesInSubnet`,
  `Gateway.NotAttached`, `InvalidAllocationID.NotFound`, `Resource.AlreadyAssociated`,
  `InternalError`, `InvalidSubnetID.NotFound`), and is accepted only with `finalState` `failed`. A
  failed gateway with no code reports `InternalError`, because the page documents a failed gateway
  as always carrying one.
- **The delete is modeled too.** `DeleteNatGateway` restarts the countdown, and the same
  `pendingObservations` report `deleting` before `deleted`.
- **What counts as an observation** is a `DescribeNatGateways` that includes the gateway, taken
  before the `state` filter runs, so a waiter polling with `Name=state,Values=available` still
  advances the countdown. `CreateNatGateway` and a `ClientToken` retry only peek.

**`ClientToken`.** `CreateNatGateway` is on EC2's list of actions that are idempotent using a
client token. A retry with the same token and the same parameters answers the gateway first created,
with its current state, and creates nothing; one with different parameters is
`IdempotentParameterMismatch`. The token is echoed as `clientToken` only when one was sent. A token
over the published 64 ASCII characters is `InvalidParameterValue`, substrate's reading: the page
publishes no code for it.

## Seeding an instance-state progression

Every state change substrate applies reached its terminal state in the same request, so
`pending`, `stopping` and `shutting-down` were three of the six codes AWS publishes for
`instanceState` that no code path could produce. The loop callers actually write around this API —
run, start or stop, then poll `DescribeInstances` until the state settles, which is what
`aws ec2 wait instance-running`, Terraform's `aws_instance` and CDK's own custom resources all do —
exited on its first iteration, so the retry, timeout and give-up branches those loops carry were
never taken ([#514](https://github.com/scttfrdmn/substrate/issues/514)).

Two halves, and only the second needs a seed.

**The operation's own response reports the transient state, unconditionally.** AWS publishes it in
the sample responses of the operations themselves: `API_StartInstances` shows `currentState` 0 /
`pending` beside `previousState` 80 / `stopped`, and `API_StopInstances` shows 64 / `stopping`
beside 16 / `running`. Substrate reported the *settled* state at all four write sites, so a
consumer reading the transition out of the call it just made — which is what a waiter's first
observation is — saw a transition that had already finished. That is a published-response
divergence with nothing to do with seeding, so it is corrected for every caller rather than behind
a seed: `RunInstances` answers `pending`, `StartInstances` `pending`, `StopInstances` `stopping`
and `TerminateInstances` `shutting-down`, while the record itself settles as before. An unseeded
test therefore sees the transient state once, in the operation's own response, and the settled
state on its first describe — AWS with an instantaneous transition.

**How long the transient state is reported is seeded.**

```bash
# The next two observations of any instance report the transient state; the third settles.
curl -X POST http://localhost:4566/v1/ec2/instance-state \
  -d '{"instanceId":"*","transientObservations":2}'

# One instance only, and back to instantaneous.
curl -X POST http://localhost:4566/v1/ec2/instance-state \
  -d '{"instanceId":"i-0abc123","transientObservations":5}'

# Clear one seed, or all of them.
curl -X DELETE 'http://localhost:4566/v1/ec2/instance-state?instanceId=i-0abc123'
curl -X DELETE http://localhost:4566/v1/ec2/instance-state
```

`instanceId` matches one instance ID or `*` (the default) for any; an ID-scoped seed wins over the
wildcard. `transientObservations` defaults to `0`, which is the instantaneous behaviour the whole
existing suite pins, and a negative count is refused rather than stored. The seed is read by
`DescribeInstances` and `DescribeInstanceStatus`, the two operations a poll loop uses; every other
read of an instance is unaffected.

**The transient state is derived, not chosen.** Unlike a snapshot's `status` — a free choice from a
five-value enumeration, which is why the snapshot seed carries `state` and `finalState` — the state
on the way to a target is fixed by AWS's own lifecycle: `pending` is "preparing to enter the
running state … when it is launched or when it is started after being in the stopped state",
`stopping` is "preparing to be stopped", `shutting-down` is "preparing to be terminated". So the
seed carries a count and nothing else, and seeding a state substrate would not transition through
is not offered.

**The progression is counted in observations, not measured as a duration**, for the reason the
snapshot seed records: the simulated clock advances with wall time from its baseline, so a duration
seed would make every "still pending" assertion depend on how long the rest of the test took. The
count is **per instance** even under a `"*"` seed, so one `DescribeInstances` over five instances
does not burn five observations off a single shared countdown. Every state change restarts the
count, which is what makes one seed serve a stop-then-start sequence rather than only the first
transition in it.

**A seed governs what an observation reports and never rewrites the instance record**, which is
load-bearing here in a way it is not for a snapshot: because the stored state is always the settled
one, a `StartInstances` that follows a `StopInstances` still sees `stopped` and succeeds, so seeding
a progression cannot break a caller's own sequence. It is also why no operation ever *observes* a
transient state, and therefore why substrate refuses nothing on account of one — neither
`API_StartInstances` nor `API_StopInstances` publishes a state precondition at all, both Errors
sections being empty.

One refusal is published, and substrate answers it now: **a `terminated` instance can be neither
started nor stopped.** The lifecycle page's state table says such an instance "has been permanently
deleted and cannot be started", so the start refusal rests on that sentence directly; the stop
refusal rests on "permanently deleted" alone, no page found stating a stop precondition, and is
substrate's reading rather than letting a stop resurrect a deleted instance into `stopped` — from
which it would then start. The code is `IncorrectInstanceState` / `400`, whose published
description is the general rule this is an instance of ("The instance is in an incorrect state for
the requested action"); neither operation page publishes an error of its own, so the message text
is substrate's.

## Seeding a Spot placement score

`GetSpotPlacementScores` answers a recommendation AWS computes from live Spot capacity, and
substrate models no capacity broker — so the observations a consumer actually branches on cannot be
derived from anything substrate knows. Its usual shape is "sample the free score, and only pay for a
fulfillment probe where it looks promising", which makes the *low*-score branch the one most worth
testing and the one that is unreachable without a seed.

```bash
# One region scores badly; the others stay nominal.
curl -X POST http://localhost:4566/v1/ec2/spot-placement-scores \
  -d '{"region":"us-east-1","score":2}'

# One zone scores lower than its siblings. Observable only under
# SingleAvailabilityZone=true, since a region-scored answer names no zone.
curl -X POST http://localhost:4566/v1/ec2/spot-placement-scores \
  -d '{"availabilityZoneId":"use1-az2","score":1}'

# One instance type is scarce everywhere.
curl -X POST http://localhost:4566/v1/ec2/spot-placement-scores \
  -d '{"instanceType":"inf2.48xlarge","score":1}'

# Clear one scope, or all of them.
curl -X DELETE 'http://localhost:4566/v1/ec2/spot-placement-scores?region=us-east-1'
curl -X DELETE http://localhost:4566/v1/ec2/spot-placement-scores
```

A seed is scoped to one Availability Zone, one region, or every region, and to one instance type or
every instance type. **The most specific scope carrying a seed decides** — zone, then region, then
the wildcard — so seeding one bad zone inside an otherwise-seeded region works rather than being
overwritten by the coarser seed. Naming both a `region` and an `availabilityZoneId` is refused
rather than resolved by precedence: an AZ ID already fixes its region, which is why AWS reports the
ID here, so a seed naming a zone in a different region has no reading that is not a guess.

Within one scope, a request naming several instance types takes the **lowest** seeded score, and
only types that carry a seed are considered at all. Both halves follow from what a seed is for:
including unseeded types at their default would let the default mask a seed, and taking the maximum
would let a nominal sibling mask the scarce type a test seeded. It is also the reading closest to
AWS's own, whose score describes fulfilling the whole request rather than its easiest member.

`score` is required and refused outside **1 to 10**. That range is published as prose on the
operation — "scored on a scale from 1 to 10" — and **not** as a `Valid Range:` line on
`SpotPlacementScore.score`, so it is enforced here, where substrate owns the refusal, rather than
being asserted as a response invariant AWS guarantees.

Absent a seed, the answer follows the one relationship between a request and its score that AWS
publishes: "if you specify one or two instance types … the returned placement score will always be
low." So a request naming one or two types scores **3** and everything else scores **7**. Those two
numbers are substrate's reading, chosen inside the published scale and far enough apart for a test
to tell them apart — a 1 would claim there is no capacity anywhere and a 10 that fulfillment is
certain, and substrate models nothing that could know either. A request naming **no** instance types
scores nominally rather than low: naming none is legal (`InstanceType.N` publishes a minimum of 0
items) and has not met the documented condition, which is about specifying one or two rather than
about specifying few.

The answer is ordered by score descending, then by region and zone ID ascending. AWS describes it as
"the top 10 Regions or Availability Zones", which fixes the primary key; the tie-break is
substrate's, and it is what makes the answer reproducible when several scopes share a score — which
the page says they may. Without it, two runs of one request could order the same scores differently
and an assertion on the first element would be a coin toss.

**No `nextToken` is reachable here**, and that is a consequence of two AWS facts meeting rather than
a gap: `MaxResults` floors at 10, and substrate seeds three regions of three zones each, so the
largest answer it can build is nine items and no legal page size can truncate it. A caller looking
for a pagination loop to exercise will not find one; `NextToken` is still parsed and refused when
malformed, as at every other paginated EC2 describe.

`DryRun` is accepted and inert, as it is at every EC2 operation substrate routes.

## Seeding a Spot request outcome

A Spot launch is fulfilled by default. The branch a consumer's wait loop exists for — no capacity,
so the request stays open — is unreachable without a seed:

```bash
# Every Spot launch of p4d.24xlarge goes unfulfilled; other types are untouched.
curl -X POST http://localhost:4566/v1/ec2/spot-request-outcomes \
  -d '{"instanceType":"p4d.24xlarge","statusCode":"capacity-not-available"}'

# Every type, until cleared.
curl -X POST http://localhost:4566/v1/ec2/spot-request-outcomes -d '{"statusCode":"price-too-low"}'

# Clear one type, or every seed.
curl -X DELETE 'http://localhost:4566/v1/ec2/spot-request-outcomes?instanceType=p4d.24xlarge'
curl -X DELETE http://localhost:4566/v1/ec2/spot-request-outcomes
```

`statusCode` is `capacity-not-available` or `price-too-low` — the two status codes that leave a
request `open` with no instance. A seed for the instance type wins over the `"*"` one. While one
applies:

| Path | Answer |
|---|---|
| `RunInstances` with `MarketType=spot` | `InsufficientInstanceCapacity`, HTTP 500 — the server-error table's code, answered at the class it publishes, as [Capacity Reservation outcomes](#seeding-a-capacity-reservation-outcome) are |
| `RequestSpotInstances` | `InstanceCount` requests, `open`, with the seeded status and no `instanceId` |
| A persistent request's replacement | the request reopens — `open`, the seeded status, no `instanceId` |

An open request is fulfilled by the first `DescribeSpotInstanceRequests` after the seed that held it
is cleared — see [Spot Instance requests](#spot-instance-requests).

The seed is read at request time from its own namespace, so it is recorded and replayed with the
rest of the state.

## A Capacity Reservation is never consumed

`CreateCapacityReservation`, `DescribeCapacityReservations` and `CancelCapacityReservation`
answer as of [#891](https://github.com/scttfrdmn/substrate/issues/891); before that all three
reached the dispatcher's default arm and answered `InvalidAction`, so a consumer whose probe
primitive *is* an immediate reservation — reserve, read the outcome, cancel — could not run at
all.

A reservation is created `active`, which is what AWS's own examples show and what the User Guide
describes ("the reserved capacity becomes available for use immediately after you create it").
`pending` is reachable by [seeding](#seeding-a-capacity-reservation-outcome) rather than as a
stage every create passes through.

**Nothing consumes a reservation.** `RunInstances` has no `CapacityReservationTarget` parameter
here, so `availableInstanceCount` equals `totalInstanceCount` for as long as a reservation holds
capacity and no instance ever occupies one — including a `targeted` reservation, whose
`instanceMatchCriteria` is recorded and matches nothing. Only a **cancel** releases capacity, and
it takes `availableInstanceCount` to zero while `totalInstanceCount` keeps reporting what was
reserved; AWS publishes what neither becomes, and of the two readings this is the one that does not
report remaining capacity on a reservation that has none. A consumer testing "did my launch land
in the reservation I paid for" cannot ask that question here; what it *can* test is that the
reservation exists, reports the capacity it asked for, is discoverable by filter and by tag, and
reaches the state its outcome implies.

**A future-dated reservation is refused rather than answered falsely.** `StartDate` and
`CommitmentDuration` each answer `Unsupported` / 400. A future-dated reservation is a different
observable thing from an immediate one — it is assessed, then scheduled, then delivered or
delayed or unsupported — and eight of the `state` member's thirteen values exist only for one of
those or for a Capacity Block: `assessing`, `scheduled`, `delayed`, `unsupported`, `cancelling`,
`payment-pending`, `payment-failed`, and `unavailable`, which appears in the Valid Values line
with no prose description anywhere on the page. Answering `active` to a request for capacity two
days out would be a false observation with nothing in it to tell a caller the request was not
modelled. The code is *substrate's reading*: the operation's Errors section is the common-types
boilerplate, and `Unsupported` ("The specified request is unsupported") is the client-table code
whose gloss covers the shape.

Also absent, and answering `InvalidAction`: `ModifyCapacityReservation` — so a reservation's
`InstanceCount` and `EndDate` cannot be changed after it is created — `GetCapacityReservationUsage`,
`GetGroupsForCapacityReservation`, the four Capacity Reservation **fleet** operations, and
`PurchaseCapacityBlock`/`DescribeCapacityBlockOfferings`. `reservationType` is therefore always
`default`; no path here creates a `capacity-block`.

| Behaviour | Answer |
|---|---|
| `EndDateType` | **Inferred**, not defaulted. AWS publishes no `Default:` line and forbids exactly two combinations — `limited` with no `EndDate`, and `unlimited` with one — so an `EndDate` alone infers `limited` and no `EndDate` infers `unlimited`. Defaulting an absent `EndDateType` to `unlimited` would turn a request naming only an `EndDate`, which nothing forbids, into the second refusal |
| Expiry | Derived at observation time from the simulated clock, per `endDate`'s own sentence ("the Capacity Reservation's state changes to expired when it reaches its end date and time"), so it is assertable without depending on wall-clock time. Only an `active` reservation expires — a seeded `failed` one has already reached a terminal state, and reporting `expired` for it would lose the outcome the test seeded |
| A zone | `AvailabilityZone` and `AvailabilityZoneId` are both `Required: No` and the page states no rule on the pair, so a request naming **neither** is accepted and reports neither element. A zone *name* is recorded as given and never validated; a zone *ID* must resolve, because it has to be translated. A pair naming two different zones is `InvalidParameterCombination` |
| `ClientToken` | Accepted and inert. Two identical creates make two reservations, where AWS's token would make the second idempotent |
| Cancelling twice | `IncorrectState` / 400, which is **substrate's reading**. AWS publishes no code for cancelling from a non-cancellable state: the operation's Errors section is boilerplate, the only state-shaped code in the reference is `InvalidCapacityReservationState.PendingActivation` (a Capacity Block that is not active yet), and AWS's own sample tooling pre-checks the state through a describe rather than catching an error. `IncorrectState` is the code substrate already answers for this shape on a volume and a snapshot |
| Finding one by tag | Through `DescribeTags`, or `CreateTags`/`DeleteTags` on the `cr-` ID — **not** through a filter on the describe, which documents none, and **not** through Resource Groups Tagging, which has no scanner for the type |
| Member order | Alphabetical, from the `CapacityReservation` type page. None of the four pages publishes a sample response, so there is no rendered order to copy; the seven members a request can leave unset are **absent** rather than empty, including `tagSet` |

`InvalidCapacityReservationId.NotFound` and `InvalidCapacityReservationId.Malformed` are both in
EC2's client-error table, and the casing is AWS's: `Id` with a lowercase `d`, where the sibling
codes are `InvalidInstanceID.NotFound` and `InvalidAllocationID.NotFound`. The `NotFound`
*message* wording follows an observed AWS response rather than the table, which only describes
the condition — match on the code, not the string.

## Seeding a Capacity Reservation outcome

The reason a consumer reserves capacity at all is that capacity is finite, so the observations
worth testing are the ones where the request does not succeed: not enough capacity in the cell,
an On-Demand quota already spent, a Region or tenancy that cannot serve the instance type, a
throttled caller. Substrate models no capacity broker and no quota ledger, so none of those can
be derived from anything it knows.

**A capacity failure has two documented observable shapes**, and that is what shapes the seed.
Either the call fails outright with a code from EC2's error tables, or it succeeds and the
reservation reports a non-nominal `state` — AWS's own prose for `failed` is "A request can fail
due to request parameters that are not valid, capacity constraints, or instance limit
constraints." Which of the two AWS produces for a given cell is not documented, so a seed selects
one rather than substrate choosing on the caller's behalf, and a seed naming **both** is refused.

```bash
# One instance type is short everywhere.
curl -X POST http://localhost:4566/v1/ec2/capacity-reservation-outcomes \
  -d '{"instanceType":"p5.48xlarge","errorCode":"InsufficientInstanceCapacity"}'

# One zone cannot serve anything, with the caller's own message.
curl -X POST http://localhost:4566/v1/ec2/capacity-reservation-outcomes \
  -d '{"availabilityZone":"us-east-1a","errorCode":"Unsupported",
       "errorMessage":"that zone does not offer this configuration"}'

# The other shape: the create succeeds and the reservation reports a state.
curl -X POST http://localhost:4566/v1/ec2/capacity-reservation-outcomes \
  -d '{"instanceType":"g6.xlarge","availabilityZone":"us-east-1b","state":"failed"}'

# Clear one cell, or every seed.
curl -X DELETE 'http://localhost:4566/v1/ec2/capacity-reservation-outcomes?instanceType=g6.xlarge&availabilityZone=us-east-1b'
curl -X DELETE http://localhost:4566/v1/ec2/capacity-reservation-outcomes
```

A seed is scoped to an (instance type, Availability Zone) pair, because that pair is what a
consumer treats as one capacity cell, and either half may be omitted to mean "every". **The most
specific scope carrying a seed decides** — the exact cell, then the type in any zone, then any
type in the zone, then the wildcard — so seeding one scarce type inside an otherwise-seeded zone
works rather than being overwritten by the coarser seed. Type before zone is *substrate's
ordering*: a seed naming an instance type is a statement about that type's scarcity, which is the
narrower claim of the two.

| Seedable | Values |
|---|---|
| `errorCode` | `InsufficientInstanceCapacity`, `RequestLimitExceeded`, `InstanceLimitExceeded`, `VcpuLimitExceeded`, `Unsupported`. A code outside the five is **refused** rather than defaulted, because the HTTP class is the half substrate cannot derive |
| `state` | `active`, `pending`, `failed`, `expired`, `cancelled` — five of the thirteen. The other eight belong to a future-dated reservation or a Capacity Block, which substrate models neither of, so seeding one would publish an observation nothing else in the emulator is consistent with |
| `errorMessage` | Optional; falls back to AWS's own words for the code |

**Two of the five codes answer a 500-series status, and that is deliberate.** EC2's
errors-overview page puts `InsufficientInstanceCapacity` and `RequestLimitExceeded` in its
*server* error table, whose preamble says such errors "are accompanied by a 500-series HTTP
response code", while the other three are in the client table. A consumer reads
`InsufficientInstanceCapacity` as a capacity signal and would expect a 400; answering one would
let retry logic pass here and fail in production. The exact number is **unverified** — neither
page assigns one, only a class — so `500` and `400` are substrate's reading of "500-series" and
"400-series". The same page contradicts itself once, writing the throttle code as
`Client.RequestLimitExceeded` in prose while listing `RequestLimitExceeded` in the server table,
and `CommonErrors` does not list it at all; the server table is followed because it is the one
place the code appears in a table at all.

A seeded state is a property of the reservation the create writes, so it survives into every
later `DescribeCapacityReservations` — unlike a seeded error, which prevents the create from
writing anything at all.

The seed survives a replay, and getting that right matters more here than for a progression seed.
A progression seed diverges an *observation*, which the next call re-derives; this one is resolved
once at create time and **persisted**, so a replay that lost the seed would write a reservation
whose stored state was `active` and answer every later `DescribeCapacityReservations` from it. The
seed is recorded as an event and re-applied before the recorded `CreateCapacityReservation` is
re-executed — see [How a seed survives a replay](/reference/concepts/seeds-and-replay#how-a-seed-survives-a-replay).

## `ListSecrets` filters, sorts and hides pending deletions

Until [#1622](https://github.com/scttfrdmn/substrate/issues/1622) every entry was `ARN` and `Name`
alone, `Filters` was ignored, and a secret scheduled for deletion was listed. An entry now carries
what `DescribeSecret` reports for the secret, with the two differences `SecretListEntry` publishes:
the version map is `SecretVersionsToStages` (only versions carrying a label, and absent for a
secret with no version), and `RotationEnabled` appears only when rotation is configured, as the
page's sample entries show.

| Filter `Key` | Matches |
|---|---|
| `name`, `tag-key`, `tag-value` | A case-sensitive prefix of the name, of any tag key, or of any tag value |
| `description` | Every word of the value begins some word of the description, ignoring case |
| `all` | Any word of the value begins any word of the name, description, tag keys or tag values, ignoring case; words split where the "Find secrets" guide says (`credsDatabase#892` is `creds`, `database`, `892`) |
| `primary-region`, `owning-service` | Nothing: substrate models neither replication nor service-managed secrets |

A value beginning `!` is negated: it matches a secret none of whose strings for that key matches
the rest of the value. The values of one filter are alternatives and separate filters are all
required. The per-key rules are `API_Filter`'s; how values combine, how a negation treats a
multi-valued attribute, and the word rule for `description` are substrate's reading, since no page
states them. More than ten filters, an unknown key, a value list outside one to ten items, a value
over 512 characters or outside the published pattern, and an unknown `SortBy` or `SortOrder` are
`InvalidParameterException`/400, with messages that are substrate's own.

Secrets are listed by `CreatedDate`, as the page says, ascending unless `SortOrder` is `desc` (the
page publishes no default direction, so ascending is substrate's reading); `SortBy` may name
`last-changed-date`, `name` or `last-accessed-date`, the last of which every secret ties on because
`LastAccessedDate` is not modelled. Ties break by name. The list is filtered and sorted before it
is cut into pages, and `NextToken` names the last entry served — its sort key and name — so the
next page resumes strictly after it: a cleanup loop that deletes each page's secrets before
fetching the next reaches every secret, where an offset would have skipped as many as it deleted.
A token substrate did not issue is `InvalidNextTokenException`. A secret scheduled for deletion is listed only with `IncludePlannedDeletion`,
and carries `DeletedDate` when it is.

## A resource policy is stored, not enforced

`PutResourcePolicy`, `GetResourcePolicy`, `DeleteResourcePolicy` and `ValidateResourcePolicy`
([#1623](https://github.com/scttfrdmn/substrate/issues/1623)) store a policy on the secret and answer
it back as the text the caller submitted. **The policy takes no part in authorization**: every
operation, `GetSecretValue` included, is decided by the caller's identity policies alone. Evaluating
resource policies is tracked as [#1636](https://github.com/scttfrdmn/substrate/issues/1636).

| Request | Answer |
|---|---|
| A policy that is not JSON, not an object (`null` included), has no `Statement` or an empty one, or whose `Statement` is not a statement or a list of them | `MalformedPolicyDocumentException`/400 — the IAM policy grammar marks `<statement_block>` required |
| No `ResourcePolicy`, or one over 20,480 characters | `InvalidParameterException`/400 |
| `PolicyName` other than `default` on `PutResourcePolicy` or `DeleteResourcePolicy` | `InvalidParameterException`/400, as both pages state |
| `PolicyName` other than `default` on `GetResourcePolicy` | `ResourceNotFoundException`/400, as that page states |
| `BlockPublicPolicy` with a policy granting broad access | `PublicPolicyException`/400, nothing stored. An absent secret is `ResourceNotFoundException` and a scheduled one `InvalidRequestException` first, whatever the policy grants |
| `ValidateResourcePolicy` with a policy granting broad access | `200`, `PolicyValidationPassed: false`, one `ValidationErrors` entry |
| `ValidateResourcePolicy` with a policy that passes | `200`, `PolicyValidationPassed: true`, `ValidationErrors: []` |

AWS decides "broad access" with Zelkova, whose rules are unpublished. Substrate uses the definition
S3 Block Public Access publishes: an `Allow` statement is broad unless its principal names only
fixed values or a condition pins a principal- or source-narrowing key (`aws:PrincipalArn`,
`aws:PrincipalAccount`, `aws:SourceVpc` and the others) to fixed values under a positive operator —
see [Block Public Access](/reference/s3#block-public-access); `StringNotEquals`, an `...IfExists` form or `Null`
narrows nothing. S3's own `s3:DataAccessPoint*` keys do not qualify a secret's policy. A `Deny` is
never broad, so a "deny everyone but this role" policy passes. That definition, the `CheckName` value
(`BroadAccessCheck`) and the error messages are substrate's own. The third check
`ValidateResourcePolicy` publishes, that the policy "does not lock out a caller", is not modelled.

## The account and Region a record carries reach no response

Every EC2 record in `emulator/ec2_types.go`, `emulator/ec2_capacity_reservations.go` and
`emulator/ec2_fleet.go` declares `AccountID` and `Region` under wire-visible
`json` tags, because the record is what `MemoryStateManager` snapshots and a replay reads back. No EC2
shape publishes either, and neither reaches a body. The records carry no `xml` tag. Every response is
marshaled from a struct declared for its operation, no XML-tagged field anywhere is typed as a record,
and no record is embedded in a response or held behind an interface. So the projection already exists
in code, and what each record needs is a test that proves it.

All sixteen records have one, across **eighty-four** subtests in `emulator/ec2_wire_test.go`, one
test per record:

- **Networking:** `EC2VPC`, `EC2Subnet`, `EC2SecurityGroup`, `EC2InternetGateway`, `EC2RouteTable`,
  `EC2NATGateway` and `EC2ElasticIP`.
- **Capacity:** `EC2CapacityReservation` and `EC2Fleet`.
- **Storage and images:** `EC2Volume`, `EC2Snapshot` and `EC2Image`.
- **Compute:** `EC2Instance`, `EC2KeyPair`, `EC2LaunchTemplate` and `EC2PlacementGroup`.

Each test walks the raw XML and fails on an element named for a bookkeeping member at any depth. As
a result, all thirty-four of EC2's entries in `scripts/wire-bookkeeping-baseline.txt` stay listed as
declarations rather than as leaks ([#756](https://github.com/scttfrdmn/substrate/issues/756)). Each
describe runs where its record is richest: a security group after its rules are authorized, a gateway
while attached, an address while associated, a volume while attached.

The account member is spelled three ways across the records: `account_id` on most, `accountId` on
`EC2KeyPair` and `accountID` on `EC2LaunchTemplate`. A case fold reconciles the last two with the Go
name `AccountID`, but not the snake_case form, so the test names that form explicitly. A control run
without it lets a leaked `<account_id>` pass. The comparison is an equality on the element *name*,
because the account also appears in published values such as `ownerId`.

Each case asserts a published element is *present* before asserting a bookkeeping one is absent. This
means a response that failed to render the record fails as a missing anchor rather than passing as an
absence. The stored record is also read back as raw JSON, which proves it held each member in the
first place. That read-back matters most for the two records with a third member under `,omitempty`,
which a response could otherwise be missing for free:

- `EC2Instance.EverTagged` is set only by a tag write (#938). The instance test therefore calls
  `CreateTags` and reads `ever_tagged: true` back before walking any response. A control without the
  tag write fails at that read-back, so the vacuous assertion #1304 shipped on EFS cannot recur here.
- `EC2KeyPair.CreatedAt` is written on both create paths and read into the published `createTime`.
  The key-pair test reads it back and anchors `DescribeKeyPairs` on `<createTime>`.

Several operations write a record of their own beside the one a test starts from: `CreateSnapshots`,
`CopySnapshot`, `RegisterImage`, `ImportKeyPair` and the instances `CreateFleet` launches. Each record
a test reads back is proved scoped. The image test starts from `CreateImage` rather than a bundled
catalog AMI, because a catalog entry is not a record the account wrote.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::EC2::VPC | VpcId | `Fn::GetAtt` answers `VpcId`, `CidrBlock` and `DefaultSecurityGroup`. `CidrBlockAssociations`, `DefaultNetworkAcl`, `Ipv6CidrBlocks` and every `VpcEncryptionControl.*` member are refused with the reason — EC2 records no CIDR association IDs, network ACLs, IPv6 blocks or encryption control ([#1591](https://github.com/scttfrdmn/substrate/issues/1591)) |
| AWS::EC2::Subnet | SubnetId | `Fn::GetAtt` answers `SubnetId`, `VpcId`, `CidrBlock`, `AvailabilityZone` and `AvailabilityZoneId`. `Ipv6CidrBlocks`, `NetworkAclAssociationId`, `OutpostArn` and `BlockPublicAccessStates.*` are refused with the reason (#1591) |
| AWS::EC2::NatGateway | NatGatewayId | `Fn::GetAtt NatGatewayId` answers; `EniId` (no network interface is created) and the regional-NAT attributes `RouteTableId`, `AutoProvisionZones` and `AutoScalingIps` are refused with the reason (#1591) |
| AWS::EC2::SecurityGroup | GroupId | Inline `SecurityGroupIngress`/`SecurityGroupEgress` rules are authorized |
| AWS::EC2::SecurityGroupIngress | GroupId | Standalone rule; resolves `SourceSecurityGroupId` through `Ref`/`GetAtt`, so self- and mutually-referencing groups work |
| AWS::EC2::SecurityGroupEgress | GroupId | Standalone rule; supports `DestinationSecurityGroupId` |
| AWS::EC2::Instance | InstanceId | Passes through `IamInstanceProfile`, `KeyName`, and `SecurityGroupIds` |
| AWS::EC2::InternetGateway | InternetGatewayId | |
| AWS::EC2::Route | `{RouteTableId}\|{destination}` | `CreateRoute` with the destination and the one target property — every target `CreateRoute` takes, `CoreNetworkArn` and `OdbNetworkArn` included; a changed target is `ReplaceRoute` in place, and a changed table or destination replaces the route. `Fn::GetAtt CidrBlock` answers the destination. Deleted through `DeleteRoute` (#1605) |
| AWS::EC2::SubnetRouteTableAssociation | the first association ID | `AssociateRouteTable`; a changed `RouteTableId` is `ReplaceRouteTableAssociation` on the subnet's live association, and a changed `SubnetId` replaces the association. The physical ID stays the association the create made, so a failed update rolls the subnet back onto its table in place; `Ref` and `Fn::GetAtt Id` answer the live association ID. Deleted through `DisassociateRouteTable` (#1605) |
| AWS::EC2::LaunchTemplate | LaunchTemplateId | Ref is the real `lt-…` ID, usable by `CreateFleet`. `LaunchTemplateData` forwards `ImageId`, `InstanceType`, `KeyName`, `UserData`, `SecurityGroupIds`, `DisableApiTermination` and the five `MetadataOptions` members — `HttpTokens`, `HttpPutResponseHopLimit`, `HttpEndpoint`, `HttpProtocolIpv6`, `InstanceMetadataTags` — with intrinsics resolved and the hop limit sent as an integer; a value EC2 refuses fails the resource with EC2's code ([#1508](https://github.com/scttfrdmn/substrate/issues/1508)). `Fn::GetAtt` answers `LaunchTemplateId`, `DefaultVersionNumber` and `LatestVersionNumber`. On `UpdateStack` a changed `LaunchTemplateData` adds a version through `CreateLaunchTemplateVersion`: the ID stays, `LatestVersionNumber` follows the new version, and `DefaultVersionNumber` stays, since "the default version of a launch template cannot be specified in AWS CloudFormation". A changed `LaunchTemplateName` replaces the template; a change to `TagSpecifications` or `VersionDescription` alone makes no call |

## Cost

EC2 instance costs approximate on-demand pricing for the instance type.
