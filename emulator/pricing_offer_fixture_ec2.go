package emulator

// This file holds the AmazonEC2 half of the offer corpus the Price List Query API
// serves (#894). The AmazonS3 half, and the types and assembly both halves share,
// are in pricing_offer_fixture.go.
//
// Every SKU, attribute value, rate code, offer-term code, description and price
// string below is copied from the live AWS offer files
// https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/{us-east-1,us-west-2,eu-west-1}/index.json
// (version 20260910195514, publicationDate 2026-09-10T19:55:14Z), each downloaded
// byte-complete and cross-checked against the pricing-page feed, which agrees to
// all ten decimal places. Nothing here is invented — a plausible-looking rate is
// worse than no rate, because a consumer computing a cost from it would be wrong
// and have no way to notice.
//
// The slice is thirty-two SKUs rather than the 107,022 compute-instance products
// us-east-1 publishes, and each one is here to exhibit a shape a caller has to
// handle:
//
//   - The seven-filter recipe for "one on-demand rate" (regionCode, instanceType,
//     operatingSystem, tenancy, preInstalledSw, capacitystatus, marketoption) is
//     NOT sufficient. Three us-east-1 m5.xlarge Windows SKUs satisfy all seven and
//     share one usagetype, differing only in licenseModel and operation — $0.376
//     with a license, $0.192 without one and $0.192 BYOL. licenseModel is an eighth
//     discriminator, and a caller that takes the first of several rates picks one of
//     those three arbitrarily.
//   - usagetype is NOT 1:1 with a SKU here, unlike S3, where the corpus's own
//     preamble calls it "reliably present and 1:1". Four SKUs in this slice share
//     BoxUsage:m5.xlarge.
//   - marketoption matters even at one instance type: p5.48xlarge publishes an
//     on-demand rate of $55.04 and a Capacity Block rate of $0.00. A query that
//     omits marketoption can report a free p5.
//   - capacitystatus and tenancy each change the usagetype token rather than only
//     the price: UnusedBox: and DedicatedUsage: — so a caller keyed on
//     "BoxUsage:" silently misses them.
//   - usagetype is region-prefixed, and the prefix is not derivable from the Region
//     code: us-east-1 has none, us-west-2 uses USW2-, and eu-west-1 uses the legacy
//     EU-, not EUW1-.
//   - us-west-2's rates are byte-identical to us-east-1's for every type here EXCEPT
//     p4d.24xlarge, which is 21.9576420000 in one and 21.9576400000 in the other. A
//     consumer that assumes two Regions agreeing on nine rates agree on the tenth is
//     wrong at the seventh decimal place.
//   - eu-west-1 is a per-FAMILY premium, not one Region multiplier: p4d ~1.080,
//     inf2 exactly 1.250, m5 ~1.115, t3 ~1.096. Deriving one Region's price from
//     another's would be wrong by up to 15%.
//   - eu-west-1 publishes NO g6, p5 or trn1 compute products at all, so the absence
//     of a rate is a real observation rather than a gap in this corpus. trn2 is
//     absent from all three Regions at every OS and tenancy, which is why no trn2
//     rate appears here: a real query returns nothing for it.
//   - The free-tier pseudo-product falsifies five invariants a parser is likely to
//     assume: it carries no instanceType, its endRange is "750" rather than "Inf",
//     its offerTermCode is not the global on-demand code, its termAttributes is
//     non-empty and its appliesTo lists 170 SKUs. Its location is "Any" and its
//     regionCode is the EMPTY STRING, so a regionCode filter never selects it.
//
// The physical attributes of an instance type — vcpu, memory, clockSpeed and the
// rest — are byte-identical across all three Regions; only location, regionCode and
// usagetype vary. pricingEC2TypeAttrs therefore records them once per type and each
// SKU overrides what actually differs. That factoring was verified against the three
// files rather than assumed, and it is the only structural liberty taken with the
// data: every value it composes is still the file's own.

// pricingServiceCodeEC2 is the Price List service code for Amazon EC2. It is not
// the same string as substrate's own "ec2" service name, nor as the "ec2" that
// awsServiceCodeMap uses for cost estimation.
const pricingServiceCodeEC2 = "AmazonEC2"

// pricingEC2OfferVersion and pricingEC2OfferPublicationDate identify the offer
// file revision the EC2 slice was taken from. EC2 publishes on its own schedule,
// so these differ from the S3 pair.
const (
	pricingEC2OfferVersion         = "20260910195514"
	pricingEC2OfferPublicationDate = "2026-09-10T19:55:14Z"
)

// pricingEC2ComputeInstance is the productFamily every compute-instance product
// carries. Note bare-metal instances are a *separate* family,
// "Compute Instance (bare metal)", 13,273 of them in us-east-1 — so a caller
// filtering productFamily="Compute Instance" does not see a .metal type.
const pricingEC2ComputeInstance = "Compute Instance"

// pricingEC2FreeTierTermCode is the offer-term code of the free-tier
// pseudo-product, the one compute-instance term in the offer file that is not the
// global on-demand code.
const pricingEC2FreeTierTermCode = "A429C66SYZ"

// pricingEC2FreeTierAppliesTo is the free-tier term's appliesTo list, verbatim: 170
// SKUs, none of them in this corpus. Every other on-demand dimension AWS emits has
// an empty appliesTo, so a parser that assumes it is always empty breaks here.
//
//nolint:gochecknoglobals // Immutable reference data, read-only after init.
var pricingEC2FreeTierAppliesTo = []string{
	"DBYKKKJW4DNWYZYV", "UZN5PC673JE8UQAY", "V9ES3NHTPE38KTTM", "7UWG6NFD2Q2CCF66",
	"DF3Y67YMPD9W4AFA", "3EP9U6T67W776A2U", "X8TPC7DRNBGBDTD5", "RDAZEH7YEHSHKUD6",
	"PPJSGTBSZ3M9G2FX", "HWE4GVVD4XAA5MSM", "74TFX697KNZ72UYS", "VP27QTDUG3CMNEE5",
	"W3AKEE2UNER2XVES", "UMK5V76RXY4XK9TC", "NWKYPJ3YZ3GD7XM5", "AW8AKUMZKQEQXMSZ",
	"W9THJ2D5XUV5A94B", "T5VTWB5QZT27MBZM", "4X9G6PSGEMMQABBA", "FY5UR93C9YDGQB44",
	"7XMF22UUB93ZJWBC", "8N6354P9QMEF7B6P", "URT2Z2UDMH6K6RV9", "T8ZUNBWNHPNKB3UR",
	"UWBEBPUFFV98TS9H", "MZGPYWV687EP4MDV", "439RFSHM9QE8FCKH", "JT35QQ22RYW6BPH2",
	"UJ5B5PTS4YCEN5XQ", "JJTHVE46DG4C44QU", "EG26BW4WNM66NNWJ", "STAAM4PYPPV66BJW",
	"EMEMYDS7QQEW8P8C", "KRDSWRBFH2J78M7D", "YXT9DDZEEM54DM2V", "QC5GYPK39D34P95P",
	"WU7XTWSVJYUDM6F5", "DTXWM5V9G5PY96GJ", "AV6NEG9K8KQJW3UY", "URTSFJQ3WUEG9FKX",
	"QRRYV68EAVPN8NW8", "FF2ERQV9SEHTV67D", "5A5N85QM5DPGS7TP", "8VCCAUF9CS4ABHNP",
	"8RXUUFRB6UFXAZDN", "NCEAAS5TNBGVUJ8A", "SFA2QGU9SZF5R9UZ", "8737UU57M2Z32WTB",
	"FSQDFZRKMZENV4E3", "QEVDH7QZ6RBJ92DP", "2BH2BSYGAMS6R9B6", "H5ESZHQF82SSEKVQ",
	"DY352HXFHSH2RGZR", "DYYA7GU8K4R78XWU", "3JPDVFFT6R7SW2PF", "KGA2J6AT8V7GC64C",
	"NZEUF49W57PGVFS6", "SX337VBYBU8Q9XFW", "73T97BE3T4S4DRNQ", "YUHU2BJZFXTP6QZB",
	"DKGAEQE4UWT4FX5N", "VD7E65KAUCKRD5SQ", "CH9K3BSKEQHD3T6Q", "NEJV9WSDCZV9HBPF",
	"RZ9JQ7CAC28ESM2W", "9GWX7R3VJUG9P8Y8", "KJ27N64UWW3XVKZU", "Y2QU9XB9ZZXS49VK",
	"SX6CR62YX8MFV28J", "4AG434AZP7VKSWQJ", "JKPG8YAXFCXAHHRY", "PNC6BYDCU4XPA59S",
	"S8WVRUZKU3K8HB7F", "BW246H76QU44M7NT", "3GPNV2DGQVM7GCSE", "MXYK6NDY6RJ3X8DP",
	"NZ8VFCUDV72C6YXE", "YTWJMDCAHC545GC9", "8BQFP8RFZFJ5Z7PV", "VXMAC6YG3V4XTE4K",
	"MPVEJW2KM6RUN7YB", "5X9GFS6MGWXD2HDN", "BNV8HMV6JWGKUWNM", "N2TD7AEUCAM58XN6",
	"KF5NMDBAT5KEFX66", "WPA36YFSG5C5F3FP", "N4C4SJ9BKHMY4ZAP", "5HCA3UTTUJNXTSMA",
	"RU9HAXN7CEU7UDZH", "CPT3F6JYW6467H35", "CJFYYA86JX4BYK6B", "2VE8QJYGZYA97HTR",
	"ZUNAW6MWT9WP3SKE", "DDDHNVR6Z5GKWVAN", "6PFHQYKYB5P54UAH", "BVPR77QTGQK4YT64",
	"QFRFZ55VTRDDHUJ7", "5S26F766N9R2RBXH", "ZMTPUYRJYZHC3VNU", "GH4MSPTMAVUBB54F",
	"KGAK5KP2ZA2QADFR", "3TK7G58X4TBK9FC6", "US7FNJHF5HP65KJJ", "6YTHGB69EBGBBE36",
	"8DECM9VMCA6DG3QS", "RGUCF5S3GNXWY8ZM", "92ATHBJX3WVG89ED", "E43C62CQKTZTM226",
	"RWFWMUM36Q87DK9Y", "4H9A3VT7J9EUCWR2", "T2WR8U3PGF27NURH", "8DU2N9W7GJJW2SSM",
	"QHFSYDQF5ECSSXW2", "QSPG47MGG65Q7VAP", "A8NMJAWDY6TWYRKJ", "VDTA589FZ6WZDGGY",
	"NJB9XQVGDRDCWJES", "8Z47T9HF9HRTYHQG", "FA5WEF72YX7K9VTB", "HME7AS7GXR6D4SFE",
	"2EUSE2TWPQ8MGRAB", "HHXC7H33CCA6NE9V", "QGV2TDH8S8XUDBZS", "VJP8YV2GB9QAKWRM",
	"Z7RKKZ93UYWRHMR2", "KAVERF7SY9M6HVT8", "RG9BV68SHERXD4CG", "CEUWNE94JVK96DP4",
	"42DP2UNY4ASHPCXT", "C7S72Q5AZ5GBK63Y", "J3S8YXERW39SBAFR", "Q2UK5BJWR749WHMQ",
	"4RSE4747CXAQPWAF", "PEU367QMBGZM3E4G", "6U8RDQ34D8WNV9QC", "M8D727XBUFCN2UT4",
	"KEQ4TT4FH5GJJNKE", "AMVKUFQ62BCYN86F", "HBHD53EFB2TH5Y75", "EV8CKVMEBCDNZ2QJ",
	"DTMNV2VDHCZYXYUC", "NPWNDS575NVCNR45", "BWJ32GT2BUJS2EU3", "AMHUY3T3XH6EV5VT",
	"VFBE34F268RM74KS", "DDMH422TCWDC97DM", "TUW8QBQFHTFCZU8G", "H28UXFKY3YU6BAP9",
	"GU72BKBRUEXEMCC4", "VM5GJSA24GYXQKQT", "G9BHS6Z826CEVCEH", "X5D4MCRYHAAS96V9",
	"RX5QV3HCGYQXTEEJ", "9R2FWZKEGC2AH9RK", "Q3K2B43DGK46F4WX", "YM2XN39USBQN9AZ5",
	"F7MEBXG8K3CC9TUA", "UKSCXQDZVRTAPHPX", "PQD9GJ3WWBX6N7WN", "WKYSXK36735QREZU",
	"9QZEPSPF8754RCGP", "RHT3EEFB3SMCKG6Q", "5MC64VBKS57PQXKB", "HDWKYG7M5GHVE2XV",
	"XBH2AVV8HN8P54U2", "NTSJZ6S2KD2YFRVB", "Y33CHC2PYKBQ7U2C", "GW3KYCFUMG7AZRN8",
	"54Y6FQY599Z6GQ5J", "WNXKXP7HYA824F76",
}

// pricingEC2TypeAttrs holds the attributes an instance type carries in every
// Region for the nominal Linux/Shared/Used/on-demand product: the physical facts
// plus the discriminators that product happens to have. location, regionCode and
// usagetype are deliberately absent, because those are the only three that vary by
// Region, and each corpus entry supplies its own.
//
//nolint:gochecknoglobals // Immutable reference data, read-only after init.
var pricingEC2TypeAttrs = map[string]map[string]string{
	"t3.micro": {
		"availabilityzone": "NA", "capacitystatus": "Used",
		"classicnetworkingsupport": "false", "clockSpeed": "3.1 GHz",
		"currentGeneration": "Yes", "dedicatedEbsThroughput": "Up to 2085 Mbps",
		"dedicatedEbsThroughputDescription": "87 Mbps", "ecu": "NA",
		"enhancedNetworkingSupported": "No", "gpuMemory": "NA",
		"instanceFamily": "General purpose", "instanceFamilyCategory": "General Purpose",
		"instanceType": "t3.micro", "intelAvx2Available": "Yes",
		"intelAvxAvailable": "Yes", "intelTurboAvailable": "Yes",
		"licenseModel": "No License required", "locationType": "AWS Region",
		"marketoption": "OnDemand", "memory": "1 GiB",
		"networkPerformance": "Up to 5 Gigabit", "normalizationSizeFactor": "0.5",
		"operatingSystem": "Linux", "operation": "RunInstances",
		"physicalProcessor": "Intel Skylake E5 2686 v5", "preInstalledSw": "NA",
		"processorArchitecture": "64-bit", "processorFeatures": "AVX; AVX2; Intel AVX; Intel AVX2; Intel AVX512; Intel Turbo",
		"servicecode": pricingServiceCodeEC2, "servicename": "Amazon Elastic Compute Cloud",
		"storage": "EBS only", "tenancy": "Shared",
		"vcpu": "2", "vpcnetworkingsupport": "true",
	},
	"m5.xlarge": {
		"availabilityzone": "NA", "capacitystatus": "Used",
		"classicnetworkingsupport": "false", "clockSpeed": "3.1 GHz",
		"currentGeneration": "Yes", "dedicatedEbsThroughput": "Up to 2120 Mbps",
		"dedicatedEbsThroughputDescription": "850 Mbps", "ecu": "16",
		"enhancedNetworkingSupported": "Yes", "gpuMemory": "NA",
		"instanceFamily": "General purpose", "instanceFamilyCategory": "General Purpose",
		"instanceType": "m5.xlarge", "intelAvx2Available": "Yes",
		"intelAvxAvailable": "Yes", "intelTurboAvailable": "Yes",
		"licenseModel": "No License required", "locationType": "AWS Region",
		"marketoption": "OnDemand", "memory": "16 GiB",
		"networkPerformance": "Up to 10 Gigabit", "normalizationSizeFactor": "8",
		"operatingSystem": "Linux", "operation": "RunInstances",
		"physicalProcessor": "Intel Xeon Platinum 8175", "preInstalledSw": "NA",
		"processorArchitecture": "64-bit", "processorFeatures": "Intel AVX; Intel AVX2; Intel AVX512; Intel Turbo",
		"servicecode": pricingServiceCodeEC2, "servicename": "Amazon Elastic Compute Cloud",
		"storage": "EBS only", "tenancy": "Shared",
		"vcpu": "4", "vpcnetworkingsupport": "true",
	},
	"g4dn.xlarge": {
		"availabilityzone": "NA", "capacitystatus": "Used",
		"classicnetworkingsupport": "false", "clockSpeed": "2.5 GHz",
		"currentGeneration": "Yes", "dedicatedEbsThroughput": "3500 Mbps",
		"dedicatedEbsThroughputDescription": "3500 Mbps", "ecu": "NA",
		"enhancedNetworkingSupported": "Yes", "gpu": "1",
		"gpuMemory": "16 GB", "instanceFamily": "GPU instance",
		"instanceFamilyCategory": "Accelerated Computing", "instanceType": "g4dn.xlarge",
		"intelAvx2Available": "Yes", "intelAvxAvailable": "Yes",
		"intelTurboAvailable": "Yes", "licenseModel": "No License required",
		"locationType": "AWS Region", "marketoption": "OnDemand",
		"memory": "16 GiB", "networkPerformance": "Up to 25 Gigabit",
		"normalizationSizeFactor": "1", "operatingSystem": "Linux",
		"operation": "RunInstances", "physicalProcessor": "Intel Xeon Family",
		"preInstalledSw": "NA", "processorArchitecture": "64-bit",
		"processorFeatures": "Intel AVX; Intel AVX2; Intel AVX512; Intel Turbo", "servicecode": pricingServiceCodeEC2,
		"servicename": "Amazon Elastic Compute Cloud", "storage": "125 GB NVMe SSD",
		"tenancy": "Shared", "vcpu": "4",
		"vpcnetworkingsupport": "true",
	},
	"g5.2xlarge": {
		"availabilityzone": "NA", "capacitystatus": "Used",
		"classicnetworkingsupport": "false", "clockSpeed": "2.8 GHz",
		"currentGeneration": "Yes", "dedicatedEbsThroughput": "Up to 3500 Mbps",
		"dedicatedEbsThroughputDescription": "850 Mbps", "ecu": "NA",
		"enhancedNetworkingSupported": "Yes", "gpu": "1",
		"gpuMemory": "24 GB", "instanceFamily": "GPU instance",
		"instanceFamilyCategory": "Accelerated Computing", "instanceType": "g5.2xlarge",
		"intelAvx2Available": "No", "intelAvxAvailable": "No",
		"intelTurboAvailable": "No", "licenseModel": "No License required",
		"locationType": "AWS Region", "marketoption": "OnDemand",
		"memory": "32 GiB", "networkPerformance": "Up to 10 Gigabit",
		"normalizationSizeFactor": "1.204771372", "operatingSystem": "Linux",
		"operation": "RunInstances", "physicalProcessor": "AMD EPYC 7R32",
		"preInstalledSw": "NA", "processorArchitecture": "64-bit",
		"processorFeatures": "AMD Turbo; AVX; AVX2", "servicecode": pricingServiceCodeEC2,
		"servicename": "Amazon Elastic Compute Cloud", "storage": "1 x 450 GB NVMe SSD",
		"tenancy": "Shared", "vcpu": "8",
		"vpcnetworkingsupport": "true",
	},
	"g6.xlarge": {
		"availabilityzone": "NA", "capacitystatus": "Used",
		"classicnetworkingsupport": "false", "clockSpeed": "2.6 GHz",
		"currentGeneration": "Yes", "dedicatedEbsThroughput": "Up to 5000 Mbps",
		"dedicatedEbsThroughputDescription": "1000 Mbps", "ecu": "NA",
		"enhancedNetworkingSupported": "Yes", "gpu": "1",
		"gpuMemory": "24 GB", "instanceFamily": "GPU instance",
		"instanceFamilyCategory": "Accelerated Computing", "instanceType": "g6.xlarge",
		"intelAvx2Available": "No", "intelAvxAvailable": "No",
		"intelTurboAvailable": "No", "licenseModel": "No License required",
		"locationType": "AWS Region", "marketoption": "OnDemand",
		"memory": "16 GiB", "networkPerformance": "Up to 10 Gigabit",
		"normalizationSizeFactor": "1", "operatingSystem": "Linux",
		"operation": "RunInstances", "physicalProcessor": "AMD EPYC 7R13 Processor",
		"preInstalledSw": "NA", "processorArchitecture": "64-bit",
		"processorFeatures": "AMD Turbo; AVX; AVX2", "servicecode": pricingServiceCodeEC2,
		"servicename": "Amazon Elastic Compute Cloud", "storage": "1 x 250 GB NVMe SSD",
		"tenancy": "Shared", "vcpu": "4",
		"vpcnetworkingsupport": "true",
	},
	"inf2.xlarge": {
		"availabilityzone": "NA", "capacitystatus": "Used",
		"classicnetworkingsupport": "false", "clockSpeed": "2.95 GHz",
		"currentGeneration": "Yes", "dedicatedEbsThroughput": "Up to 10 Gbps",
		"dedicatedEbsThroughputDescription": "1250 Mbps", "ecu": "NA",
		"enhancedNetworkingSupported": "Yes", "gpu": "1",
		"gpuMemory": "32 GB", "instanceFamily": "Machine Learning ASIC Instances",
		"instanceFamilyCategory": "Accelerated Computing", "instanceType": "inf2.xlarge",
		"intelAvx2Available": "No", "intelAvxAvailable": "No",
		"intelTurboAvailable": "No", "licenseModel": "No License required",
		"locationType": "AWS Region", "marketoption": "OnDemand",
		"memory": "16 GiB", "networkPerformance": "Up to 15 Gigabit",
		"normalizationSizeFactor": "1", "operatingSystem": "Linux",
		"operation": "RunInstances", "physicalProcessor": "AMD EPYC 7R13 Processor",
		"preInstalledSw": "NA", "processorArchitecture": "64-bit",
		"processorFeatures": "AMD Turbo; AVX; AVX2", "servicecode": pricingServiceCodeEC2,
		"servicename": "Amazon Elastic Compute Cloud", "storage": "EBS only",
		"tenancy": "Shared", "vcpu": "4",
		"vpcnetworkingsupport": "true",
	},
	"p4d.24xlarge": {
		"availabilityzone": "NA", "capacitystatus": "Used",
		"classicnetworkingsupport": "false", "clockSpeed": "3 GHz",
		"currentGeneration": "Yes", "dedicatedEbsThroughput": "19000 Mbps",
		"dedicatedEbsThroughputDescription": "19000 Mbps", "ecu": "345",
		"enhancedNetworkingSupported": "No", "gpu": "8",
		"gpuMemory": "320 GB HBM2", "instanceFamily": "GPU instance",
		"instanceFamilyCategory": "Accelerated Computing", "instanceType": "p4d.24xlarge",
		"intelAvx2Available": "Yes", "intelAvxAvailable": "Yes",
		"intelTurboAvailable": "Yes", "licenseModel": "No License required",
		"locationType": "AWS Region", "marketoption": "OnDemand",
		"memory": "1152 GiB", "networkPerformance": "400 Gigabit",
		"normalizationSizeFactor": "192", "operatingSystem": "Linux",
		"operation": "RunInstances", "physicalProcessor": "Intel Xeon Platinum 8275L",
		"preInstalledSw": "NA", "processorArchitecture": "64-bit",
		"processorFeatures": "Intel AVX; Intel AVX2; Intel AVX512; Intel Turbo", "servicecode": pricingServiceCodeEC2,
		"servicename": "Amazon Elastic Compute Cloud", "storage": "8 x 1000 SSD",
		"tenancy": "Shared", "vcpu": "96",
		"vpcnetworkingsupport": "true",
	},
	"p5.48xlarge": {
		"availabilityzone": "NA", "capacitystatus": "Used",
		"classicnetworkingsupport": "false", "clockSpeed": "2.95 GHz",
		"currentGeneration": "Yes", "dedicatedEbsThroughput": "80 Gbps",
		"dedicatedEbsThroughputDescription": "80 Gbps", "ecu": "NA",
		"enhancedNetworkingSupported": "Yes", "gpu": "8",
		"gpuMemory": "640 GB HBM3", "instanceFamily": "GPU instance",
		"instanceFamilyCategory": "Accelerated Computing", "instanceType": "p5.48xlarge",
		"intelAvx2Available": "No", "intelAvxAvailable": "No",
		"intelTurboAvailable": "No", "licenseModel": "No License required",
		"locationType": "AWS Region", "marketoption": "OnDemand",
		"memory": "2048 GiB", "networkPerformance": "3200 Gigabit",
		"normalizationSizeFactor": "384", "operatingSystem": "Linux",
		"operation": "RunInstances", "physicalProcessor": "AMD EPYC 7R13 Processor",
		"preInstalledSw": "NA", "processorArchitecture": "64-bit",
		"processorFeatures": "AMD Turbo; AVX; AVX2", "servicecode": pricingServiceCodeEC2,
		"servicename": "Amazon Elastic Compute Cloud", "storage": "8 x 3840 GB SSD",
		"tenancy": "Shared", "vcpu": "192",
		"vpcnetworkingsupport": "true",
	},
	"trn1.32xlarge": {
		"availabilityzone": "NA", "capacitystatus": "Used",
		"classicnetworkingsupport": "false", "clockSpeed": "Up to 3.5 GHz",
		"currentGeneration": "Yes", "dedicatedEbsThroughput": "Up to 80 Gbps",
		"dedicatedEbsThroughputDescription": "80 Gbps", "ecu": "NA",
		"enhancedNetworkingSupported": "Yes", "gpu": "16",
		"gpuMemory": "512 GB", "instanceFamily": "Machine Learning ASIC Instances",
		"instanceFamilyCategory": "Accelerated Computing", "instanceType": "trn1.32xlarge",
		"intelAvx2Available": "Yes", "intelAvxAvailable": "Yes",
		"intelTurboAvailable": "Yes", "licenseModel": "No License required",
		"locationType": "AWS Region", "marketoption": "OnDemand",
		"memory": "512 GiB", "networkPerformance": "800 Gigabit",
		"normalizationSizeFactor": "256", "operatingSystem": "Linux",
		"operation": "RunInstances", "physicalProcessor": "Intel Xeon Scalable (Icelake)",
		"preInstalledSw": "NA", "processorArchitecture": "64-bit",
		"processorFeatures": "Intel AVX; Intel AVX2; Intel AVX512; Intel Turbo", "servicecode": pricingServiceCodeEC2,
		"servicename": "Amazon Elastic Compute Cloud", "storage": "4 x 1900 NVMe SSD",
		"tenancy": "Shared", "vcpu": "128",
		"vpcnetworkingsupport": "true",
	},
}

// pricingEC2Attrs composes one product's attributes from its instance type's
// Region-invariant set and the values that differ for this SKU. It returns a fresh
// map, so a caller cannot mutate the shared base.
func pricingEC2Attrs(instanceType string, override map[string]string) map[string]string {
	base := pricingEC2TypeAttrs[instanceType]
	attrs := make(map[string]string, len(base)+len(override))
	for k, v := range base {
		attrs[k] = v
	}
	for k, v := range override {
		attrs[k] = v
	}
	return attrs
}

// pricingCorpusEC2 is the AmazonEC2 slice of the offer corpus, ordered
// us-east-1, us-west-2, eu-west-1 within each group so that iteration — and
// therefore GetProducts' PriceList order and its NextToken pages — is stable.
//
//nolint:gochecknoglobals // Immutable reference data, read-only after init.
var pricingCorpusEC2 = []pricingCorpusEntry{

	// --- us-east-1 ---
	{
		// The nominal row a consumer wants: Linux, Shared tenancy, Used capacity,
		// no pre-installed software, on-demand. us-east-1 usagetype carries no
		// Region prefix at all.
		sku:           "CRAJUW7BTXFMT2UJ",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("t3.micro", map[string]string{
			"location": "US East (N. Virginia)", "regionCode": "us-east-1",
			"usagetype": "BoxUsage:t3.micro",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "CRAJUW7BTXFMT2UJ.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.0104 per On Demand Linux t3.micro Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.0104000000"}, AppliesTo: []string{},
		}},
	},
	{
		sku:           "5G4TA8Z4MUKE6MJB",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("m5.xlarge", map[string]string{
			"location": "US East (N. Virginia)", "regionCode": "us-east-1",
			"usagetype": "BoxUsage:m5.xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "5G4TA8Z4MUKE6MJB.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.192 per On Demand Linux m5.xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.1920000000"}, AppliesTo: []string{},
		}},
	},
	{
		sku:           "7TVXMFHQR6U7W5CV",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("g4dn.xlarge", map[string]string{
			"location": "US East (N. Virginia)", "regionCode": "us-east-1",
			"usagetype": "BoxUsage:g4dn.xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "7TVXMFHQR6U7W5CV.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.526 per On Demand Linux g4dn.xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.5260000000"}, AppliesTo: []string{},
		}},
	},
	{
		sku:           "FX9TUYTJKEENVBSR",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("g5.2xlarge", map[string]string{
			"location": "US East (N. Virginia)", "regionCode": "us-east-1",
			"usagetype": "BoxUsage:g5.2xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "FX9TUYTJKEENVBSR.JRTCKXETXF.6YS6EN2CT7",
			Description: "$1.212 per On Demand Linux g5.2xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "1.2120000000"}, AppliesTo: []string{},
		}},
	},
	{
		sku:           "TPKUC6XYSUQ3VFK2",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("g6.xlarge", map[string]string{
			"location": "US East (N. Virginia)", "regionCode": "us-east-1",
			"usagetype": "BoxUsage:g6.xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "TPKUC6XYSUQ3VFK2.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.8048 per On Demand Linux g6.xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.8048000000"}, AppliesTo: []string{},
		}},
	},
	{
		sku:           "M85N22VB4U7UGU9M",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("inf2.xlarge", map[string]string{
			"location": "US East (N. Virginia)", "regionCode": "us-east-1",
			"usagetype": "BoxUsage:inf2.xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "M85N22VB4U7UGU9M.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.7582 per On Demand Linux inf2.xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.7582000000"}, AppliesTo: []string{},
		}},
	},
	{
		// p4d.24xlarge is the rate that does NOT match across us-east-1 and
		// us-west-2: 21.9576420000 against 21.9576400000. Copied per Region.
		sku:           "H7NGEAC6UEHNTKSJ",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("p4d.24xlarge", map[string]string{
			"location": "US East (N. Virginia)", "regionCode": "us-east-1",
			"usagetype": "BoxUsage:p4d.24xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "H7NGEAC6UEHNTKSJ.JRTCKXETXF.6YS6EN2CT7",
			Description: "$21.957642 per On Demand Linux p4d.24xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "21.9576420000"}, AppliesTo: []string{},
		}},
	},
	{
		sku:           "3D4V8UAYEMB38GU2",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("p5.48xlarge", map[string]string{
			"location": "US East (N. Virginia)", "regionCode": "us-east-1",
			"usagetype": "BoxUsage:p5.48xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "3D4V8UAYEMB38GU2.JRTCKXETXF.6YS6EN2CT7",
			Description: "$55.04 per On Demand Linux p5.48xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "55.0400000000"}, AppliesTo: []string{},
		}},
	},
	{
		sku:           "WVVQPJVCQU4SR8XF",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("trn1.32xlarge", map[string]string{
			"location": "US East (N. Virginia)", "regionCode": "us-east-1",
			"usagetype": "BoxUsage:trn1.32xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "WVVQPJVCQU4SR8XF.JRTCKXETXF.6YS6EN2CT7",
			Description: "$21.50 per On Demand Linux trn1.32xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "21.5000000000"}, AppliesTo: []string{},
		}},
	},

	// --- us-west-2 ---
	{
		// us-west-2: a different SKU and a USW2- usagetype prefix for the same
		// product, at a rate byte-identical to us-east-1's — except p4d.24xlarge.
		sku:           "VJNS2RY9CFHRVCMH",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("t3.micro", map[string]string{
			"location": "US West (Oregon)", "regionCode": "us-west-2",
			"usagetype": "USW2-BoxUsage:t3.micro",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "VJNS2RY9CFHRVCMH.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.0104 per On Demand Linux t3.micro Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.0104000000"}, AppliesTo: []string{},
		}},
	},
	{
		sku:           "T5CRJD6MS45TWRHG",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("m5.xlarge", map[string]string{
			"location": "US West (Oregon)", "regionCode": "us-west-2",
			"usagetype": "USW2-BoxUsage:m5.xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "T5CRJD6MS45TWRHG.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.192 per On Demand Linux m5.xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.1920000000"}, AppliesTo: []string{},
		}},
	},
	{
		sku:           "DY2XVQ4WPT7BKDCH",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("g4dn.xlarge", map[string]string{
			"location": "US West (Oregon)", "regionCode": "us-west-2",
			"usagetype": "USW2-BoxUsage:g4dn.xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "DY2XVQ4WPT7BKDCH.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.526 per On Demand Linux g4dn.xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.5260000000"}, AppliesTo: []string{},
		}},
	},
	{
		sku:           "GY47PZ4E422T8DWN",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("g5.2xlarge", map[string]string{
			"location": "US West (Oregon)", "regionCode": "us-west-2",
			"usagetype": "USW2-BoxUsage:g5.2xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "GY47PZ4E422T8DWN.JRTCKXETXF.6YS6EN2CT7",
			Description: "$1.212 per On Demand Linux g5.2xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "1.2120000000"}, AppliesTo: []string{},
		}},
	},
	{
		sku:           "PCX39NHAEPRR9GUY",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("g6.xlarge", map[string]string{
			"location": "US West (Oregon)", "regionCode": "us-west-2",
			"usagetype": "USW2-BoxUsage:g6.xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "PCX39NHAEPRR9GUY.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.8048 per On Demand Linux g6.xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.8048000000"}, AppliesTo: []string{},
		}},
	},
	{
		sku:           "JQQFHDV8JU9MJ96J",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("inf2.xlarge", map[string]string{
			"location": "US West (Oregon)", "regionCode": "us-west-2",
			"usagetype": "USW2-BoxUsage:inf2.xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "JQQFHDV8JU9MJ96J.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.7582 per On Demand Linux inf2.xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.7582000000"}, AppliesTo: []string{},
		}},
	},
	{
		// p4d.24xlarge is the rate that does NOT match across us-east-1 and
		// us-west-2: 21.9576420000 against 21.9576400000. Copied per Region.
		sku:           "DHAMWQPURBTZBUA6",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("p4d.24xlarge", map[string]string{
			"location": "US West (Oregon)", "regionCode": "us-west-2",
			"usagetype": "USW2-BoxUsage:p4d.24xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "DHAMWQPURBTZBUA6.JRTCKXETXF.6YS6EN2CT7",
			Description: "$21.95764 per On Demand Linux p4d.24xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "21.9576400000"}, AppliesTo: []string{},
		}},
	},
	{
		sku:           "HNW4W6GM8AE2PCQX",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("p5.48xlarge", map[string]string{
			"location": "US West (Oregon)", "regionCode": "us-west-2",
			"usagetype": "USW2-BoxUsage:p5.48xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "HNW4W6GM8AE2PCQX.JRTCKXETXF.6YS6EN2CT7",
			Description: "$55.04 per On Demand Linux p5.48xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "55.0400000000"}, AppliesTo: []string{},
		}},
	},
	{
		sku:           "7AKU67FQVWG865YR",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("trn1.32xlarge", map[string]string{
			"location": "US West (Oregon)", "regionCode": "us-west-2",
			"usagetype": "USW2-BoxUsage:trn1.32xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "7AKU67FQVWG865YR.JRTCKXETXF.6YS6EN2CT7",
			Description: "$21.50 per On Demand Linux trn1.32xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "21.5000000000"}, AppliesTo: []string{},
		}},
	},

	// --- eu-west-1 ---
	{
		// eu-west-1: the legacy EU- prefix, and a per-family premium over
		// us-east-1. g6, p5 and trn1 publish nothing here.
		sku:           "DXH6X4VVFUT4EUU5",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("t3.micro", map[string]string{
			"location": "EU (Ireland)", "regionCode": "eu-west-1",
			"usagetype": "EU-BoxUsage:t3.micro",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "DXH6X4VVFUT4EUU5.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.0114 per On Demand Linux t3.micro Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.0114000000"}, AppliesTo: []string{},
		}},
	},
	{
		sku:           "HNKY83Z77VRXC2UH",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("m5.xlarge", map[string]string{
			"location": "EU (Ireland)", "regionCode": "eu-west-1",
			"usagetype": "EU-BoxUsage:m5.xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "HNKY83Z77VRXC2UH.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.214 per On Demand Linux m5.xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.2140000000"}, AppliesTo: []string{},
		}},
	},
	{
		sku:           "PEJRQD7KDD2NBFH5",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("g4dn.xlarge", map[string]string{
			"location": "EU (Ireland)", "regionCode": "eu-west-1",
			"usagetype": "EU-BoxUsage:g4dn.xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "PEJRQD7KDD2NBFH5.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.587 per On Demand Linux g4dn.xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.5870000000"}, AppliesTo: []string{},
		}},
	},
	{
		sku:           "P2KE3JYU9KQ2RD7B",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("g5.2xlarge", map[string]string{
			"location": "EU (Ireland)", "regionCode": "eu-west-1",
			"usagetype": "EU-BoxUsage:g5.2xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "P2KE3JYU9KQ2RD7B.JRTCKXETXF.6YS6EN2CT7",
			Description: "$1.35296 per On Demand Linux g5.2xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "1.3529600000"}, AppliesTo: []string{},
		}},
	},
	{
		sku:           "PRB4AAR5CU7BQTGA",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("inf2.xlarge", map[string]string{
			"location": "EU (Ireland)", "regionCode": "eu-west-1",
			"usagetype": "EU-BoxUsage:inf2.xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "PRB4AAR5CU7BQTGA.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.94775 per On Demand Linux inf2.xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.9477500000"}, AppliesTo: []string{},
		}},
	},
	{
		// p4d.24xlarge is the rate that does NOT match across us-east-1 and
		// us-west-2: 21.9576420000 against 21.9576400000. Copied per Region.
		sku:           "CQ3RZ8A8BQKVHSKT",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("p4d.24xlarge", map[string]string{
			"location": "EU (Ireland)", "regionCode": "eu-west-1",
			"usagetype": "EU-BoxUsage:p4d.24xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "CQ3RZ8A8BQKVHSKT.JRTCKXETXF.6YS6EN2CT7",
			Description: "$23.71569 per On Demand Linux p4d.24xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "23.7156900000"}, AppliesTo: []string{},
		}},
	},

	// --- us-east-1, one attribute away from the nominal row ---
	{
		// Windows with a license, $0.376. The next two SKUs satisfy the same
		// seven filters and share this usagetype, differing only in licenseModel
		// and operation — which is why the documented seven-filter recipe
		// returns three rates for Windows, not one.
		sku:           "HXGQ45BFM9J2FJ3F",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("m5.xlarge", map[string]string{
			"location": "US East (N. Virginia)", "operatingSystem": "Windows",
			"operation": "RunInstances:0002", "regionCode": "us-east-1",
			"usagetype": "BoxUsage:m5.xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "HXGQ45BFM9J2FJ3F.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.376 per On Demand Windows m5.xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.3760000000"}, AppliesTo: []string{},
		}},
	},
	{
		// Windows without licenses, $0.192 — the Linux rate, under operation
		// RunInstances:0002:box.
		sku:           "AWJMJ73C9RDQFD5N",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("m5.xlarge", map[string]string{
			"licenseModel": "License Included - Infrastructure", "location": "US East (N. Virginia)",
			"operatingSystem": "Windows", "operation": "RunInstances:0002:box",
			"regionCode": "us-east-1", "usagetype": "BoxUsage:m5.xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "AWJMJ73C9RDQFD5N.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.192 per On Demand Windows without licenses m5.xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.1920000000"}, AppliesTo: []string{},
		}},
	},
	{
		// Windows BYOL, $0.192, operation RunInstances:0800.
		sku:           "8AW2DYRV3M9N5RRZ",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("m5.xlarge", map[string]string{
			"licenseModel": "Bring your own license", "location": "US East (N. Virginia)",
			"operatingSystem": "Windows", "operation": "RunInstances:0800",
			"regionCode": "us-east-1", "usagetype": "BoxUsage:m5.xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "8AW2DYRV3M9N5RRZ.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.192 per On Demand Windows BYOL m5.xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.1920000000"}, AppliesTo: []string{},
		}},
	},
	{
		// preInstalledSw is the discriminator here, and it triples the rate:
		// $0.672 against $0.192. operation encodes the software as :0004.
		sku:           "EHFDZFP2EH9BD4DX",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("m5.xlarge", map[string]string{
			"location": "US East (N. Virginia)", "operation": "RunInstances:0004",
			"preInstalledSw": "SQL Std", "regionCode": "us-east-1",
			"usagetype": "BoxUsage:m5.xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "EHFDZFP2EH9BD4DX.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.672 per On Demand Linux with SQL Std m5.xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.6720000000"}, AppliesTo: []string{},
		}},
	},
	{
		// Dedicated tenancy changes the usagetype token to DedicatedUsage:,
		// not only the price, so a caller matching "BoxUsage:" misses it.
		sku:           "AN9HBCS6XCP64Z83",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("m5.xlarge", map[string]string{
			"location": "US East (N. Virginia)", "regionCode": "us-east-1",
			"tenancy": "Dedicated", "usagetype": "DedicatedUsage:m5.xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "AN9HBCS6XCP64Z83.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.204 per Dedicated Linux m5.xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.2040000000"}, AppliesTo: []string{},
		}},
	},
	{
		// UnusedCapacityReservation: same $0.192, usagetype UnusedBox:, and it
		// carries an instancesku attribute pointing at the Used SKU above —
		// a cross-reference nothing else in the corpus has.
		sku:           "E9RWRGDUV5X76FUB",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("m5.xlarge", map[string]string{
			"capacitystatus": "UnusedCapacityReservation", "instancesku": "5G4TA8Z4MUKE6MJB",
			"location": "US East (N. Virginia)", "regionCode": "us-east-1",
			"usagetype": "UnusedBox:m5.xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "E9RWRGDUV5X76FUB.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.192 per Unused Reservation Linux m5.xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.1920000000"}, AppliesTo: []string{},
		}},
	},
	{
		// A Capacity Block p5.48xlarge at $0.00, against $55.04 on demand for
		// the same instance type in the same Region. marketoption is the only
		// filter that separates them, which is why omitting it can report a
		// free p5.
		sku:           "5Q7AP3RH7KTVUNCJ",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: pricingEC2Attrs("p5.48xlarge", map[string]string{
			"location": "US East (N. Virginia)", "marketoption": "CapacityBlock",
			"operation": "RunInstances:CB", "regionCode": "us-east-1",
			"usagetype": "BoxUsage:p5.48xlarge",
		}),
		effectiveDate: "2026-09-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "5Q7AP3RH7KTVUNCJ.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.00 per Capacity Block Linux p5.48xlarge Instance Hour",
			BeginRange:  "0", EndRange: "Inf", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.0000000000"}, AppliesTo: []string{},
		}},
	},

	// --- the free-tier pseudo-product ---
	{
		// Present in every Region file with this one SKU. It has no instanceType, its
		// location is "Any" and its regionCode is the empty string, so no regionCode
		// filter selects it; its term is not the global on-demand code, its endRange is
		// "750" rather than "Inf", its termAttributes is non-empty and its appliesTo
		// lists 170 SKUs. Any assertion that a Compute Instance carries an instanceType,
		// or that endRange is always "Inf", is falsified by this record — which is
		// exactly why it is here.
		sku:           "R3WPVNCD34N9W7UV",
		serviceCode:   pricingServiceCodeEC2,
		productFamily: pricingEC2ComputeInstance,
		attributes: map[string]string{
			"group": "FreeTrial", "groupDescription": "Free Trial for running instances for a limited number of hours and instances, see marketing for details",
			"location": "Any", "locationType": "AWS Region",
			"operation": "RunInstances", "regionCode": "",
			"servicecode": pricingServiceCodeEC2, "servicename": "Amazon Elastic Compute Cloud",
			"usagetype": "Global-BoxUsage:freetrial",
		},
		effectiveDate:  "2026-09-01T00:00:00Z",
		offerTermCode:  pricingEC2FreeTierTermCode,
		termAttributes: map[string]string{"Restriction": "Limited SKU Usage"},
		dimensions: []pricingPriceDimension{{
			RateCode:    "R3WPVNCD34N9W7UV.A429C66SYZ.TKQXKGP374",
			Description: "Global",
			BeginRange:  "0", EndRange: "750", Unit: "Hrs",
			PricePerUnit: map[string]string{"USD": "0.0000000000"},
			AppliesTo:    pricingEC2FreeTierAppliesTo,
		}},
	},
}

// pricingEC2Attributes is the attribute names DescribeServices reports for
// AmazonEC2: exactly the union of what the SKUs above carry, not EC2's full
// published set, so a caller moving from DescribeServices to GetAttributeValues to
// a GetProducts filter never hits a name that filters to nothing.
//
// The casing is AWS's and it is not consistent: gpu is lowercase where gpuMemory is
// camel, instancesku is all lowercase, and intelAvx2Available puts the digit before
// Available.
//
//nolint:gochecknoglobals // Immutable reference data, read-only after init.
var pricingEC2Attributes = []string{
	"availabilityzone",
	"capacitystatus",
	"classicnetworkingsupport",
	"clockSpeed",
	"currentGeneration",
	"dedicatedEbsThroughput",
	"dedicatedEbsThroughputDescription",
	"ecu",
	"enhancedNetworkingSupported",
	"gpu",
	"gpuMemory",
	"group",
	"groupDescription",
	"instanceFamily",
	"instanceFamilyCategory",
	"instanceType",
	"instancesku",
	"intelAvx2Available",
	"intelAvxAvailable",
	"intelTurboAvailable",
	"licenseModel",
	"location",
	"locationType",
	"marketoption",
	"memory",
	"networkPerformance",
	"normalizationSizeFactor",
	"operatingSystem",
	"operation",
	"physicalProcessor",
	"preInstalledSw",
	"processorArchitecture",
	"processorFeatures",
	"regionCode",
	"servicecode",
	"servicename",
	"storage",
	"tenancy",
	"usagetype",
	"vcpu",
	"vpcnetworkingsupport",
}
