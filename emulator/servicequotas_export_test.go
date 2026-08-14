package emulator

// This file exports the Service Quotas seed tables for the external test package,
// so their consistency can be asserted directly rather than inferred from the API
// responses they feed. It is compiled only when running tests.

// ServiceQuotaCodesForTest returns the service codes keyed in
// defaultServiceQuotas — the table ListServiceQuotas and GetServiceQuota read.
func ServiceQuotaCodesForTest() []string {
	codes := make([]string, 0, len(defaultServiceQuotas))
	for code := range defaultServiceQuotas {
		codes = append(codes, code)
	}
	return codes
}

// ServiceQuotaRowsForTest returns every quota in defaultServiceQuotas for one
// service code, and whether the service is in the table at all.
func ServiceQuotaRowsForTest(serviceCode string) ([]ServiceQuota, bool) {
	quotas, ok := defaultServiceQuotas[serviceCode]
	return quotas, ok
}

// ServiceListForTest returns the service codes and names in defaultServiceList —
// the table ListServices reads.
func ServiceListForTest() map[string]string {
	names := make(map[string]string, len(defaultServiceList))
	for _, entry := range defaultServiceList {
		names[entry["ServiceCode"]] = entry["ServiceName"]
	}
	return names
}

// ServiceListLenForTest is the number of entries in defaultServiceList, exported
// so a duplicate code can be told apart from a missing one.
//
// The Organizations ceilings this package's tests compare the seeded values
// against are already exported by organizations_export_test.go.
func ServiceListLenForTest() int { return len(defaultServiceList) }
