package emulator

// policyOperation claims the service control policy lifecycle operations. The
// policy, attachment and FullAWSAccess stores live in organizations_state.go;
// this file owns only the operations over them.
func (p *OrganizationsPlugin) policyOperation(_ string) (orgHandler, bool) {
	return nil, false
}
