package emulator

// tagOperation claims the resource tagging operations. Tags reach the
// authorization decision through the organizations arms of addRequestTags and
// addResourceTags in authz.go.
func (p *OrganizationsPlugin) tagOperation(_ string) (orgHandler, bool) {
	return nil, false
}
