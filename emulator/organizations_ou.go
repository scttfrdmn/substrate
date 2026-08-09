package emulator

// ouOperation claims the organizational-unit and hierarchy operations. The
// hierarchy itself lives in organizations_state.go; this file owns only the
// operations that read and build it.
func (p *OrganizationsPlugin) ouOperation(_ string) (orgHandler, bool) {
	return nil, false
}
