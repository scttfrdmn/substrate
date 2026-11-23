package emulator

import (
	"context"
	"encoding/xml"
	"net/http"
)

// cloudformation_validate_template.go is ValidateTemplate on the wire (#1605).

// cfnTemplateParameterXML is one TemplateParameter: "ParameterKey", "DefaultValue", "NoEcho" and
// "Description", as the data type's page lists them.
type cfnTemplateParameterXML struct {
	ParameterKey string  `xml:"ParameterKey"`
	DefaultValue *string `xml:"DefaultValue,omitempty"`
	NoEcho       bool    `xml:"NoEcho"`
	Description  string  `xml:"Description,omitempty"`
}

// validateTemplate answers ValidateTemplate: "Validates a specified template. CloudFormation first
// checks if the template is valid JSON. If it isn't, CloudFormation checks if the template is valid
// YAML. If both these checks fail, CloudFormation returns a template validation error."
//
// "You must pass TemplateURL or TemplateBody. If both are passed, only TemplateBody is used." A
// TemplateURL alone is read from substrate's S3 by [CloudFormationPlugin.cfnTemplateFromRequest],
// as CreateStack reads one (#1606).
//
// The checks are [StackDeployer.ValidateTemplate]'s, which answer what AWS would: strict mode is
// not consulted, so a type substrate does not emulate is a valid template here and is refused
// only when a stack is created from it.
func (p *CloudFormationPlugin) validateTemplate(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	body := req.Params["TemplateBody"]
	if body == "" && req.Params["TemplateURL"] != "" {
		// Only when TemplateBody is absent: unlike CreateStack, ValidateTemplate accepts both and
		// uses the body, so the resolver's refusal of the pair does not apply here.
		fetched, templateErr := p.cfnTemplateFromRequest(reqCtx, req)
		if templateErr != nil {
			return nil, templateErr
		}
		body = fetched
	}
	if body == "" {
		return nil, cfnMissingParameter("TemplateBody")
	}
	v, err := p.scopedDeployer(reqCtx).ValidateTemplate(context.Background(), body)
	if err != nil {
		return nil, cfnMapDeployerError(err)
	}

	type parameters struct {
		Members []cfnTemplateParameterXML `xml:"member"`
	}
	// members is a list element, held by pointer so an empty list is omitted: encoding/xml
	// writes the parent of an `a>b,omitempty` path even when the list is empty.
	type members struct {
		Members []string `xml:"member"`
	}
	list := func(v []string) *members {
		if len(v) == 0 {
			return nil
		}
		return &members{Members: v}
	}
	type result struct {
		// Parameters is rendered even when the template declares none, as an empty element:
		// that is what the CLI's "Parameters": [] in a ValidateTemplate answer reflects.
		Parameters         parameters `xml:"Parameters"`
		Description        string     `xml:"Description,omitempty"`
		Capabilities       *members   `xml:"Capabilities,omitempty"`
		CapabilitiesReason string     `xml:"CapabilitiesReason,omitempty"`
		DeclaredTransforms *members   `xml:"DeclaredTransforms,omitempty"`
	}
	type response struct {
		XMLName  xml.Name            `xml:"ValidateTemplateResponse"`
		XMLNS    string              `xml:"xmlns,attr"`
		Result   result              `xml:"ValidateTemplateResult"`
		Metadata cfnResponseMetadata `xml:"ResponseMetadata"`
	}
	out := result{
		Description:        v.Description,
		Capabilities:       list(v.Capabilities),
		CapabilitiesReason: v.CapabilitiesReason,
		DeclaredTransforms: list(v.DeclaredTransforms),
	}
	for _, param := range v.Parameters {
		out.Parameters.Members = append(out.Parameters.Members, cfnTemplateParameterXML(param))
	}
	return cfnXMLResponse(http.StatusOK, response{XMLNS: cfnXMLNS, Result: out, Metadata: cfnMetadata(reqCtx)})
}
