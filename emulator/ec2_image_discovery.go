package emulator

// Finding an AMI on a fresh server (#1432).
//
// RunInstances refuses an AMI it cannot resolve (#733), and a fresh server holds no image the
// account owns — so a test that launched from a fabricated ami-12345678 broke with an error
// pointing at the ID rather than at the missing step, and a test asking DescribeImages what
// exists was told nothing. The bundled catalog ([bundledImages]) was always there; these two
// helpers make it discoverable: DescribeImages lists it for Owner.N=amazon, and the refusal
// says where an AMI comes from.

// ec2ImageOwnerSelected reports whether Owner.N selects img.
//
// An empty list selects every image the describe walks. Otherwise each value is one of the
// forms API_DescribeImages publishes: `self` or an account ID matches the image's owning
// account, and an alias (`amazon`, `aws-marketplace`, `aws-backup-vault`) matches the image's
// imageOwnerAlias. A bundled AMI has no owning account substrate can name — see
// [bundledImage.image] — so it is selected by its alias alone, and the two Canonical entries,
// which carry none, are selected by no owner value: their owner is an account substrate does
// not model, and naming one here would be an observation nothing backs.
func ec2ImageOwnerSelected(img EC2Image, owners []string, accountID string) bool {
	if len(owners) == 0 {
		return true
	}
	for _, owner := range owners {
		switch {
		case owner == "self":
			if img.AccountID != "" && img.AccountID == accountID {
				return true
			}
		case img.AccountID != "" && owner == img.AccountID:
			return true
		case img.OwnerAlias != "" && owner == img.OwnerAlias:
			return true
		}
	}
	return false
}

// ec2OwnersNameBundledAlias reports whether Owner.N names an alias some bundled AMI is
// published under, which is what makes the catalog part of a describe's answer.
func ec2OwnersNameBundledAlias(owners []string) bool {
	for _, owner := range owners {
		for _, b := range bundledImages {
			if b.OwnerAlias != "" && owner == b.OwnerAlias {
				return true
			}
		}
	}
	return false
}

// ec2AMINotFoundHint extends RunInstances' InvalidAMIID.NotFound with where a launchable AMI
// comes from, and returns every other refusal unchanged.
//
// The code and the leading sentence are the kind's own, so a caller dispatching on either is
// unaffected; the added sentences are what the issue's consumer needed — 21 tests broke on a
// fabricated ID with nothing in the error naming the missing step. The parameter named is the
// catalog's default, the one [BundledImageID] resolves for a launch that names none.
func ec2AMINotFoundHint(err *AWSError) *AWSError {
	if err == nil || err.Code != ec2ImageIDKind.NotFound {
		return err
	}
	hinted := *err
	hinted.Message = err.Message + ". No image with this ID is registered in this account and Region: " +
		"register one with RegisterImage, or launch a public AMI resolved from an SSM public parameter " +
		"such as " + bundledImageDefaultParameter + " (DescribeImages with Owner=amazon lists them)"
	return &hinted
}
