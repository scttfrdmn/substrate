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
// imageOwnerAlias. An AWS-published bundled AMI has no owning account substrate can name — see
// [bundledImage.image] — so it is selected by its alias alone. The four Canonical entries carry
// no alias and report Canonical's documented account instead ([bundledImage.OwnerID]), so
// Owner.N naming 099720109477 selects them (#1494); until then no owner value did.
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

// ec2OwnersNameBundledPublisher reports whether Owner.N names an alias some bundled AMI is
// published under, or the account one is published from, which is what makes the catalog part
// of a describe's answer.
func ec2OwnersNameBundledPublisher(owners []string) bool {
	for _, owner := range owners {
		for _, b := range bundledImages {
			if (b.OwnerAlias != "" && owner == b.OwnerAlias) || (b.OwnerID != "" && owner == b.OwnerID) {
				return true
			}
		}
	}
	return false
}

// ec2ImageExecutableSelected reports whether ExecutableBy.N selects img (#1494).
//
// API_DescribeImages: "Scopes the images by users with explicit launch permissions. Specify an
// AWS account ID, self (the sender of the request), or all (public AMIs)." — all returns "all
// public AMIs", self or the caller's own account ID returns the "AMIs shared with your
// account", and another account ID "only AMIs shared with that specific AWS account ID".
//
// Substrate stores no launch permissions: ModifyImageAttribute is not modeled, so no AMI is ever
// shared with an account. all therefore selects the public images — the bundled catalog, since
// a registered AMI is never public — and every account form, self included, selects nothing.
// An AMI the caller owns is not "shared with" it, which is why self does not select one; the
// owner's images are what Owner.N=self names. An empty list selects every image, and several
// values union.
func ec2ImageExecutableSelected(img EC2Image, executableBy []string) bool {
	if len(executableBy) == 0 {
		return true
	}
	for _, who := range executableBy {
		if who == "all" && img.Public {
			return true
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
