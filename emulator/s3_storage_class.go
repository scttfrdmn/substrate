package emulator

import (
	"net/http"
)

// S3StorageClassStandard is the storage class S3 assigns to an object written
// without an x-amz-storage-class header. It is also the one class S3 omits from
// the x-amz-storage-class response header on GetObject and HeadObject.
const S3StorageClassStandard = "STANDARD"

// s3StorageClasses is the set of values S3 accepts in x-amz-storage-class.
//
// It is the full documented enumeration, including the classes reachable only
// through S3 on Outposts, Snow, Express One Zone and the FSx-backed tiers. Those
// are not modeled as distinct behavior here, but accepting them keeps substrate
// from rejecting a value real S3 would take — an emulator that returns an error
// where AWS succeeds is the same class of defect as one that succeeds where AWS
// errors.
var s3StorageClasses = map[string]bool{
	S3StorageClassStandard: true,
	"REDUCED_REDUNDANCY":   true,
	"STANDARD_IA":          true,
	"ONEZONE_IA":           true,
	"INTELLIGENT_TIERING":  true,
	"GLACIER":              true,
	"DEEP_ARCHIVE":         true,
	"OUTPOSTS":             true,
	"GLACIER_IR":           true,
	"SNOW":                 true,
	"EXPRESS_ONEZONE":      true,
	"FSX_OPENZFS":          true,
	"FSX_ONTAP":            true,
}

// s3ArchiveStorageClasses is the set of storage classes whose objects are not
// directly readable: a GetObject against one fails until the object is restored.
//
// GLACIER_IR is deliberately absent. It is the instant-retrieval tier and reads
// like any other class; treating it as archival is a plausible-looking mistake
// that would make a consumer's restore path fire where real S3 never would.
var s3ArchiveStorageClasses = map[string]bool{
	"GLACIER":      true,
	"DEEP_ARCHIVE": true,
}

// resolveStorageClass returns the storage class a write should record, given the
// request's x-amz-storage-class header, along with the error response to serve
// when the value is not one S3 accepts.
//
// An absent or empty header yields STANDARD, which is S3's documented default for
// a newly created object. This is applied to CopyObject too: the copy's class comes
// from the request, and is *not* inherited from the source — "if the
// x-amz-storage-class header is not used, the copied object will be stored in the
// STANDARD Storage Class by default".
func resolveStorageClass(headers map[string]string) (string, *AWSResponse) {
	value := headerValueFold(headers, "x-amz-storage-class")
	if value == "" {
		return S3StorageClassStandard, nil
	}
	if !s3StorageClasses[value] {
		return "", s3ErrorResponse("InvalidStorageClass", "The storage class you specified is not valid.", http.StatusBadRequest)
	}
	return value, nil
}

// storageClassOf returns an object's storage class, treating the empty string as
// STANDARD so that objects written before the field existed read back correctly.
func storageClassOf(obj *S3Object) string {
	if obj.StorageClass == "" {
		return S3StorageClassStandard
	}
	return obj.StorageClass
}

// evaluateStorageClassRead returns the error response to serve when an object's
// storage class makes its body unavailable, or nil when the read may proceed.
//
// Only GetObject uses this. HeadObject deliberately does not: "Even if the object
// is stored in S3 Glacier, all object metadata is still available", and the
// HeadObject reference lists no error but NoSuchKey. A HEAD of an archived object
// is a 200 — which is what makes HEAD the way a consumer discovers that a GET
// would need a restore first.
func evaluateStorageClassRead(obj *S3Object) *AWSResponse {
	if !s3ArchiveStorageClasses[storageClassOf(obj)] {
		return nil
	}
	return s3ErrorResponse("InvalidObjectState", "The action is not valid for the object's storage class", http.StatusForbidden)
}
