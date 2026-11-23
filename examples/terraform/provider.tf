# The provider is pointed at substrate by the environment rather than by an
# endpoints block: AWS_ENDPOINT_URL sends every service to one URL, and
# AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY supply any key. See README.md.
provider "aws" {
  region = var.region

  # Without these the provider validates the key against STS, probes the EC2
  # instance-metadata service and looks up the caller's account before planning.
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  # Substrate serves every bucket from one host, so the bucket name goes in the path.
  s3_use_path_style = true
}
