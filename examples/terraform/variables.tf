variable "region" {
  description = "Region the resources are created in."
  type        = string
  default     = "us-east-1"
}

variable "name" {
  description = "Prefix for every resource name."
  type        = string
  default     = "substrate-tf"
}
