locals {
  data_lake_bucket = "weather-gis"
}

variable "project" {
  description   = "Your GCP Project ID"
  default       = "monitor-tropical-storms"
  type          = string
}

variable "region" {
  description   = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default       = "us-west1"
  type          = string
}

variable "storage_class" {
  description   = "Storage class type for your bucket. Check official docs for more info."
  default       = "STANDARD"
  type          = string
}

variable "BQ_SUFFIX" {
  description   = "BigQuery Dataset that raw data (from GCS) will be written to"
  default       = "gis"
  type          = string
}