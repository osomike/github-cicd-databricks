variable "azure_subs_id" {
  type        = string
  default     = "ef0661c5-0e9a-4467-ba85-e57a8816570d"
  description = "Subscription ID"
}

variable "default_prefix" {
  type        = string
  default     = "github" # Up to 10 alphanumerical characters
  description = "Prefix used to name the resources and applications"
}

variable "random_id" {
  type        = string
  default     = "36db51" # up to 6 alphanumerical characters. Preferably starts and ends with a number.
  description = "Random alphanumerical value to generate unique names for different components within Azure"
}

variable "owner" {
  type        = string
  default     = "Oscar Mike"
  description = "Name of the owner"
}

variable "environment" {
  type        = string
  default     = "tst" # Preferably "prd" | "dev" | "tst". Up to 3 characters
  description = "Type of environment. Production, Development or Test"
}

variable "location" {
  type        = string
  default     = "West Europe"
  description = "Location where the resources will be deployed"
}

variable "default_cluster" {
  type = map(any)
  default = {
    spark_version             = "10.4.x-scala2.12"
    node_type_id              = "Standard_DS3_v2"
    "max_workers"             = 4
    "min_workers"             = 2
    "autotermination_minutes" = 20
  }
  description = "Parameters for creating the default cluster for databricks"
}

variable "mount_notebook" {
  type = map(any)
  default = {
    src      = "./notebooks/adls_gen2_mount_into_databricks_script.py"
    language = "PYTHON"
    dst      = "/Shared/mount_notebook"
  }
  description = "Notebook example to mount ASDL Gen2 into databricks"
}

variable "delta_tables_notebook" {
  type = map(any)
  default = {
    src      = "./notebooks/delta_tables_generator.py"
    language = "PYTHON"
    dst      = "/Shared/delta_tables_example"
  }
  description = "Notebook example to generate delta tables using spark in databricks"
}

variable "admins_object_ids" {
  type    = list(string)
  default = [
    "5a80984f-b433-4ce2-b1d1-08b94c3c0ccf",     # Oscar Mike Claure Cabrera
    "675d806a-e74a-4fe6-a2b5-663e454b18fc",     # Michael Hogervorst
    "11ffdf4c-30e1-4ab6-8ba7-fb2b91b536b7"      # Sander van Dorsten
    ]

}

variable "contributors_object_ids" {
  type    = list(string)
  default = [
    "de11e3a1-20e6-4b8b-8b8a-61325976a0f2",     # Bob Strube
    "7a2c9caf-3e8e-4de5-acfd-4cc1505b4ff4",     # Casper Damen
    "36a85ba4-4b9d-41d0-a2e0-ce08dfa141e8"      # Job Almekinders
    ]

}