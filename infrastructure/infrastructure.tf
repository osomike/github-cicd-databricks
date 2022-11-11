###########################################################
### Default configuration block when working with Azure ###
###########################################################
terraform {
  # Provide configuration details for Terraform
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>2.90"
    }

    databricks = {
      source  = "databricks/databricks"
      version = "~>0.4"
    }

  }
  # This block allows us to save the terraform.tfstate file on the cloud, so a team of developers can use the terraform
  # configuration to update the infrastructure.
  # Link: https://learn.microsoft.com/en-us/azure/developer/terraform/store-state-in-azure-storage?tabs=azure-cli
  # Note.- Before using this block, is important that the resource group, storage account and container ARE DEPLOYED.
  backend "azurerm" {
    resource_group_name  = "dip-prd-master-rg"
    storage_account_name = "dipprdmasterst"
    container_name       = "dip-prd-asdlgen2-fs-config"
    key                  = "github-tst-36db51-rg/terraform.tfstate"
    
  }
}

# provide configuration details for the Azure terraform provider
provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = true
    }
  }
}

## comment this if the resource group has to be imported (See Readme)
## uncomment again once the import is done and you run "terraform apply"
# provide configuration details for the Databricks terraform provider
provider "databricks" {
  # Important to keep
  azure_workspace_resource_id = azurerm_databricks_workspace.databricks_workspace.id
}

# For naming conventions please refer to:
# https://docs.microsoft.com/en-us/azure/azure-resource-manager/management/resource-name-rules

# Get info about current user
data "azuread_client_config" "current" {}


###########################################################
###################  Resource Group #######################
###########################################################
resource "azurerm_resource_group" "rg" {
  location = var.location
  name     = "${var.default_prefix}-${var.environment}-${var.random_id}-rg"
  tags = {
    owner       = var.owner
    environment = var.environment

  }
}

# Add Ownership roles for the resource group to the list of contributors.
# Note.- Admins do not need to be added. They are already owners of the subscription, so they will inherit the ownershipt for this resource group. Otherwise there will be listed twice.
resource "azurerm_role_assignment" "roles_on_rg_for_contributors" {
  for_each = toset(var.contributors_object_ids)
  role_definition_name = "Owner" # "Owner" | "Contributor" | azurerm_role_definition.rd.name
  scope                = azurerm_resource_group.rg.id
  principal_id         = each.key
}

###########################################################
##############  Azure Databricks Workspace ################
###########################################################
resource "azurerm_databricks_workspace" "databricks_workspace" {
  name = "${var.default_prefix}-${var.environment}-${var.random_id}-databricks" # Between 3 to 64 characters and
                                                                                # UNIQUE within resource group
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "standard"

  tags = {
    owner       = var.owner
    environment = var.environment
  }
}


###########################################################
##################  Databricks Cluster ####################
###########################################################
resource "databricks_cluster" "db_cluster_default" {
  cluster_name            = "${var.default_prefix}-${var.environment}-${var.random_id}-cluster-01"
  spark_version           = var.default_cluster["spark_version"]
  node_type_id            = var.default_cluster["node_type_id"]
  autotermination_minutes = var.default_cluster["autotermination_minutes"]
  # Set environmental variables into cluster
  spark_env_vars = {
    AZURE_ENVIRONMENT_VALUE = var.environment

  }
  autoscale {
    min_workers = var.default_cluster["min_workers"]
    max_workers = var.default_cluster["max_workers"]
  }
  depends_on = [azurerm_databricks_workspace.databricks_workspace]
}


###########################################################
#####  Upload notebooks into Databricks Workspace #########
###########################################################
resource "databricks_notebook" "mount_example" {
  source   = var.mount_notebook["src"]
  path     = var.mount_notebook["dst"]
  language = var.mount_notebook["language"]
}

resource "databricks_notebook" "delta_example" {
  source   = var.delta_tables_notebook["src"]
  path     = var.delta_tables_notebook["dst"]
  language = var.delta_tables_notebook["language"]
}
