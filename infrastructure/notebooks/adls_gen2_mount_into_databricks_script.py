# Databricks notebook source
# Set general vaiables
env = 'prd' # 'prd' | 'dev'
storage_account_name = 'dip{env}46s4e6st'.format(env=env)
container_name_01 = 'dip-{env}-asdlgen2-fs-raw-zone'.format(env=env)
container_name_02 = 'dip-{env}-asdlgen2-fs-curated-zone'.format(env=env)
folder_path_01 = 'raw'
folder_path_02 = 'curated'
mounting_point_01 = '/mnt/raw'
mounting_point_02 = '/mnt/curated'

# COMMAND ----------

#application_client_id = '856f8437-4a9a-4cd8-8b5f-cb8bc089509a'
#client_secret = '37X7Q~yexx8gcuI_pHwx75jQV4JJUMJGuenaD'
#tenant_id = '4e545e1b-d10d-4607-b5f2-69af9be47d24'

# COMMAND ----------

# Accessing Azure KeyVault Secrets via Databricks Secrets Scope
application_client_id = dbutils.secrets.get(scope='dip-{env}-46s4e6-kv'.format(env=env), key='dip-{env}-46s4e6-terraform-app-01-clientid'.format(env=env))
client_secret = dbutils.secrets.get(scope='dip-{env}-46s4e6-kv'.format(env=env), key='dip-{env}-46s4e6-terraform-app-01-secret'.format(env=env))
tenant_id = dbutils.secrets.get(scope='dip-{env}-46s4e6-kv'.format(env=env), key='tenantid')

# COMMAND ----------

# Define End point
end_point = 'https://login.microsoftonline.com/{tenant_id}/oauth2/token'.format(tenant_id=tenant_id)
end_point

# COMMAND ----------

# Define Source path
source_path_01 = 'abfss://{container}@{storage_account}.dfs.core.windows.net/{folder_path}'.format(container=container_name_01, storage_account=storage_account_name, folder_path=folder_path_01)
source_path_02 = 'abfss://{container}@{storage_account}.dfs.core.windows.net/{folder_path}'.format(container=container_name_02, storage_account=storage_account_name, folder_path=folder_path_02)
print('source_path_01: {}\nsource_path_02: {}'.format(source_path_01, source_path_02))

# COMMAND ----------

print('mounting_point_01: {}\nmounting_point_02: {}'.format(mounting_point_01, mounting_point_02))

# COMMAND ----------

# Preparing Configuration
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": end_point}

# COMMAND ----------

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = source_path_01,
  mount_point = mounting_point_01,
  extra_configs = configs)

# COMMAND ----------

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = source_path_02,
  mount_point = mounting_point_02,
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls('.')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/raw

# COMMAND ----------

import pandas as pd

# COMMAND ----------

pd.read_csv(r'/dbfs/mnt/raw/cities.csv')

# COMMAND ----------

# Unmount a mounted point
dbutils.fs.unmount("/mnt/raw_files")