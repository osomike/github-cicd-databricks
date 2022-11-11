# Databricks notebook source
import os
import pandas as pd
import datetime
from pyspark.sql.types import StructType, StringType, DoubleType, BooleanType, TimestampType, IntegerType
from pyspark.sql.functions import to_date, col, to_timestamp, date_format, lit

# COMMAND ----------

#pd.read_csv('/dbfs/mnt/raw/cities.csv')

# COMMAND ----------

for i in dbutils.fs.mounts():
    print(i.mountPoint)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt

# COMMAND ----------

# Path to test file
source_file = '/databricks-datasets/timeseries/Fires/Fire_Department_Calls_for_Service.csv'

# Define the input and output formats and paths and the table name.
write_format = 'delta'
partition_by = ['year', 'month', 'day', 'table_name']
table_name = 'delta_test_01'
save_path = '/mnt/raw/{table_name}'.format(table_name=table_name)

# COMMAND ----------

# reading top rows of the file with pandas
pd_df = pd.read_csv('/dbfs/databricks-datasets/timeseries/Fires/Fire_Department_Calls_for_Service.csv', nrows=100)
pd_df.head()

# COMMAND ----------

pd_df.dtypes

# COMMAND ----------

# creating schema for current file
schema = StructType() \
 .add('Call Number', StringType(), True) \
 .add('Unit ID', StringType(), True) \
 .add('Incident Number', IntegerType(), True) \
 .add('Call Type', StringType(), True) \
 .add('Call Date', StringType(), True) \
 .add('Watch Date', StringType(), True) \
 .add('Received DtTm', StringType(), True) \
 .add('Entry DtTm', StringType(), True) \
 .add('Dispatch DtTm', StringType(), True) \
 .add('Response DtTm', StringType(), True) \
 .add('On Scene DtTm', StringType(), True) \
 .add('Transport DtTm', StringType(), True) \
 .add('Hospital DtTm', StringType(), True) \
 .add('Call Final Disposition', StringType(), True) \
 .add('Available DtTm', StringType(), True) \
 .add('Address', StringType(), True) \
 .add('City', StringType(), True) \
 .add('Zipcode of Incident', StringType(), True) \
 .add('Battalion', StringType(), True) \
 .add('Station Area', StringType(), True) \
 .add('Box', StringType(), True) \
 .add('Original Priority', StringType(), True) \
 .add('Priority', StringType(), True) \
 .add('Final Priority', StringType(), True) \
 .add('ALS Unit', StringType(), True) \
 .add('Call Type Group', StringType(), True) \
 .add('Number of Alarms', IntegerType(), True) \
 .add('Unit Type', StringType(), True) \
 .add('Unit sequence in call dispatch', StringType(), True) \
 .add('Fire Prevention District', StringType(), True) \
 .add('Supervisor District', StringType(), True) \
 .add('Neighborhooods - Analysis Boundaries', StringType(), True) \
 .add('Location', StringType(), True) \
 .add('RowID', StringType(), True)

# COMMAND ----------

# Load file in a spark DataFrame
sp_df = spark \
            .read \
            .format('csv') \
            .option('header',True) \
            .option('nullValue', 'null') \
            .schema(schema) \
            .load(source_file) \
            #.limit(100)
sp_df.show()

# COMMAND ----------

sp_df.select('Received DtTm').take(3)

# COMMAND ----------

# Cast columns to Data or Datetime(timestamp) types.
# Create columns to partition data
sp_df1 = sp_df \
        .withColumn('Call Date', to_date(col('Call Date'), format='MM/dd/yyyy')) \
        .withColumn('Watch Date', to_date(col('Watch Date'), format='MM/dd/yyyy')) \
        .withColumn('Received DtTm', to_timestamp(col('Received DtTm'), format='MM/dd/yyyy hh:mm:ss a')) \
        .withColumn('Entry DtTm', to_timestamp(col('Entry DtTm'), format='MM/dd/yyyy hh:mm:ss a')) \
        .withColumn('Dispatch DtTm', to_timestamp(col('Dispatch DtTm'), format='MM/dd/yyyy hh:mm:ss a')) \
        .withColumn('Response DtTm', to_timestamp(col('Response DtTm'), format='MM/dd/yyyy hh:mm:ss a')) \
        .withColumn('On Scene DtTm', to_timestamp(col('On Scene DtTm'), format='MM/dd/yyyy hh:mm:ss a')) \
        .withColumn('Transport DtTm', to_timestamp(col('Transport DtTm'), format='MM/dd/yyyy hh:mm:ss a')) \
        .withColumn('Hospital DtTm', to_timestamp(col('Hospital DtTm'), format='MM/dd/yyyy hh:mm:ss a')) \
        .withColumn('Available DtTm', to_timestamp(col('Available DtTm'), format='MM/dd/yyyy hh:mm:ss a')) \
        .withColumn('year', date_format(col('Call Date'), format='yyyy')) \
        .withColumn('month', date_format(col('Call Date'), format='MM')) \
        .withColumn('day', date_format(col('Call Date'), format='dd')) \
        .withColumn('table_name', lit('fire_calls'))
sp_df1.show()

# COMMAND ----------

sp_df1.select('Received DtTm').take(3)

# COMMAND ----------

# Spaces on spaces (' ') are bot allowed. Renaming all headers and changing them to lowercase.
renaming_dict = {}
for i in sp_df1.columns:
    renaming_dict.update({i: i.lower().replace(' ', '_')})

for k, v in renaming_dict.items():
    sp_df1 = sp_df1.withColumnRenamed(existing=k,new=v)
sp_df1.show()

# COMMAND ----------

!ls /dbfs/mnt/raw

# COMMAND ----------

# Delete table from file system
#dbutils.fs.rm("dbfs:/mnt/raw/delta_test_01", recurse=True)

# COMMAND ----------

save_path

# COMMAND ----------

# Write the data to its target.
sp_df1.write \
    .format(write_format) \
    .partitionBy(partition_by) \
    .save(save_path)

# COMMAND ----------

# Create the table.
spark.sql("CREATE TABLE " + table_name + " USING DELTA LOCATION '" + save_path + "'")

# COMMAND ----------

#spark.sql("DROP TABLE " + 'dip_analytics')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC FROM delta_test_01
# MAGIC SELECT *
# MAGIC WHERE year=2019 AND month=7 AND day=8

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE delta_test_01