# Databricks notebook source
# MAGIC %md
# MAGIC ## Packages

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install packages

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install adal

# COMMAND ----------

# MAGIC %md
# MAGIC ### Download packages from github

# COMMAND ----------

! git clone https://github.com/YousefGh/kmeans-feature-importance.git
# Load python functions
exec(open('./kmeans-feature-importance/kmeans_interp/kmeans_feature_imp.py', encoding='utf-8').read())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import packages

# COMMAND ----------

import os
import re
import numpy as np
import pandas as pd
import datetime

import pyspark.sql.functions as pyssql_func
import pyspark.sql.types as pyssql_types
import pyspark.ml.feature as pysml_feature 
import pyspark.sql.window as pyssql_window
import pyspark
import delta.tables as delta_tbl
import shutil
import adal

import re

# COMMAND ----------

# MAGIC %md
# MAGIC ## Arguments Setup

# COMMAND ----------

# MAGIC %run
# MAGIC /path_to_where_you_placed_your_config_notebook/config

# COMMAND ----------

# get the arguments commonly used from your config notebook
user_email_path = config.get('user_email_path') 

# custom arguments
project_name = 'testing'

# COMMAND ----------

## Arguments for scheduled jobs
# add argument widgets
dbutils.widgets.text("data_month", "")
# # remove argument widgets
# dbutils.widgets.removeAll()

# Get arguments
if dbutils.widgets.get("data_month") == "": # Check if exist
  dbutils.notebook.exit("Please add an argument of a process, data_month")
else: 
  data_month = dbutils.widgets.get("data_month")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install customized functions

# COMMAND ----------

# MAGIC %run
# MAGIC /path_to_where_you_placed_your_functions/db_connection

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connection to database

# COMMAND ----------

pushdown_query = "db_schema.db_tableA"
tableA = spark.read\
.jdbc(
    url = db_access.jdbcUrl, 
    table=pushdown_query, 
    properties = db_access.connectionProperties
)\
.select(
    pyssql_func.trim('original_colA').alias('new_colA'),
    pyssql_func.trim('original_colB').alias('new_colB')
).limit(10).dropDuplicates()
display(tableA)

# COMMAND ----------

# MAGIC %md
# MAGIC # Directory

# COMMAND ----------

# create dir for project
project_dir = f'dbfs:/FileStore/{user_email_path}/Project/{project_name}'
upload_path = f'{project_dir}/upload/'
download_path = f'{project_dir}/download/'
input_path = f'{project_dir}/input/'
output_path = f'{project_dir}/output/'

dbutils.fs.mkdirs(upload_path)
dbutils.fs.mkdirs(download_path)
dbutils.fs.mkdirs(input_path)
dbutils.fs.mkdirs(output_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Common util

# COMMAND ----------

# MAGIC %md
# MAGIC ### List Directory

# COMMAND ----------

display(dbutils.fs.ls(f'dbfs:/FileStore/{user_email_path}/Project/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Copy Files

# COMMAND ----------

dbutils.fs.cp(
    f'{upload_path}/tableA.csv',
    f'{input_path}/tableA.csv',
    True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Move Files

# COMMAND ----------

dbutils.fs.mv(
    f{upload_path}/tableA.csv',
    f'{output_path}/tableA.csv',
    True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read files

# COMMAND ----------

# read pickle
df = pd.read_pickle(re.sub(':', '', f'/{input_path}/tableA.pkl', compression = 'gzip'))

# COMMAND ----------

# Using spark to read csv
df = spark.read.format('csv').option('header', 'true').option('delimiter', '\t').load(f'/mnt/data_folder/*/*').limit(5)

# COMMAND ----------

# Read data through Hive
from pyspark.sql import SparkSession, HiveContext,DataFrameWriter
from pyspark import SparkContext, SparkConf
conf = SparkConf() #Declare spark conf variable\
conf.setAppName("Read-and-write-data-to-Hive-table-spark")
sc = SparkContext.getOrCreate(conf=conf)
#Instantiate hive context class to get access to the hive sql method
hc = HiveContext(sc)
hive_context= HiveContext(sc)

table_path = 'db_schema.db_tableA'
df = hive_context.sql(("select * from {}").format(table_path))

# COMMAND ----------

# Convert to pandas DF
df = df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write files

# COMMAND ----------

# save spark df file
spark_df.coalesce(1).write.mode('overwrite').format('csv').save(f'{output_path}/spark_df', header='true')

# COMMAND ----------

# save csv
tableA.to_csv(re.sub(':', '', f'/{output_path}/tableA.csv'), index=False)
# save pickle
tableA.to_pickle(re.sub(':', '', f'/{output_path}/tableA.pkl'))

# COMMAND ----------

# zip the whole directory
import shutil
sourcePath = re.sub(':', '', f'/{project_dir}')
filename = 'project_zip'
zipPath = f'./{filename}'
shutil.make_archive(zipPath, 'zip', sourcePath)
# os.listdir('.')
shutil.move(f'{filename}.zip', f'{sourcePath}/{filename}.zip')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete file

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/your_email@company.com/Project/testing/project_zip.zip",True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Download file to local laptop
# MAGIC https://xxxxxxxxxxxxxxxxxxxx.azuredatabricks.net/files/your_email@company.com/Project/testing/project_zip.zip
