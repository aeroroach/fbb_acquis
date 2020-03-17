# Databricks notebook source
lag_shift <- as.numeric(dbutils.widgets.get("lag_shift"))

# Production
# dbutils.widgets.remove("lag_shift")
# lag_shift <- 0 

lag_shift

# COMMAND ----------

# DBTITLE 1,Delay time
# delay <- function(x)
# {
#     p1 <- proc.time()
#     Sys.sleep(x)
#     proc.time() - p1 # The cpu usage should be negligible
# }
# delay(240)

# COMMAND ----------

# MAGIC %md # Scoring data for FBB postpaid acquisition project
# MAGIC 
# MAGIC This notebokk illustrate the pipeline of new data scoring. All the dependencies functions has been kept in `./func/`.
# MAGIC 
# MAGIC This scoring pipeline consist of steps as below...
# MAGIC 1. Generating Household data : Aggregating GEO analytics household behaviors
# MAGIC 2. Data preparation : Get all the relevance features in multiple data sources
# MAGIC 3. Feature enhancing : Derive features has been made to represent time series
# MAGIC 4. Data cleansing : Impute missing values and regroup the values
# MAGIC 5. Data scoring : Scoring by latest saved model
# MAGIC 6. Writing output : Writing text file for campaign utilization

# COMMAND ----------

# DBTITLE 1,Library loading
library(dplyr)
library(lubridate)
library(sparklyr)
library(readr)

# COMMAND ----------

# DBTITLE 1,Initiate spark connection
sc <- spark_connect(method = "databricks")

# COMMAND ----------

# MAGIC %md ## 1: Datamart generation step

# COMMAND ----------

# MAGIC %run ./func/04_Datamart_gen_func

# COMMAND ----------

hh_gen(lag_time = 1 + lag_shift)

# COMMAND ----------

# MAGIC %md ## 2: Data preparation step

# COMMAND ----------

# MAGIC %run ./func/02_Preparation_func

# COMMAND ----------

dt_prep <- prep_data(dt_input = NULL, lag_time = 1 + lag_shift, training = F)

# COMMAND ----------

sdf_dim(dt_prep)

# COMMAND ----------

# MAGIC %md ## 3: Feature enhancing step

# COMMAND ----------

dt_prep_enh <- feat_enhance(dt_prep, his_time = 3 + lag_shift, training = F)

# COMMAND ----------

sdf_dim(dt_prep_enh)

# COMMAND ----------

# MAGIC %md ## 3: Data cleansing step

# COMMAND ----------

dt_cleansed <- dt_cleansing(dt_prep_enh, training = F)

# COMMAND ----------

dt_final <- feat_derive(dt_cleansed)

# COMMAND ----------

sdf_dim(dt_final)

# COMMAND ----------

glimpse(dt_final)

# COMMAND ----------

# MAGIC %md ## 5: Model training

# COMMAND ----------

library(h2o)
library(rsparkling)

# COMMAND ----------

# MAGIC %run ./func/03_Model_func

# COMMAND ----------

# DBTITLE 1,Initiate H2O context
h2o_context(sc)

# COMMAND ----------

model_scoring(dt_final, fraud_cut = 0)

# COMMAND ----------

# MAGIC %fs ls /mnt/cvm02/cvm_output/MCK/FMC/CAMPAIGN/h2o/

# COMMAND ----------

# MAGIC %md ## 6: Export files

# COMMAND ----------

list_export()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/cvm02/cvm_output/MCK/FMC/CAMPAIGN/

# COMMAND ----------

# MAGIC %fs
# MAGIC head dbfs:/mnt/cvm02/cvm_output/MCK/FMC/CAMPAIGN/FMC_post_202003_SCORE.txt