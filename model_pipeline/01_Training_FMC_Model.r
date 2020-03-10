# Databricks notebook source
# dbutils.widgets.text("lag_shift", "0", "Lag shift")

# COMMAND ----------

lag_shift <- as.numeric(dbutils.widgets.get("lag_shift"))
lag_shift

# COMMAND ----------

# MAGIC %md # Training model for FBB postpaid acquisition project
# MAGIC 
# MAGIC This notebokk illustrate the pipeline that use in model training. All the dependencies functions has been kept in `./func/`.
# MAGIC 
# MAGIC This training pipeline consist of steps as below...
# MAGIC 1. Data labeling : Get the label for supervised learning
# MAGIC 2. Data preparation : Get all the relevance features in multiple data sources
# MAGIC 3. Feature enhancing : Derive features has been made to represent time series
# MAGIC 4. Data cleansing : Impute missing values and regroup the values
# MAGIC 5. Model training : Training model by H20 lib

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

# MAGIC %md ## 1: Data labeling step

# COMMAND ----------

# DBTITLE 1,Loading labeling function
# MAGIC %run ./func/01_labeling_func

# COMMAND ----------

response <- labeling_data(month_lag=1 + lag_shift)

# COMMAND ----------

sdf_dim(response)

# COMMAND ----------

glimpse(response)

# COMMAND ----------

# MAGIC %md ## 2: Data preparation step

# COMMAND ----------

# MAGIC %run ./func/02_Preparation_func

# COMMAND ----------

dt_prep <- prep_data(response, lag_time = 4 + lag_shift, training = T)

# COMMAND ----------

sdf_dim(dt_prep)

# COMMAND ----------

glimpse(dt_prep)

# COMMAND ----------

# MAGIC %md ## 3: Feature enhancing step

# COMMAND ----------

dt_prep_enh <- feat_enhance(dt_prep, his_time = 6 + lag_shift, training = T)

# COMMAND ----------

sdf_dim(dt_prep_enh)

# COMMAND ----------

glimpse(dt_prep_enh)

# COMMAND ----------

# MAGIC %md ## 3: Data cleansing step

# COMMAND ----------

# DBTITLE 1,Imputing and regroup
dt_cleansed <- dt_cleansing(dt_prep_enh,training = T)

# COMMAND ----------

# DBTITLE 1,Derive features
dt_final <- feat_derive(dt_cleansed)

# COMMAND ----------

sdf_dim(dt_final)

# COMMAND ----------

glimpse(dt_final)

# COMMAND ----------

count(dt_final)

# COMMAND ----------

# DBTITLE 1,Writing single table
spark_write_table(dt_final, "mck_fmc.te01_postpaid_training", mode = "overwrite")

# COMMAND ----------

# MAGIC %md ## 5: Model training

# COMMAND ----------

# MAGIC %run ./func/03_Model_func

# COMMAND ----------

library(mlflow)
install_mlflow()

# COMMAND ----------

model_path <- "/mnt/cvm02/user/pitchaym/spark_model_fbb/"

# COMMAND ----------

# ML flow start log
mlflow_start_run()

gbt_model <- model_training(dt_final, lag_time = 4 + lag_shift)

# ML flow end log
mlflow_end_run()

# COMMAND ----------

# MAGIC %fs rm -r /mnt/cvm02/user/pitchaym/spark_model_fbb/

# COMMAND ----------

ml_save(gbt_model, "/mnt/cvm02/user/pitchaym/spark_model_fbb/")