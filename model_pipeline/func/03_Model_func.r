# Databricks notebook source
# MAGIC %md
# MAGIC ### Model function
# MAGIC 
# MAGIC Model function utilized H2O library. It's mandatory to attached the cluster that prepared for H2O.
# MAGIC This notebook consist with list of functions below.
# MAGIC 1. `model_training()` : Train the model by H2O XGboost
# MAGIC 2. `model_scoring()` : Scoring the model from the model object
# MAGIC 3. `list_export()` : Export the list for campaign

# COMMAND ----------

model_training <- function (dt_input, lag_time = 4, sample_frac = 0.5) {
  
  # Logging month profile
  month_end <- ceiling_date(today() %m-% months(lag_time), unit="month") %m-% days(1)
  mlflow_log_param("Profile Month", paste(month_end, "-", month_end %m+% months(1)))
  
  # Sample 0 label
  dt_input %>%
  filter(camp_response == 0) %>%
  sdf_sample(fraction = 0.5, replacement = F) -> dt_0
  
  dt_input %>%
  filter(camp_response == 1) -> dt_1
  
  dt_down <- sdf_bind_rows(dt_0, dt_1)
  
  dt_hex <- as_h2o_frame(sc, dt_down)
  
  # Casting ---------------------
  # Label
  dt_hex$camp_response <- as.factor(dt_hex$camp_response)

  # Predictor
  dt_hex$mnp_flag <- as.factor(dt_hex$mnp_flag)
  dt_hex$gender <- as.factor(dt_hex$gender)
  dt_hex$handset_os <- as.factor(dt_hex$handset_os)
  dt_hex$serenade <- as.factor(dt_hex$serenade)
  dt_hex$top30_province <- as.factor(dt_hex$top30_province)
  dt_hex$new_pack_cat <- as.factor(dt_hex$new_pack_cat)
  
  # Define relevance variables
  var_list <- h2o.names(dt_hex)
  ommit_var <- c(-1:-5)
  var_model <- var_list[ommit_var]
  
  # Logging feature
  write(var_model, file = "fmc_var.txt")
  mlflow_log_artifact("fmc_var.txt")
  
  # Splitting data frame
  dt.split <- h2o.splitFrame(dt_hex, ratios = 0.75, seed = 567)
  
  dt.train <- dt.split[[1]]
  dt.test <- dt.split[[2]]
  
  
  # Hyper parameter list
  ntrees <- 400
  learn_rate <- 0.01
  max_depth <- 8
  sample_rate <- 0.8
  col_sample_rate <- 0.61
  
  # Logging hyper parameter
  mlflow_log_param("ntrees", ntrees)
  mlflow_log_param("learn_rate", learn_rate)
  mlflow_log_param("max_depth", max_depth)
  mlflow_log_param("sample_rate", sample_rate)
  mlflow_log_param("col_sample_rate", col_sample_rate)
  
  best_xg <- h2o.xgboost(training_frame = dt.train , 
                          x = var_model , 
                          y = "camp_response" , 
                          validation_frame = dt.test , 
                          ntrees = ntrees ,
                          learn_rate = learn_rate ,
                          max_depth = max_depth ,
                          sample_rate = sample_rate ,
                          col_sample_rate = col_sample_rate , 
                          seed = 567
                         )
  
  xg_perf <- h2o.performance(best_xg, newdata = dt.test)
  fmc_auc <- h2o.auc(xg_perf)
  print(paste("AUC is :", fmc_auc))
  
  # Logging metrics
  mlflow_log_metric("AUC", fmc_auc)
  
  png("fmc_auc.png")
  plot(xg_perf,type='roc')
  dev.off()
  mlflow_log_artifact("fmc_auc.png")
  
  write_csv(h2o.varimp(best_xg), "fmc_varimp.csv")
  mlflow_log_artifact("fmc_varimp.csv")
  
  write_csv(h2o.gainsLift(best_xg, newdata = dt.test), "fmc_lift.csv")
  mlflow_log_artifact("fmc_lift.csv")
  
  return(best_xg)
}

# COMMAND ----------

model_scoring <- function(dt_input, fraud_cut = 1, 
                         model_path = "/dbfs/mnt/cvm02/user/pitchaym/h2o_model_fbb/") {
  
  fraud_table <- "default.fbb_fraud_model_output"
  gen_dir <- "/dbfs/mnt/cvm02/cvm_output/MCK/FMC/CAMPAIGN/h2o"
  
  target_model <- list.files(model_path, pattern = "model")
  target_model <- paste0(model_path, target_model)
  
  classifier <- h2o.loadModel(target_model)
  
  # Finding latest fraud score
  if (fraud_cut > 0) {
    fraud <- tbl(sc, fraud_table)

    fraud %>%
    mutate(ddate = to_date(ddate)) -> fraud

    fraud %>%
    distinct(ddate) %>%
    mutate(ddate = to_date(ddate)) %>%
    arrange(desc(ddate)) %>%
    top_n(1) %>%
    collect() -> fraud_latest

    fraud_latest <- fraud_latest$ddate

    fraud %>%
    filter(ddate == fraud_latest, charge_type == "Post-paid", 
    fraud_decile <= fraud_cut) -> fraud_ex
    sdf_register(fraud_ex, "fraud_ex")
    tbl_cache(sc, "fraud_ex")

    # Exclude fraud people and convert register date type
    dt_input %>%
    anti_join(fraud_ex, by = c("analytic_id", "register_date")) %>%
    mutate(register_date = as.character(register_date)) -> dt_score
    
  } else {
    dt_input %>%
    mutate(register_date = as.character(register_date)) -> dt_score
  }
  
  count(dt_score) %>%
  collect() -> temp
  temp <- temp$n
  print(paste("Total rows of data after filter :", temp))
  
  # Casting variables
  dt_score_hex <- as_h2o_frame(sc, dt_score)
  
  dt_score_hex$mnp_flag <- as.factor(dt_score_hex$mnp_flag)
  dt_score_hex$gender <- as.factor(dt_score_hex$gender)
  dt_score_hex$handset_os <- as.factor(dt_score_hex$handset_os)
  dt_score_hex$serenade <- as.factor(dt_score_hex$serenade)
  dt_score_hex$top30_province <- as.factor(dt_score_hex$top30_province)
  dt_score_hex$new_pack_cat <- as.factor(dt_score_hex$new_pack_cat)
  
  pred <- h2o.predict(object = classifier, newdata = dt_score_hex)
  pred$p1 <- round(pred$p1, digit = 4)
  pred <- h2o.cbind(dt_score_hex[,c("crm_sub_id", "analytic_id", "register_date")],pred[,"p1"])
  
  unlink(gen_dir, recursive= T)
  
  h2o.exportFile(pred, gen_dir, parts = -1)
  
}

# COMMAND ----------

list_export <- function(path = "/mnt/cvm02/cvm_output/MCK/FMC/CAMPAIGN/h2o/", 
                       gen_dir = "/mnt/cvm02/cvm_output/MCK/FMC/CAMPAIGN/") {
  
  dt <- spark_read_csv(sc, "score", path, delimiter = ",")
  
  dt %>%
  rename(fmc_score = p1) %>%
  mutate(register_date = to_date(register_date), 
        fmc_decile = ntile(fmc_score, 10),
        fmc_percentile = ntile(fmc_score, 100),
        fmc_decile = 11-fmc_decile, 
        fmc_percentile = 101-fmc_percentile,
        charge_type = "Post-paid") -> dt
  
  dt %>%
  sdf_coalesce(1) -> dt_export
  
  spark_write_csv(dt_export, paste0(gen_dir,"/post"), delimiter="|")
  target_file <- list.files(paste0("/dbfs", gen_dir, "/post/"), pattern = "csv$")
  
  file_name <- format(Sys.Date(), "%Y%m")
  file_name <- paste0("FMC_post_" ,file_name, "_SCORE.txt")
  
  file.rename(from = file.path(paste0("/dbfs", gen_dir, "/post/", target_file)), 
            to = file.path(paste0("/dbfs", gen_dir, file_name)))
  
  unlink(paste0("/dbfs", gen_dir, "/post/"), recursive = T)
  unlink(paste0("/dbfs", gen_dir, "/h2o/"), recursive = T)
  
  print(paste(file_name, "has been written"))
  glimpse(dt)
  
}