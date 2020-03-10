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
  
  # Splitting data frame
  dt_down %>%
  sdf_partition(train = 0.75, test = 0.25, seed = 789) -> partition
  
  sdf_register(partition$train, "train_set")
  tbl_cache(sc, "train_set")
  
  # Hyper parameter list
  max_iter <- 400
  step_size <- 0.01
  max_depth <- 8
  subsampling_rate <- 0.8
  
  # Logging hyper parameter
  mlflow_log_param("max_iter", max_iter)
  mlflow_log_param("step_size", step_size)
  mlflow_log_param("max_depth", max_depth)
  mlflow_log_param("subsampling_rate", subsampling_rate)
  
  gbt_model <- ml_gbt_classifier(partition$train, 
                               camp_response ~ . -analytic_id -crm_sub_id -national_id -register_date, 
                               step_size = step_size,
                               max_depth = max_depth, 
                               subsampling_rate = subsampling_rate,
                               max_iter = max_iter
                               )
  
  # Validate model result
  pred <- ml_predict(gbt_model, partition$test)
  pred %>%
  select(analytic_id, register_date, camp_response, probability_1) -> pred
  
  gbt_auc <- ml_binary_classification_eval(pred_mod, "camp_response", "probability_1", metric = "areaUnderROC")
  print(paste("AUC is :", gbt_auc))
  
  # Logging AUC
  mlflow_log_metric("AUC", gbt_auc)
  
  # Get prior prob
  pred_mod %>%
  summarise(prior_prob = mean(camp_response)) %>%
  collect() -> prior_prob

  prior_prob <- prior_prob$prior_prob
  
  # Calculate lift table and logging
  pred %>%
  rename(score = probability_1) %>%
  mutate(decile = ntile(score, n=10), 
         decile = 11-decile) %>%
  group_by(decile) %>%
  summarise(no_subs = n(), 
            lower_bound = min(score),
            no_response = sum(camp_response),
            prop_response = mean(camp_response),
           ) %>%
  arrange(decile) %>%
  mutate(cum_sub = cumsum(no_subs),
         cum_response = cumsum(no_response),
         cum_response_rate = cum_response/cum_sub,
         lift = cum_response_rate/prior_prob) %>%
  collect() -> lift_tbl
  write_csv(lift_tbl, "fmc_lift.csv")
  mlflow_log_artifact("fmc_lift.csv")      
  
  # Plotting ROC curve and logging
  library(pROC)
  pred_mod %>%
  collect() -> pred_R
        
  png("fmc_auc.png")
  roc(pred_R$camp_response,pred_R$probability_1,
    smoothed = TRUE,
    # arguments for ci
    ci=TRUE, ci.alpha=0.95, stratified=FALSE,
    # arguments for plot
    plot=TRUE, auc.polygon=TRUE, grid=TRUE,
    print.auc=TRUE, show.thres=TRUE,
    thresholds = "best", print.thres="best"
    )
  dev.off()
  mlflow_log_artifact("fmc_auc.png")
  
  var_im <- ml_feature_importances(gbt_model)
  write_csv(var_im, "fmc_varimp.csv")
  mlflow_log_artifact("fmc_varimp.csv")
  
  return(gbt_model)
}

# COMMAND ----------

model_scoring <- function(dt_input, fraud_cut = 0, 
                         model_path = "/mnt/cvm02/user/pitchaym/spark_model_fbb/") {
  
  fraud_table <- "default.fbb_fraud_model_output"
  
  gbt_pipeline <- ml_load(sc, model_path)
  
  # Finding latest fraud score
  if (fraud_cut > 0) {
    fraud <- tbl(sc, fraud_table)

    fraud %>%
    mutate(ddate = to_date(ddate)) -> fraud

    fraud %>%
    distinct(ddate) %>%
    mutate(ddate = to_date(ddate)) %>%
    filter(ddate == max(ddate)) %>%
    collect() -> fraud_latest

    fraud_latest <- fraud_latest$ddate

    fraud %>%
    filter(ddate == fraud_latest, charge_type == "Post-paid", 
    fraud_decile <= fraud_cut) -> fraud_ex
    sdf_register(fraud_ex, "fraud_ex")
    tbl_cache(sc, "fraud_ex")

    # Exclude fraud people and convert register date type
    dt_input %>%
    anti_join(fraud_ex, by = c("analytic_id", "register_date")) -> dt_score
    
  } else {
    dt_score <- dt_input
  }
  
  count(dt_score) %>%
  collect() -> temp
  temp <- temp$n
  print(paste("Total rows of data after filter :", temp))
  
  # Prediction
  pred <- ml_transform(gbt_pipeline, dt)
  
  pred %>%
  select(crm_sub_id, analytic_id, register_date, probability) %>%
  sdf_separate_column("probability", c("prob_no", "fmc_score")) %>%
  select(-probability, -prob_no) -> pred_extract

  return(pred_extract)
  
}

# COMMAND ----------

list_export <- function(dt_input, 
                       gen_dir = "/mnt/cvm02/cvm_output/MCK/FMC/CAMPAIGN/") {
  
  dt_input %>%
  mutate(register_date = to_date(register_date), 
        fmc_decile = ntile(fmc_score, 10),
        fmc_percentile = ntile(fmc_score, 100),
        fmc_decile = 11-fmc_decile, 
        fmc_percentile = 101-fmc_percentile,
        charge_type = "Post-paid",
        model = "cvm") -> dt
  
  dt %>%
  sdf_coalesce(1) -> dt_export
  
  spark_write_csv(dt_export, paste0(gen_dir,"/post"), delimiter="|")
  target_file <- list.files(paste0("/dbfs", gen_dir, "/post/"), pattern = "csv$")
  
  file_name <- format(Sys.Date(), "%Y%m")
  file_name <- paste0("FMC_post_" ,file_name, "_SCORE.txt")
  
  file.rename(from = file.path(paste0("/dbfs", gen_dir, "/post/", target_file)), 
            to = file.path(paste0("/dbfs", gen_dir, file_name)))
  
  unlink(paste0("/dbfs", gen_dir, "/post/"), recursive = T)
  
  print(paste(file_name, "has been written"))
  glimpse(dt)
  
}