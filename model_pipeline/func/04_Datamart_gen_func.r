# Databricks notebook source
# MAGIC %md
# MAGIC ### Datamart generation function
# MAGIC 
# MAGIC These notebook consist of function that generate the data that will be utilized in model features below
# MAGIC 1. `hh_gen()` : Generating household function

# COMMAND ----------

hh_gen <- function(lag_time = 1) {
  
  output_tbl <- "mck_fmc.hh_aggregation"
  end_month <- ceiling_date(today() %m-% months(lag_time), unit="month") %m-% days(1)
  
  # Loading data
  hh <- tbl(sc, "prod_raw.dm302_mobile_post_pre_household")
  
  # Checkine latest data
  hh_test <- tbl(sc, output_tbl)
  
  hh_test %>%
  distinct(ddate) %>%
  arrange(desc(ddate)) %>%
  top_n(1) %>%
  collect() -> latest_ddate
  latest_ddate <- latest_ddate$ddate[1]
  
  if (end_month > latest_ddate) {
    
    # Filter selected month
    hh %>% filter(ddate == end_month) -> hh
    
    hh %>%
    group_by(home_analytic_id, geo_home_shape_id) %>%
    summarise(total_household_arpu = sum(arpu), 
         average_household_arpu = mean(arpu), 
         max_household_arpu = max(arpu), 
         min_household_arpu = min(arpu), 
         std_household_arpu = sd(arpu), 
         total_household_voice_minute = sum(total_voice_minute), 
         average_household_voice_minute = mean(total_voice_minute), 
         max_household_voice_minute = max(total_voice_minute), 
         min_household_voice_minute = min(total_voice_minute), 
         std_household_voice_minute = sd(total_voice_minute), 
         total_household_data_usage = sum(total_data_usage), 
         average_household_data_usage = mean(total_data_usage), 
         max_household_data_usage = max(total_data_usage), 
         min_household_data_usage = min(total_data_usage), 
         std_household_data_usage = sd(total_data_usage), 
         average_household_age = mean(ma_age), 
         max_household_age = max(ma_age), 
         min_household_age = min(ma_age), 
         std_household_age = sd(ma_age), 
         average_household_service_month = mean(service_month), 
         max_household_service_month = max(service_month), 
         min_household_service_month = min(service_month), 
         std_household_service_month = sd(service_month), 
         household_person_count = n()) -> hh_features
    
    hh %>%
    select(analytic_id, register_date, home_analytic_id, geo_home_shape_id) %>%
    left_join(hh_features, by = c("home_analytic_id", "geo_home_shape_id")) %>%
    mutate(ddate = as.Date(end_month)) -> hh_final
    
    spark_write_table(hh_final, output_tbl, mode = "append")
    
    print(paste("New month(ddate) has been appended :", end_month))
    
    hh_final %>%
    distinct(ddate) %>%
    arrange(desc(ddate)) %>%
    top_n(5)
    
  } else {
    
    print(paste(output_tbl, "on", end_month, "already existed"))
    
  }
  
}