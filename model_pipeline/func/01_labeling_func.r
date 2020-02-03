# Databricks notebook source
# MAGIC %md 
# MAGIC ### Data labeling function
# MAGIC 
# MAGIC `labeling_data()`
# MAGIC 
# MAGIC Table sources
# MAGIC + `prod_delta.fbb_x_sell_for_serenade` - Campaign label and gathering latest months of campaign data
# MAGIC 
# MAGIC There's some issues that need to be taken care of
# MAGIC + Duplicate of `crm_sub_id` and `contact_status` : It's joinig problem from other table that can be solved by distinct both column.
# MAGIC + Same `crm_sub_id` but different `contact_status` : There's an multiple contact from outbound. Need to keep "Closure" response as priority and drop the rest.

# COMMAND ----------

labeling_data <- function (in_table = "prod_delta.fbb_x_sell_for_serenade", 
                           month_lag = 1) {
  
  # composite table
  prof_table <- "prod_delta.dm07_sub_clnt_info"
  
  # Target month
  label_begin <- floor_date(today() %m-% months(month_lag + 1), unit="month")
  label_end <- ceiling_date(today() %m-% months(month_lag), unit="month") %m-% days(1)
  print(paste("Target begin month is :", label_begin))
  print(paste("Target end month is :", label_end))
  
  # Loading table
  response <- tbl(sc, in_table)
  prof <- tbl(sc, prof_table)
  
  # Wrangling campaign label
  response %>% 
  filter(last_upd >= label_begin, last_upd <= label_end) %>%
  mutate(cam_month = last_day(last_upd)) %>%
  filter(contact_status %in% c("Success Closure", "Success", "Success Inbound", "Closure Inbound")) %>% 
  mutate(camp_response = ifelse(contact_status %in% c("Success Closure", "Closure Inbound"), 1, 0), 
         last_upd = to_date(last_upd)) %>%
  select(crm_sub_id, camp_response, cam_month) %>%
  distinct(crm_sub_id, camp_response, cam_month) -> response
  
  # Filtering duplication sub
  count(response, crm_sub_id) -> count_sub
  
  response %>%
  left_join(count_sub, by = "crm_sub_id") %>%
  filter(!(camp_response == 0 & n > 1)) %>%
  select(-n) %>%
  group_by(crm_sub_id, camp_response) %>%
  arrange(cam_month) %>%
  top_n(-1) %>% 
  mutate(prof_month = add_months(cam_month, -2)) -> response_clean
  
  # Analytic ID mapping
  response_clean %>%
  ungroup() %>%
  distinct(prof_month) %>%
  collect() -> prof_month
  prof_month <- prof_month$prof_month
  prof_month
  
  prof %>%
  filter(ddate %in% prof_month) %>%
  select(crm_sub_id, analytic_id, ddate) -> prof
  
  response_clean %>%
  left_join(prof, by=c("crm_sub_id", "prof_month"="ddate")) %>%
  ungroup() -> response_clean
  
  sdf_register(response_clean, "response_tbl")
  tbl_cache(sc, "response_tbl")
  
  return(response_clean)
  
}