# Databricks notebook source
# MAGIC %md
# MAGIC ### Data preparation functions
# MAGIC 
# MAGIC This notebook consist with list of functions below
# MAGIC 1. `prep_data()` : Gathering data from relevance sources and join into single table
# MAGIC 2. `feat_enhance()` : Enriching features by derive the meaningful time series
# MAGIC 3. `dt_cleansing()` : Imputing and regroup categories variables
# MAGIC 4. `feat_derive()` : Calculating derive time series features

# COMMAND ----------

# DBTITLE 1,Data prep function
prep_data <- function(dt_input = NULL, 
                     lag_time = 4, 
                     training = T) {

  begin_month <- floor_date(today() %m-% months(lag_time), unit="month")
  # Target month
  if (training == T) {
  end_month <- ceiling_date(today() %m-% months(lag_time -1), unit="month") %m-% days(1)
  } else {
  end_month <- ceiling_date(today() %m-% months(lag_time), unit="month") %m-% days(1)
  }

  print(paste("Target month begin is :", begin_month))
  print(paste("Target month begin is :", end_month))
  
  # List of original source tables
  prof_table <- "prod_delta.dm07_sub_clnt_info"
  app_table <- "prod_delta.dm09041_du_sum_usage_by_app_monthly"
  usage_table <- "prod_delta.dm09_mobile_day_split"
  hh_aggregation_table <- "mck_fmc.hh_aggregation"
  pack_info_table <- "prod_delta.dm26_postpaid_pack_info"
  fbb_table <- "prod_delta.dm2100_fbb_active_sub"
  
  # Export directory
  top_app_master <- "/dbfs/mnt/cvm02/user/pitchaym/share/top_app_master.csv"
  top_app_training <- "/dbfs/mnt/cvm02/user/pitchaym/share/top_app_training.csv"
  
  # Loading source data
  prof <- tbl(sc, prof_table)
  usage <- tbl(sc, usage_table)
  house <- tbl(sc, hh_aggregation_table)
  pack_info <- tbl(sc, pack_info_table)
  app_soc <- tbl(sc, app_table)
  
  # Minimizing data size ---------------------------
  
  # SOC top app list
  if (training == T) {
    
    top_app <- read_csv(top_app_master)
    top_app <- top_app$top_app
    
  } else {
    top_app <- read_csv(top_app_training)
    top_app <- top_app$training_app_list
  }
  
  # Filter date and relevance subs
  if (training == T) {
    
    prof %>% filter(charge_type == "Post-paid", (ddate >= begin_month & ddate <= end_month), 
                    mobile_status == "Active") %>% 
    semi_join(dt_input, by = c("crm_sub_id", "ddate"="prof_month")) -> prof
    
    # Filter sub
    prof %>%
    select(analytic_id, ddate) -> fil_id
    
    sdf_register(fil_id, "fil_id")
    tbl_cache(sc, "fil_id")
    
    app_soc %>%
    filter(ddate >= begin_month, ddate <= end_month, application %in% top_app) %>%
    semi_join(fil_id, by = "analytic_id") %>%
    mutate(vol_sum_kb = volume_ul_kb + volume_dw_kb) -> app_soc
    
    # Writing training app
    app_soc %>%
    distinct(application) %>%
    collect() -> training_app_list
    training_app_list <- training_app_list$application
    write_csv(data.frame(training_app_list), top_app_training)
    
    usage %>%
    filter(ddate >= begin_month, ddate <= end_month, 
            hour_id >= 0, hour_id <= 23) %>%
    semi_join(fil_id, by = "analytic_id") -> usage
    
    pack_info %>% 
    filter(ddate >= begin_month & ddate <= end_month) %>% 
    semi_join(fil_id, by = c("analytic_id", "ddate")) -> pack_info
    
    house %>% 
    filter(ddate >= begin_month & ddate <= end_month) %>% 
    semi_join(fil_id, by = c("analytic_id", "ddate")) -> house
    
  } else {
    
    # Exclude existing FBB
    fbb <- tbl(sc, fbb_table)
    fbb %>%
    distinct(ddate) %>%
    filter(ddate == max(ddate)) %>%
    collect() -> current_fbb
    current_fbb <- current_fbb$ddate[1]
    
    fbb %>%
    filter(ddate == current_fbb) %>%
    select(national_id) -> fbb
    
    prof %>% 
    filter(charge_type == "Post-paid", (ddate == end_month), mobile_status == "Active") %>% 
    anti_join(fbb, by = "national_id") -> prof
    
    sdf_register(prof, "prof")
    tbl_cache(sc, "prof")

    app_soc %>%
    filter(ddate == end_month, application %in% top_app) %>%
    semi_join(prof, by ="analytic_id") %>%
    mutate(vol_sum_kb = volume_ul_kb + volume_dw_kb) -> app_soc
    
    usage %>%
    filter(ddate >= begin_month, ddate <= end_month, 
            hour_id >= 0, hour_id <= 23) %>%
    semi_join(prof, by = "analytic_id") -> usage
    
    pack_info %>% filter(ddate == end_month) %>% semi_join(prof, by = c("analytic_id", "ddate")) -> pack_info
    house %>% filter(ddate == end_month) %>% semi_join(prof, by = c("analytic_id", "ddate")) -> house
    
  }
  
  
  # Prep APP SOC table ---------------------------
  
    sdf_register(app_soc, "app_soc")
    tbl_cache(sc, "app_soc")

    app_soc %>%
    mutate(application = paste0(application, "_duration")) %>%
    sdf_pivot(analytic_id + ddate ~ application, list(duration_sec="sum")) %>%
    na.replace(0) -> app_pivot_duration

    app_soc %>%
    mutate(application = paste0(application, "_count")) %>%
    sdf_pivot(analytic_id + ddate ~ application, list(time_cnt="sum")) %>%
    na.replace(0) -> app_pivot_count

    app_soc %>%
    mutate(application = paste0(application, "_vol")) %>%
    sdf_pivot(analytic_id + ddate ~ application, list(vol_sum_kb="sum")) %>%
    na.replace(0) -> app_pivot_day

    app_pivot_duration %>%
    full_join(app_pivot_count, by = c("analytic_id", "ddate")) %>%
    full_join(app_pivot_day, by = c("analytic_id", "ddate"))-> app_pivot_all

    sdf_register(app_pivot_all, "app_pivot_all")
    tbl_cache(sc, "app_pivot_all")

    # Prep usage table ---------------------------

    sdf_register(usage, "usage")
    tbl_cache(sc, "usage")

    usage %>%
    mutate(date = to_date(date_id)) -> usage

    usage %>%
    mutate(wday = date_format(date, "EEE"),
          period = case_when(hour_id > 0 & hour_id <= 5 ~ "night", 
                              hour_id > 5 & hour_id <= 12 ~ "morning", 
                              hour_id > 12 & hour_id <= 18 ~ "afternoon",
                              hour_id > 18 & hour_id <= 24 ~ "prime",
                              hour_id == 0 ~ "prime"
                              )) -> usage

    usage %>%
    mutate(wday = case_when(wday == "Mon" ~ "01_Mon",
                           wday == "Tue" ~ "02_Tue",
                           wday == "Wed" ~ "03_Wed",
                           wday == "Thu" ~ "04_Thu",
                           wday == "Fri" ~ "05_Fri",
                           wday == "Sat" ~ "06_Sat",
                           wday == "Sun" ~ "07_Sun")) -> usage

    usage %>%
    mutate(month_id = last_day(ddate)) -> usage

    usage %>%
    group_by(analytic_id, date, wday, period, month_id) %>%
    summarise(sum_data_vol = sum(data_vol_kb), 
             sum_data_min = sum(data_usg_minute), 
             sum_voice_out = sum(voice_out_minute), 
             sum_voice_in = sum(voice_in_minute)) -> usage_agg_daily

    sdf_register(usage_agg_daily, "usage_agg_daily")
    tbl_cache(sc, "usage_agg_daily")

    usage_agg_daily %>%
    ungroup() %>%
    group_by(analytic_id, wday, period, month_id) %>%
    summarise(mean_data_vol = mean(sum_data_vol), 
             mean_data_min = mean(sum_data_min), 
             mean_voice_out = mean(sum_voice_out),
             mean_voice_in = mean(sum_voice_in)) -> usage_agg_wday

    usage_agg_wday %>%
    mutate(wday_period = paste(wday, period, sep = "_")) -> usage_agg_wday

    sdf_register(usage_agg_wday, "usage_agg_wday")
    tbl_cache(sc, "usage_agg_wday")

    # Pivoting
    usage_agg_wday %>%
    mutate(wday = paste0(wday_period, "_mean_data_vol")) %>%
    sdf_pivot(analytic_id + month_id ~ wday, list(mean_data_vol="mean")) %>%
    na.replace(0) -> usage_data_vol_pivot

    usage_agg_wday %>%
    mutate(wday = paste0(wday_period, "_mean_data_min")) %>%
    sdf_pivot(analytic_id + month_id ~ wday, list(mean_data_min="mean")) %>%
    na.replace(0) -> usage_data_min_pivot

    usage_agg_wday %>%
    mutate(wday = paste0(wday_period, "_mean_voice_out")) %>%
    sdf_pivot(analytic_id + month_id ~ wday, list(mean_voice_out="mean")) %>%
    na.replace(0) -> usage_voice_out_pivot

    usage_agg_wday %>%
    mutate(wday = paste0(wday_period, "_mean_voice_in")) %>%
    sdf_pivot(analytic_id + month_id ~ wday, list(mean_voice_in="mean")) %>%
    na.replace(0) -> usage_voice_in_pivot

    usage_data_vol_pivot %>%
    full_join(usage_data_min_pivot, by = c("analytic_id", "month_id")) %>%
    full_join(usage_voice_in_pivot, by = c("analytic_id", "month_id")) %>%
    full_join(usage_voice_out_pivot, by = c("analytic_id", "month_id")) -> usage_pivot_all

    sdf_register(usage_pivot_all, "usage_pivot_all")
    tbl_cache(sc, "usage_pivot_all")

    # Select relevance features ----------------------------
    prof %>% select(analytic_id, national_id, crm_sub_id, activation_date, ddate,
                 age, credit_limit, days_active, service_month, foreigner_flag, gender, 
                 mnp_flag, master_segment_id, crm_most_usage_province, 
                 handset_launchprice, handset_os, 
                 norms_net_revenue_avg_3mth_p0_p2, norms_net_revenue_gprs, norms_net_revenue_vas, norms_net_revenue_voice) -> prof

    pack_info %>% select(analytic_id, ddate, data_3g_usage_mb, data_4g_usage_mb, data_traffic_subs_mb, new_pack_cat, 
                        mou_ic_total, mou_og_intl, mou_og_roaming, mou_og_total, 
                        num_of_days_data_used, sms_og_total, distinct_out_number) -> pack_info

    # Joining table --------------------------------------

    prof %>%
    left_join(pack_info, by = c("analytic_id", "ddate")) %>%
    left_join(house, by = c("analytic_id", "ddate")) %>%
    left_join(app_pivot_all, by = c("analytic_id", "ddate")) %>%
    left_join(usage_pivot_all, by = c("analytic_id", "ddate"="month_id")) -> feature_tbl

    sdf_register(feature_tbl, "feature_tbl")
    tbl_cache(sc, "feature_tbl")

    # Joining with training label
    if (training == T) {
      dt_input %>%
      filter(crm_sub_id != "NA") %>%
      left_join(select(feature_tbl, -analytic_id), by = c("crm_sub_id", "prof_month"="ddate")) -> label_tbl

      sdf_register(label_tbl, "label_tbl")
      tbl_cache(sc, "label_tbl")

      return(label_tbl)

    } else {
      return(feature_tbl)
  }
}

# COMMAND ----------

# DBTITLE 1,Feature enhancing function
feat_enhance <- function(data_input, 
                    his_time = 6, training = T) {
  
  # Table sources
  prof_table <- "prod_delta.dm07_sub_clnt_info"
  hh_aggregation_table <- "mck_fmc.hh_aggregation"
  pack_info_table <- "prod_delta.dm26_postpaid_pack_info"
  
  # Target month
  oldest_month <- ceiling_date(today() %m-% months(his_time), unit="month") %m-% days(1)
  
  if (training == T) {
    newest_month <- ceiling_date(today() %m-% months(his_time - 2), unit="month") %m-% days(1)
  } else {
    newest_month <- ceiling_date(today() %m-% months(his_time - 1), unit="month") %m-% days(1)
  }

  print(paste("Oldest history month is :", oldest_month))
  print(paste("Newest history month is :", newest_month))
  
  # Profile mapping
  if (training == T) {
    data_input %>%
    mutate(p1_map = add_months(prof_month, -1), 
    p2_map = add_months(prof_month, -2)) -> dt
  } else {
    data_input %>%
    mutate(p1_map = add_months(ddate, -1), 
    p2_map = add_months(ddate, -2)) -> dt
  }
  
  # Loading history data
  prof <- tbl(sc, prof_table)
  hh <- tbl(sc, hh_aggregation_table)
  pack_info <- tbl(sc, pack_info_table)
  
  prof %>% select(analytic_id, norms_net_revenue_avg_3mth_p0_p2, norms_net_revenue_gprs, 
               norms_net_revenue_vas, norms_net_revenue_voice, ddate) -> prof
 
  hh %>% select(-register_date, -home_analytic_id, -geo_home_shape_id, ddate) -> hh
 
  pack_info %>% select(analytic_id, data_traffic_subs_mb, mou_ic_total, mou_og_total, 
                       distinct_out_number, ddate) -> pack_info
  
  # Joining with main table and P1 data
  dt %>%
  left_join(prof, by =c("analytic_id", "p1_map"="ddate"), suffix = c("","_p1")) %>% 
  left_join(hh, by = c("analytic_id", "p1_map"="ddate"), suffix = c("","_p1")) %>%
  left_join(pack_info, by = c("analytic_id", "p1_map"="ddate"), suffix = c("","_p1")) -> dt_p1
  
  sdf_register(dt_p1, "dt_p1")
  tbl_cache(sc, "dt_p1")
  
  # Joining with P2 data
  dt_p1 %>%
  left_join(prof, by =c("analytic_id", "p2_map"="ddate"), suffix = c("","_p2")) %>% 
  left_join(hh, by = c("analytic_id", "p2_map"="ddate"), suffix = c("","_p2")) %>%
  left_join(pack_info, by = c("analytic_id", "p2_map"="ddate"), suffix = c("","_p2")) -> dt_p2
  
  sdf_register(dt_p2, "dt_p2")
  tbl_cache(sc, "dt_p2")
  
  return(dt_p2)
}

# COMMAND ----------

# DBTITLE 1,Data cleansing function
dt_cleansing <- function (dt_input, training = T) {
  
  # Export directory
  top_province_export <- "/dbfs/mnt/cvm02/user/pitchaym/share/aa_top_province.csv"
  
  # initial criteria
  dt_input %>%
  mutate(age = as.numeric(age)) %>%
  filter(!is.na(analytic_id)) -> dt_trans
  
  # Mean imputing
  im_mean <- c("age", "handset_launchprice", "credit_limit")
  
  if (training == T) {
    dt_trans %>%
    ft_imputer(input_cols=im_mean, output_cols=im_mean, strategy="mean") %>%
    select(analytic_id, crm_sub_id, camp_response, national_id, register_date, 
           age, handset_launchprice, credit_limit, 
      mnp_flag, master_segment_id, crm_most_usage_province) -> mean_im
  } else {
    dt_trans %>%
    ft_imputer(input_cols=im_mean, output_cols=im_mean, strategy="mean") %>%
    select(analytic_id, crm_sub_id, national_id, register_date, 
           age, handset_launchprice, credit_limit, 
      mnp_flag, master_segment_id, crm_most_usage_province) -> mean_im
  }
  
  # Unknown imputing
  dt_trans %>%
  select(analytic_id, gender, handset_os, new_pack_cat) %>%
  mutate(gender = case_when(
    gender %in% c("Male", "Female") ~ gender,
    TRUE ~ "unknown"
  )) %>%
  na.replace("unknown") -> unknown_im
 
  # Zero imputing
  dt_trans %>%
  select(analytic_id, days_active, service_month, norms_net_revenue_avg_3mth_p0_p2:norms_net_revenue_voice, 
        data_3g_usage_mb:data_traffic_subs_mb, mou_ic_total:distinct_out_number,
        total_household_arpu:`07_Sun_prime_mean_voice_out`, 
        norms_net_revenue_avg_3mth_p0_p2_p1:distinct_out_number_p2
        ) %>%
  na.replace(0) -> zero_im
  
  # Joining back
  mean_im %>%
  left_join(unknown_im, by = "analytic_id") %>%
  left_join(zero_im, by = "analytic_id") -> base_clean
  
  # Regroup ----------------------------
  
  # Serenade class
  base_clean %>%
  mutate(serenade = case_when(
    master_segment_id %in% c("NA", "Prospect Emerald") | is.na(master_segment_id) ~ "Classic",
    master_segment_id %in% c("Platinum Plus", "Prospect Plat Plus") ~ "Platinum",
    master_segment_id == "Prospect Platinum" ~ "Gold",
    master_segment_id == "Prospect Gold" ~ "Emerald",
    master_segment_id == "Standard" ~ "Classic",
    TRUE ~ master_segment_id)) %>%
  select(-master_segment_id) -> base_clean
  
  # Handset os
  base_clean %>%
  mutate(handset_os = case_when(
  handset_os %in% c("Android", "iOS", "Proprietary", "Unknown") ~ handset_os, 
  TRUE ~ "others")) -> base_clean
  
  # MNP flag
  base_clean %>%
  mutate(mnp_flag = ifelse(is.na(mnp_flag),"N" , mnp_flag)) -> base_clean
  
  # Checking for training and scoring process
  if (training == T) {
      base_clean %>%
      filter(!is.na(crm_most_usage_province)) %>%
      count(crm_most_usage_province) %>%
      arrange(desc(n)) %>%
      filter(min_rank(crm_most_usage_province) <= 30) %>%
      collect() -> most_province
    
      write_csv(most_province, top_province_export)
    
  } else {
    most_province <- read_csv(top_province_export)
  }
  
  # Regroup province
  most_province <- most_province$crm_most_usage_province
  
  base_clean %>%
  mutate(top30_province = case_when(
  is.na(crm_most_usage_province) ~ "Others", 
  crm_most_usage_province %in% most_province ~ crm_most_usage_province,
  TRUE ~ "Others")) %>%
  select(-crm_most_usage_province) -> base_clean
  
  sdf_register(base_clean, "base_clean")
  tbl_cache(sc, "base_clean")
  
  return(base_clean)
}

# COMMAND ----------

# DBTITLE 1,Feature deriving
feat_derive <- function (dt_input) {
  
  # Average
  dt_input %>%
  mutate(avg_gprs = (norms_net_revenue_gprs + norms_net_revenue_gprs_p1 + norms_net_revenue_gprs_p2)/3, 
      avg_vas = (norms_net_revenue_vas + norms_net_revenue_vas_p1 + norms_net_revenue_vas_p2)/3, 
      avg_voice = (norms_net_revenue_voice + norms_net_revenue_voice_p1 + norms_net_revenue_voice_p2)/3, 
      avg_total_hh_arpu = (total_household_arpu + total_household_arpu_p1 + total_household_arpu_p2)/3, 
      avg_average_hh_arpu = (average_household_arpu + average_household_arpu_p1 + average_household_arpu_p2)/3, 
      avg_max_hh_arpu = (max_household_arpu + max_household_arpu_p1 + max_household_arpu_p2)/3, 
      avg_min_hh_arpu = (min_household_arpu + min_household_arpu_p1 + min_household_arpu_p2)/3,
      avg_std_hh_arpu = (std_household_arpu + std_household_arpu_p1 + std_household_arpu_p2)/3,
      avg_average_hh_voice_minute = (average_household_voice_minute + average_household_voice_minute_p1 + average_household_voice_minute_p2)/3,
      avg_max_hh_voice_minute = (max_household_voice_minute + max_household_voice_minute_p1 + max_household_voice_minute_p2)/3, 
      avg_min_hh_voice_minute = (min_household_voice_minute + min_household_voice_minute_p1 + min_household_voice_minute_p2)/3,
      avg_std_hh_voice_minute = (std_household_voice_minute + std_household_voice_minute_p1 + std_household_voice_minute_p2)/3,
      avg_average_hh_data_usage = (average_household_data_usage + average_household_data_usage_p1 + average_household_data_usage_p2)/3,
      avg_max_hh_data_usage = (max_household_data_usage + max_household_data_usage_p1 + max_household_data_usage_p2)/3, 
      avg_min_hh_data_usage = (min_household_data_usage + min_household_data_usage_p1 + min_household_data_usage_p2)/3,
      avg_std_hh_data_usage = (std_household_data_usage + std_household_data_usage_p1 + std_household_data_usage_p2)/3,
      avg_average_hh_age = (average_household_age + average_household_age_p1 + average_household_age_p2)/3,
      avg_max_hh_age = (max_household_age + max_household_age_p1 + max_household_age_p2)/3, 
      avg_min_hh_age = (min_household_age + min_household_age_p1 + min_household_age_p2)/3,
      avg_std_hh_age = (std_household_age + std_household_age_p1 + std_household_age_p2)/3,
      avg_average_hh_service_month = (average_household_service_month + average_household_service_month_p1 + average_household_service_month_p2)/3,
      avg_max_hh_service_month = (max_household_service_month + max_household_service_month_p1 + max_household_service_month_p2)/3, 
      avg_min_hh_service_month = (min_household_service_month + min_household_service_month_p1 + min_household_service_month_p2)/3,
      avg_std_hh_service_month = (std_household_service_month + std_household_service_month_p1 + std_household_service_month_p2)/3, 
      avg_hh_person_count = (household_person_count + household_person_count_p1 + household_person_count_p2)/3, 
      avg_data_traffic_subs_mb = (data_traffic_subs_mb + data_traffic_subs_mb_p1 + data_traffic_subs_mb_p2)/3,
      avg_mou_ic_total = (mou_ic_total + mou_ic_total_p1 + mou_ic_total_p2)/3, 
      avg_mou_og_total = (mou_og_total + mou_og_total_p1 + mou_og_total_p2)/3,
      avg_distinct_out_number = (distinct_out_number + distinct_out_number_p1 + distinct_out_number_p2)/3
      ) -> base_feature
  
    # Slope
    base_feature %>%
    mutate(
    slope_net_rev = norms_net_revenue_avg_3mth_p0_p2/((norms_net_revenue_avg_3mth_p0_p2_p1 + norms_net_revenue_avg_3mth_p0_p2_p2)/2) - 1, 
    slope_gprs = norms_net_revenue_gprs/((norms_net_revenue_gprs_p1 + norms_net_revenue_gprs_p2)/2) - 1, 
    slope_vas = norms_net_revenue_vas/((norms_net_revenue_vas_p1 + norms_net_revenue_vas_p2)/2) - 1, 
    slope_voice = norms_net_revenue_voice/((norms_net_revenue_voice_p1 + norms_net_revenue_voice_p2)/2) - 1,
    slope_total_household_arpu = total_household_arpu/((total_household_arpu_p1 + total_household_arpu_p2)/2) - 1,
    slope_average_household_arpu = average_household_arpu/((average_household_arpu_p1 + average_household_arpu_p2)/2) - 1,
    slope_max_household_arpu = max_household_arpu/((max_household_arpu_p1 + max_household_arpu_p2)/2) - 1,
    slope_min_household_arpu = min_household_arpu/((min_household_arpu_p1 + min_household_arpu_p2)/2) - 1,
    slope_std_household_arpu = std_household_arpu/((std_household_arpu_p1 + std_household_arpu_p2)/2) - 1,
    slope_total_household_voice_minute = total_household_voice_minute/((total_household_voice_minute_p1 + total_household_voice_minute_p2)/2) - 1,
    slope_average_household_voice_minute = average_household_voice_minute/((average_household_voice_minute_p1 + average_household_voice_minute_p2)/2) - 1,
    slope_max_household_voice_minute = max_household_voice_minute/((max_household_voice_minute_p1 + max_household_voice_minute_p2)/2) - 1,
    slope_min_household_voice_minute = min_household_voice_minute/((min_household_voice_minute_p1 + min_household_voice_minute_p2)/2) - 1,
    slope_std_household_voice_minute = std_household_voice_minute/((std_household_voice_minute_p1 + std_household_voice_minute_p2)/2) - 1,
    slope_total_household_data_usage = total_household_data_usage/((total_household_data_usage_p1 + total_household_data_usage_p2)/2) - 1,
    slope_average_household_data_usage = average_household_data_usage/((average_household_data_usage_p1 + average_household_data_usage_p2)/2) - 1,
    slope_max_household_data_usage = max_household_data_usage/((max_household_data_usage_p1 + max_household_data_usage_p2)/2) - 1,
    slope_min_household_data_usage = min_household_data_usage/((min_household_data_usage_p1 + min_household_data_usage_p2)/2) - 1,
    slope_std_household_data_usage = std_household_data_usage/((std_household_data_usage_p1 + std_household_data_usage_p2)/2) - 1,
    slope_average_household_age = average_household_age/((average_household_age_p1 + average_household_age_p2)/2) - 1,
    slope_max_household_age = max_household_age/((max_household_age_p1 + max_household_age_p2)/2) - 1,
    slope_min_household_age = min_household_age/((min_household_age_p1 + min_household_age_p2)/2) - 1,
    slope_std_household_age = std_household_age/((std_household_age_p1 + std_household_age_p2)/2) - 1,
    slope_average_household_service_month = average_household_service_month/((average_household_service_month_p1 + average_household_service_month_p2)/2) - 1,
    slope_max_household_service_month = max_household_service_month/((max_household_service_month_p1 + max_household_service_month_p2)/2) - 1,
    slope_min_household_service_month = min_household_service_month/((min_household_service_month_p1 + min_household_service_month_p2)/2) - 1,
    slope_std_household_service_month = std_household_service_month/((std_household_service_month_p1 + std_household_service_month_p2)/2) - 1,
    slope_household_person_count = household_person_count/((household_person_count_p1 + household_person_count_p2)/2) - 1,
    slope_data_traffic_subs_mb = data_traffic_subs_mb/((data_traffic_subs_mb_p1 + data_traffic_subs_mb_p2)/2) - 1,
    slope_mou_ic_total = mou_ic_total/((mou_ic_total_p1 + mou_ic_total_p2)/2) - 1,
    slope_mou_og_total = mou_og_total/((mou_og_total_p1 + mou_og_total_p2)/2) - 1,
    slope_distinct_out_number = distinct_out_number/((distinct_out_number_p1 + distinct_out_number_p2)/2) - 1,
    ) -> base_feature

    # Imputing
    base_feature %>%
    na.replace(0) %>%
    select(-norms_net_revenue_avg_3mth_p0_p2_p1:-distinct_out_number_p2) -> base_feature

    sdf_register(base_feature, "base_feature")
    tbl_cache(sc, "base_feature")
  
    return(base_feature)
  
} 