# NUS_DATATHON_SINGLIFE_-232-.ipynb
library("arrow")
library("dplyr")
library("tidyr")
df <- read_parquet("../data/catB_train.parquet")
head(df)

### PART 1: CLEANING DATA

# the TARGET COL
df_target <- df %>% select(c(clntnum, f_purchase_lh))

# DEALING WITH NA
threshhold = 0.5
cols_to_be_droppped = c()

for (i in c(2:305)) {
  if (sum(is.na(df[,i]))/17992 > threshhold) {
    cols_to_be_droppped <- c(cols_to_be_droppped, i)
  }
}

df1 <- df %>% select(-cols_to_be_droppped)

### General Client Information
#We are skipping this, too many factors already

#### Client Risk and Status Indicators

to_select_risk <- c('flg_substandard','flg_is_borderline_standard','flg_is_revised_term','flg_is_rental_flat','flg_has_health_claim','flg_has_life_claim','flg_gi_claim','flg_is_proposal','flg_with_preauthorisation','flg_is_returned_mail')
df_risk <- df1 %>% select(clntnum, any_of(to_select_risk))

df_risk_col_mean <- df_risk %>% summarise(across(any_of(to_select_risk), mean, na.rm = TRUE))

# from df_risk_col_mean we can tell that the percentage of flagged cases (indicated by 1) is very low, almost always < 4% of the total cases.
# Therefore we deem the factors under this category as not valuable to the target column, and thus will. not include this in our model


### Client Consent and Communication Preferences: consent to be contactable: whether they consent to at least one among the means of communications

df_consent <- df1 %>% select(c(1,contains("consent"), contains("valid"))) 

can_via_phone <- ifelse(df_consent$is_consent_to_call == 1 |  df_consent$is_consent_to_sms == 1, 1, 0)
can_via_mail <- ifelse(df_consent$is_consent_to_mail == 1 & df_consent$is_valid_dm == 1, 1,0)
can_via_email <- ifelse(df_consent$is_consent_to_email == 1 & df_consent$is_valid_email == 1, 1,0)

df_consent$can_via_phone <- can_via_phone
df_consent$can_via_mail <- can_via_mail
df_consent$can_via_email <- can_via_email

df_consent$can_contact <- df_consent$can_via_phone + df_consent$can_via_mail + df_consent$can_via_email
df_consent$flg_consent <- ifelse(df_consent$can_contact > 0, 1,0)

df_consent_final <- df_consent %>% select(c(1,12))

### Demographic and Household Information
library('lubridate')

df_age_group <- df %>%
  mutate(cltdob_fix = ifelse(cltdob_fix == "None", NA, cltdob_fix))
df_age_group$year_column <- year(as.Date(df$cltdob_fix))
df_age_group$age <- 2024 - df_age_group$year_column
df_age_group$annual_income_est <- as.factor(as.character(df_age_group$annual_income_est))
df_age_group <- df_age_group %>% select(clntnum, age, annual_income_est)

### Policy and Claim History

to_select_policy <- c('n_months_last_bought_products', 'flg_latest_being_lapse', 'flg_latest_being_cancel', 'recency_lapse', 'recency_cancel', 'tot_inforce_pols', 'tot_cancel_pols', 'f_ever_declined_la')
df_policy <- df1 %>% select(clntnum, any_of(to_select_policy))

df_policy_col_mean <- df_policy %>% summarise(across(any_of(to_select_policy), mean, na.rm = TRUE))
# same as above, we exclude flg_latest_being_cancel out of our consideration as the rate is less than 1%
df_policy_final <- df_policy %>% select(-flg_latest_being_cancel)

### Anonymized Insurance Product Metrics (APE, Sum Insured, Prepaid Premiums)
contains_ape <- c("ape_") 
df_ape <- df1 %>%
  mutate(total_ape = rowSums(select(., starts_with(contains_ape)))) %>%
  select(c(clntnum, total_ape))

contains_sumins <- c("sumins_") 
df_sumins <- df1 %>%
  mutate(total_sumins = rowSums(select(., starts_with(contains_sumins)))) %>%
  select(c(clntnum, total_sumins))

contains_prempaid = c("prempaid_")
df_prepaid <- df1 %>%
  mutate(total_prempaid = rowSums(select(., starts_with(contains_prempaid)))) %>%
  select(c(clntnum, total_prempaid))

### Other Flags and Metrics
to_select_flags <- c('f_elx', 'f_mindef_mha', 'f_retail', 'flg_affconnect_*', 'affcon_visit_days', 'n_months_since_visit_affcon', 'clmcon_visit_days', 'recency_clmcon', 'recency_clmcon_regis', 'hlthclaim_amt', 'giclaim_amt', 'recency_hlthclaim', 'recency_giclaim', 'hlthclaim_cnt_success', 'giclaim_cnt_success', 'flg_hlthclaim_', 'flg_gi_claim_')
df_sector <- df1 %>% select(clntnum, any_of(to_select_flags))

### Purchase and Lapse Metrics for Specific Products

# regarding whether the customer has ever bought insurance:
df$sum_ever_bought <- rowSums(df[, grepl("^f_ever_bought", names(df))]) 
df_sum_ever_bought <- df %>% select(clntnum, sum_ever_bought)

### PART 2: DATA EXPLORATION - VISUALISATION

### GRAPH 1: Age group, annual income versus purchase history

# Remove "None" in Age column
df <- df %>%
  mutate(cltdob_fix = ifelse(cltdob_fix == "None", NA, cltdob_fix)) 

# Year of birth
df$year_column <- year(as.Date(df$cltdob_fix)) 
# Age in 2024
df$age_in_2024 <- 2024 - df$year_column

# Define the age group breaks
age_breaks <- c(0, 20, 30, 40, 50, 60, 70, Inf)
# Labels for the age groups
age_labels <- c("0-20", "21-30", "31-40","41-50", "51-60", "61-70", "71+")

df$age_group <- cut(df$age_in_2024, breaks = age_breaks, labels = age_labels, include.lowest = TRUE, na.omit = TRUE)

# Remove NAs
df <- df %>%
  filter(!is.na(age_group) & !is.na(annual_income_est))

df_list <- split(df, df$annual_income_est)

A_df <- df_list$A.ABOVE200K
B_df <- df_list$`B.100K-200K`
C_df <- df_list$`C.60K-100K`
D_df <- df_list$`D.30K-60K`
E_df <- df_list$E.BELOW30K

# Above 200K Annual Income
A_df_total <- A_df %>%
  group_by(age_group) %>%
  mutate(mean_purchase = mean(sum_ever_bought, na.rm = TRUE)) %>%
  ungroup()

ggplot(A_df_total, aes(x = age, y = sum_ever_bought, color = age_group)) + 
  geom_jitter() + 
  geom_smooth(aes(y = mean_purchase), method = loess, color = "black", se = FALSE) +
  geom_text(aes(x = 90, y = mean_purchase[1], label = "Mean"), colour = "black") +
  scale_x_continuous(breaks = seq(0, 70, by = 10)) +  # Set x-axis breaks
  labs(title = "Above 200K Annual Income", x = "Age", y = "Products Bought")

# 100K-200K
B_df_total <- B_df %>%
  group_by(age_group) %>%
  mutate(mean_purchase = mean(sum_ever_bought, na.rm = TRUE)) %>%
  ungroup()

ggplot(B_df_total, aes(x = age, y = sum_ever_bought, color = age_group)) + 
  geom_jitter() + 
  geom_smooth(aes(y = mean_purchase), method = loess, color = "black", se = FALSE) +
  geom_text(aes(x = 90, y = mean_purchase[1], label = "Mean"), colour = "black") +
  scale_x_continuous(breaks = seq(0, 70, by = 10)) +  # Set x-axis breaks
  labs(title = "100K-200K Annual Income", x = "Age", y = "Products Bought")

# 60K-100K
C_df_total <- C_df %>%
  group_by(age_group) %>%
  mutate(mean_purchase = mean(sum_ever_bought, na.rm = TRUE)) %>%
  ungroup()

ggplot(C_df_total, aes(x = age, y = sum_ever_bought, color = age_group)) + 
  geom_jitter() + 
  geom_smooth(aes(y = mean_purchase), method = loess, color = "black", se = FALSE) +
  geom_text(aes(x = 90, y = mean_purchase[1], label = "Mean"), colour = "black") +
  scale_x_continuous(breaks = seq(0, 70, by = 10)) +  # Set x-axis breaks
  labs(title = "60K-100K Annual Income", x = "Age", y = "Products Bought")

# 30K-60K
D_df_total <- D_df %>%
  group_by(age_group) %>%
  mutate(mean_purchase = mean(sum_ever_bought, na.rm = TRUE)) %>%
  ungroup()

ggplot(D_df_total, aes(x = age, y = sum_ever_bought, color = age_group)) + 
  geom_jitter() + 
  geom_smooth(aes(y = mean_purchase), method = loess, color = "black", se = FALSE) +
  geom_text(aes(x = 90, y = mean_purchase[1], label = "Mean"), colour = "black") +
  scale_x_continuous(breaks = seq(0, 70, by = 10)) +  # Set x-axis breaks
  labs(title = "30K-60K Annual Income", x = "Age", y = "Products Bought")

# Below 30K
E_df_total <- E_df %>%
  group_by(age_group) %>%
  mutate(mean_purchase = mean(sum_ever_bought, na.rm = TRUE)) %>%
  ungroup()

ggplot(E_df_total, aes(x = age, y = sum_ever_bought, color = age_group)) + 
  geom_jitter() + 
  geom_smooth(aes(y = mean_purchase), method = loess, color = "black", se = FALSE) +
  geom_text(aes(x = 90, y = mean_purchase[1], label = "Mean"), colour = "black") +
  scale_x_continuous(breaks = seq(0, 70, by = 10)) +  # Set x-axis breaks
  labs(title = "Below 30K Annual Income", x = "Age", y = "Products Bought")

## Each graph is grouped by their income group, using the points we can assume how many products they will buy (maybe 
## means of satisfaction) using KNN? idk -ALIVS:))

# GRAPH 2: Total APE versus Annual income

library(ggplot2)

# Replace "column1" and "column2" with actual column names, and adjust conditions as needed
condition <- (df$f_ever_declined_la == "1") 

# Use filter to filter rows based on the conditions
df_filtered <- df %>% filter(condition)

# create condition for columns selection
contains_ape <- c("ape_") 

# Sum columns with the specified starting letters
data_result <- df_filtered %>%
  mutate(total_ape = rowSums(select(., starts_with(contains_ape))))

# boxplot
boxplot(total_ape ~ annual_income_est, data = df, col = "lightblue", main = "Boxplot of Price by Category", outline = FALSE)


### PART 3: MERGING DATAFRAMES TOGETHER, CREATING MODEL

df_mastermind <- df_target %>% inner_join(df_consent_final) %>%
  inner_join(df_age_group) %>%
  inner_join(df_policy_final) %>%
  inner_join(df_ape) %>%
  inner_join(df_sumins) %>%
  inner_join(df_prepaid) %>%
  inner_join(df_sector) %>%
  inner_join(df_sum_ever_bought)


install.packages("rpart")
library(rpart)


columns_to_convert <- c("f_purchase_lh", "flg_consent", 'flg_latest_being_lapse', 'f_elx', 'f_mindef_mha', 'f_retail')
df_mastermind_factor <- df_mastermind %>% mutate(across(all_of(columns_to_convert), factor))

df_mastermind_replace_na <- df_mastermind_factor %>%
  mutate(f_purchase_lh = forcats::fct_explicit_na(f_purchase_lh, "0"))

for (i in colnames(df_mastermind_replace_na)) {
  print(summary(df_mastermind_replace_na[, i]))
}

### DECISION TREE
# Create a decision tree model
tree_model <- rpart(f_purchase_lh ~ flg_consent + age + annual_income_est + n_months_last_bought_products + flg_latest_being_lapse +
                      tot_inforce_pols + total_ape + total_sumins + total_prempaid + f_elx + f_mindef_mha + f_retail + sum_ever_bought,
                    data = df_mastermind_replace_na, method = "class")

print(tree_model)
plot(tree_model)
text(tree_model)

# Error in plot.rpart(tree_model) : fit is not a tree, just a root
# meaning data is imbalance for tree to grow



### KNN: only works with int, cannot work with factor
library(class)
set.seed(5)

df_mastermind_replace_na <- df_mastermind %>%
  mutate(f_purchase_lh = forcats::fct_explicit_na(f_purchase_lh, "0")) %>%
  na.omit()


# Split the data into training and testing sets
indices <- sample(1:nrow(df_mastermind_replace_na), 0.7 * nrow(df_mastermind_replace_na))
train_data <- df_mastermind_replace_na[indices, ]
test_data <- df_mastermind_replace_na[-indices, ]

# Standardize the features
train_features <- scale(train_data[, c("age", "n_months_last_bought_products", "tot_inforce_pols", "total_ape", "total_sumins", "total_prempaid", "sum_ever_bought")])
test_features <- scale(test_data[, c("age", "n_months_last_bought_products", "tot_inforce_pols", "total_ape", "total_sumins", "total_prempaid", "sum_ever_bought")])

# Apply kNN (can experiment with more values)
k <- 5
predictions <- knn(train = train_features, test = test_features, cl = train_data$f_purchase_lh, k = k)

# Evaluate the performance
confusion_matrix <- table(predictions, test_data$f_purchase_lh)
print(confusion_matrix)
