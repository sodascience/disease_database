# Compute smoothing model
library(tidyverse)
library(nanoparquet)
library(lme4)

DB_VERSION <- "v1.2"

# get disease database data
db_path <- file.path(
  "processed_data", "database",
  paste0("disease_database_", DB_VERSION, ".parquet")
)
out_path <- file.path(
  "processed_data", "database",
  paste0("disease_database_smooth", DB_VERSION, ".parquet")
)

smooth_dir <- file.path("processed_data", "smooth_fits")
if (!dir.exists(smooth_dir)) dir.create(smooth_dir)


disease_db <- read_parquet(db_path)
disease_db$mention_rate_smooth <- 0.0


for (dis in unique(disease_db$disease)) {
  cat(format(Sys.time()), "| Fitting model for disease:", dis, "\n")
  df_dis <-
    disease_db |>
    filter(disease == !!dis) |>
    select(year, month, cbscode, n_location, n_both)

  # fit the model (takes 20 minutes or so):
  fit <- glmer(
    formula = n_both / n_location ~ (1 | year:month / cbscode),
    family  = binomial(),
    weights = n_location,
    data    = df_dis,
    verbose = 2
  )

  cat(format(Sys.time()), "| Writing model for disease:", dis, "\n")
  fit_path <- file.path(
    smooth_dir,
    paste0("fit_", dis, "_", DB_VERSION, ".rds")
  )
  write_rds(fit, fit_path)

  cat(format(Sys.time()), "| Generating predictions for disease:", dis, "\n")
  pred <- predict(fit, df_dis, allow.new.levels = TRUE, type = "response")
  disease_db[disease_db$disease == dis, "mention_rate_smooth"] <- pred
}

# save the file
disease_db |>
  write_parquet(
    file = out_path,
    schema = read_parquet_schema(db_path)
  )
