library(sf)
library(tidyverse)
library(jsonlite)
library(writexl)
library(arrow)

map <- st_read("https://nlgis.nl/api/maps?year=1869", crs = "EPSG:4326")
mentions_1864 <- read_parquet("maps/cholera_1864.parquet") |> filter(n_location < 2500) |> select(cbscode, n_both, n_location)
mentions_1865 <- read_parquet("maps/cholera_1865.parquet") |> filter(n_location < 2500) |> select(cbscode, n_both, n_location)
mentions_1866 <- read_parquet("maps/cholera_1866.parquet") |> filter(n_location < 2500) |> select(cbscode, n_both, n_location)
mentions_1867 <- read_parquet("maps/cholera_1867.parquet") |> filter(n_location < 2500) |> select(cbscode, n_both, n_location)
mentions_1868 <- read_parquet("maps/cholera_1868.parquet") |> filter(n_location < 2500) |> select(cbscode, n_both, n_location)

map_joined_1864 <- left_join(map, mentions_1864, by = "cbscode") |> mutate(year = 1864)
map_joined_1865 <- left_join(map, mentions_1865, by = "cbscode") |> mutate(year = 1865)
map_joined_1866 <- left_join(map, mentions_1866, by = "cbscode") |> mutate(year = 1866)
map_joined_1867 <- left_join(map, mentions_1867, by = "cbscode") |> mutate(year = 1867)
map_joined_1868 <- left_join(map, mentions_1868, by = "cbscode") |> mutate(year = 1868)

bind_rows(map_joined_1864, map_joined_1865, map_joined_1866, map_joined_1867, map_joined_1868) |> 
  mutate(
    lower = qbeta(0.025, n_both + 0.5, n_location - n_both + 0.5),
    upper = qbeta(0.975, n_both + 0.5, n_location - n_both + 0.5),
    width = upper - lower
  ) |>
  mutate(
    across(c(n_both, n_location, lower, upper, width), \(x) ifelse(is.na(x), mean(x, na.rm = TRUE), x)), .by = year
  ) |>
  ggplot(aes(fill = n_both/n_location, alpha = width)) + 
  scale_fill_continuous(na.value = "transparent") + 
  scale_alpha_continuous(trans = "log1p",  range = c(1, 0.1)) +
  geom_sf(color = "transparent", size=0.3) + 
  theme_minimal() + 
  labs(
    fill = "Mention rate", 
    alpha = "Uncertainty",
  ) +
  facet_grid(cols = vars(year))

ggsave("maps/cholera_1865_1868.png", width = 35, height = 10, bg = "white")

# working-age population
pop_m <- read_json("https://nlgis.nl/api/data?code=TOX1&year=1930")
pop_f <- read_json("https://nlgis.nl/api/data?code=TOX2&year=1930")

pop_df_m <- tibble(amsterdamcode = as.integer(names(pop_m)), pop_m = sapply(pop_m, \(x) x$value))
pop_df_f <- tibble(amsterdamcode = as.integer(names(pop_f)), pop_f = sapply(pop_f, \(x) x$value))

pop_df <- left_join(pop_df_m, pop_df_f) |> mutate(pop = pop_m + pop_f)

map_bev <- left_join(map, pop_df)

ggplot(map_bev) + 
  geom_sf(aes(fill = pop), colour = "white", linewidth = 0.05) + 
  theme_minimal() +
  theme(legend.position = "none") +
  labs(
    title = "Municipalities in the Netherlands in 1939"
  ) +
  scale_fill_viridis_c(trans = "log", na.value = "grey")


ggsave("maps/municipalities_1939.png", bg = "white", width = 7, height = 8)


names_df <- 
  map_bev |> 
  as_tibble() |> 
  select(name, cbscode, amsterdamcode, pop) |> 
  arrange(desc(pop)) |> 
  mutate(manual_query = character(1))

write_xlsx(names_df, "raw_data/query_names.xlsx", format_headers = TRUE)
