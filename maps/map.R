library(sf)
library(tidyverse)
library(jsonlite)
library(writexl)

map <- st_read("https://nlgis.nl/api/maps?year=1939", crs = "EPSG:4326")

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
