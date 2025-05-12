library(tidyverse)
library(nanoparquet)
library(sf)

dis_df <- read_parquet("disease_database/disease_database_v1.2.parquet")
nl1869 <- read_sf("disease_database/nlgis_1869.geojson")

dis_df |>
  filter(year <= 1868, year >= 1864, disease == "cholera") |> 
  summarize(mention_rate = mean(mention_rate, na.rm = TRUE), .by = c(cbscode, year)) |> 
  left_join(nl1869, by = join_by(cbscode)) |> 
  st_as_sf() |> 
  st_transform("EPSG:28992") |> 
  ggplot(aes(fill = mention_rate)) +
  geom_sf(color = "grey", linewidth = 0.1) +
  facet_grid(cols = vars(year)) +
  scale_fill_gradient(
    na.value = "#ffffcc",
    low = "#f7fbff",
    high = "#08306b",
    limits = c(0, .6),
    transform = scales::transform_pseudo_log(sigma = .06),
    guide = "none"
  ) +
  theme_void()

ggsave("img/cholera_1864_1868.png", width = 14, height = 3.5, dpi = 500, bg = "white")
