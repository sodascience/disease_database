# Ensure necessary directories exist
if (!dir.exists("maps")) {
  dir.create("maps")
}
if (!dir.exists("raw_data")) {
  dir.create("raw_data")
}

# Load necessary libraries
install.packages(c("sf", "tidyverse", "jsonlite", "writexl"))

library(sf)
library(tidyverse)
library(jsonlite)
library(writexl)

# Read map data
map <- st_read("https://nlgis.nl/api/maps?year=1869", crs = "EPSG:4326")

# Load cholera mentions data
cholera_mentions <- read_csv("C:\\github\\disease_database\\initialresults\\full country searches\\cholera_mentions_august_1866.csv")

# Convert amsterdamcode to character type in both datasets to ensure proper joining
map$amsterdamcode <- as.character(map$amsterdamcode)
cholera_mentions$amsterdamcode <- as.character(cholera_mentions$amsterdamcode)

# Join map and cholera mentions data based on amsterdamcode
map_cholera <- left_join(map, cholera_mentions, by = "amsterdamcode")

# Check if the 'name' column exists after the join
if (!"name" %in% colnames(map_cholera)) {
  if ("name.x" %in% colnames(map_cholera)) {
    map_cholera <- map_cholera %>% rename(name = name.x)
  } else if ("name.y" %in% colnames(map_cholera)) {
    map_cholera <- map_cholera %>% rename(name = name.y)
  } else {
    stop("The 'name' column could not be found after the join.")
  }
}

# Exclude cases where name matches Sluis, Broek, Heer, Haren, Vries, Bergen, Waarde
map_cholera <- map_cholera %>%
  mutate(n_both = ifelse(grepl("Sluis|Broek|Heer|Haren|Vries|Bergen|Waarde", name), NA, n_both))

# Define predefined color scale limits
min_n_both <- 0
max_n_both <- 400  # Adjusted maximum limit to accommodate values above 200

# Plot the map with cholera mentions
ggplot(map_cholera) + 
  geom_sf(aes(fill = n_both), colour = "white", linewidth = 0.05) + 
  theme_minimal() +
  labs(
    title = "Cholera Mentions per Municipality in August 1866",
    fill = "Cholera Mentions" # Title of the legend
  ) +
  scale_fill_gradientn(
    colors = c("lightgrey", "yellow", "orange", "red", "purple", "navy"), 
    values = scales::rescale(c(0, 5, 15, 25, 100, 200, 250, 300, 350, 400)), # Adjusted to include higher values
    limits = c(min_n_both, max_n_both), # Set fixed limits for consistent scaling
    na.value = "lightgrey" # Plot no data (NA values) as light grey
  ) +
  guides(fill = guide_colorbar(title = "Cholera Mentions")) # Ensure legend title is clear

# Save the plot
ggsave("maps/municipalities_cholera_august1866.png", bg = "white", width = 7, height = 8)
