library(httr)
library(jsonlite)
library(sf)
library(stringr)
library(dplyr)
library(ggplot2)
library(classInt)
library(gganimate)
library(readr)
library(gifski)
# install.packages("trasnformr")

constants = tribble(
  ~query, ~file_loc, ~proj4, ~id,
  "admin_level:%221%22%20AND%20country_iso3:USA", "https://raw.githubusercontent.com/SuLab/outbreak.info/master/web/src/assets/geo/US_states.json", "+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +a=6370997 +b=6370997 +units=m +no_defs", "US_states"
)

clean_map = function(map_obj) {
  map = sf::read_sf(map_obj$file_loc)
  # convert it to Albers equal area
  map = sf::st_transform(map, map_obj$proj4)
  
  if(map_obj$id %in% c("US_states", "US_metro", "US_counties")){
    
    # Based on https://github.com/hrbrmstr/rd3albers
    # and https://r-spatial.github.io/sf/articles/sf3.html#affine-transformations
    # extract, then rotate, shrink & move alaska (and reset projection)
    rot = function(a) matrix(c(cos(a), sin(a), -sin(a), cos(a)), 2, 2)
    
    alaska <- map[map$GEOID == "AK",]
    AK_ctr = st_centroid(alaska$geometry)
    AK_scale = 0.5
    AK = (alaska$geometry - AK_ctr) * rot((-50*pi)/180) * AK_scale + AK_ctr + c(0500000, -5000000)
    
    hawaii <- map[map$GEOID == "HI",]
    HI_ctr = st_centroid(alaska$geometry)
    HI_scale = 1.75
    HI = (hawaii$geometry - HI_ctr) * rot((-35*pi)/180) * HI_scale + HI_ctr + c(2.75e6, 3.5e6)
    
    puertorico <- map[map$GEOID == "PR",]
    PR_scale = 2
    PR_ctr = st_centroid(puertorico$geometry)
    PR = (puertorico$geometry) * rot((15*pi)/180) * PR_scale + PR_ctr + c(-6.8e6,6e6)
    
    map = map %>% mutate(geometry = st_sfc(ifelse(GEOID == "AK", AK[1], ifelse(GEOID == "HI", HI[1], ifelse(GEOID == "PR", PR[1], geometry)))))}
  return(map)
}

# Grab data: all metro locations ---------------------------------------------------------------
fetchOne = function(url, scroll_id=NA, query_num=1) {
  print(str_c("Executing query #", query_num))
  if(!is.na(scroll_id)) {
    url = str_c(url, "&scroll_id=", scroll_id)
  }
  resp = httr::GET(url)
  if(resp$status_code == 200) {
    all = fromJSON(content(resp, as="text"), flatten=TRUE)
    if(length(all[["success"]] == 0)) {
      return(NA)
    } else {
      return(list(df = all[["hits"]], id = all[["_scroll_id"]], count = query_num + 1))  
    }
    
  }
}


fetchAll = function(url) {
  url = str_c(url, "&fetch_all=true")
  df = tibble()
  res = fetchOne(url)
  
  while(!is.na(res)){
    df = df %>% bind_rows(res$df)
    res = fetchOne(url, res$id, res$count)
  }
  return(df)
}



getEpiData = function(id, variable, numColors = 9) {
  # Pull the constants for this variable
  map_obj = constants %>% filter(id == id)
  
  map = clean_map(map_obj)
  
  url = str_c("https://api.outbreak.info/covid19/query?q=",map_obj$query,"&fields=location_id,date,", variable)
  
  df = fetchAll(url)
  
  breaks = classIntervals(df %>% pull(variable), numColors, style="fisher")
  
  norm_breaks = c(-5000, -4000, breaks$brks)
  
  df = df %>% mutate(ids = str_split(location_id, "_")) %>% 
    rowwise() %>% 
    mutate(date_time = readr::parse_date(date, format="%Y-%m-%d"),
           GEOID = str_replace(ids[[2]], "US-", ""),
           fill = cut(.data[[variable]], norm_breaks))
  
  maps = map %>% left_join(df, by="GEOID")
  
  return(list(maps = maps, blank_map = map, breaks = norm_breaks))
}

maps = getEpiData("US_states", "confirmed_rolling_14days_ago_diff")

variable = "confirmed_rolling_14days_ago_diff"


# DC screws things up, since it has no polygon; filter out places without geoms
p1 =
  ggplot(maps$maps %>% filter(!st_is_empty(geometry))) +
  geom_sf(size = 0.2, data = maps$blank_map, fill = NA) +
  geom_sf(size = 0.2, aes(fill = fill, group=date)) + 
  # scale_fill_stepsn(colours = c("#313695", "#4575b4", "#74add1", "#abd9e9", "#e0f3f8", "#ffffbf", "#fee090", "#fdae61", "#f46d43", "#d73027", "#a50026"), limits=range(maps$breaks), breaks=maps$breaks[1:11], na.value = "white", show.limits=T, guide="colourbar") +
  scale_fill_manual(values=c("#313695", "#4575b4", "#74add1", "#abd9e9", "#e0f3f8", "#ffffbf", "#fee090", "#fdae61", "#f46d43", "#d73027", "#a50026"), breaks = levels(maps$fill), na.value = "white", drop=FALSE) +
  labs(title = "{frame_time}") +
  theme_void() +
  # #   theme(legend.position = "none") +
  transition_time(date_time)

p2 =
  ggplot(maps$maps %>% filter(!st_is_empty(geometry), !is.na(.data[[variable]])), aes(x=fill, fill=fill)) +
  geom_histogram(stat="count") +
  scale_fill_manual(values=c("#313695", "#4575b4", "#74add1", "#abd9e9", "#e0f3f8", "#ffffbf", "#fee090", "#fdae61", "#f46d43", "#d73027", "#a50026"), breaks = levels(maps$fill), na.value = "white", drop=FALSE) +
  theme_minimal() +
  labs(title = "{frame_time}") +
  transition_time(date_time)

map_anim2 <- animate(p2, fps=2, renderer = gifski_renderer(), end_pause = 20)
anim_save("test.gif", map_anim2)


ggplot(mpg, aes(x = displ, y = hwy, colour = hwy)) + 
  geom_point(size = 3) + 
  scale_colour_stepsn(colours = c("#313695", "#4575b4", "#74add1", "#abd9e9", "#e0f3f8", "#ffffbf", "#fee090", "#fdae61", "#f46d43", "#d73027", "#a50026"), 
                      breaks=seq(-2.5, 50, by = 5), 
                      limits = c(-5,50),
                      guide="legend")
