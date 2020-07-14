library(httr)
library(jsonlite)
library(sf)
library(stringr)
library(dplyr)
library(tidyr)
library(ggplot2)
library(classInt)
library(gganimate)
library(readr)
library(gifski)
library(magick)
# install.packages("transformr")

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

calcHist = function(values, breaks) {
  hist_values = hist(values, breaks = breaks, plot = FALSE)
  return(tibble(count = hist_values$counts, midpt = hist_values$mids))
}

calcBreaks = function(values, numColors) {
  breaks = classIntervals(values, numColors, style="fisher")
  
  # Ensure the breaks are centered at 0
  midpoint = which((breaks$brks < 0 & breaks$brks %>% lead() > 0) | breaks$brks == 0)
  
  padLength = length(breaks$brks) - 2 * midpoint - 1;
  domain = breaks$brks
  
  # ensure that the padding is an even number, so the limits all apply
  if(padLength %% 2) {
    padLength = padLength + 1
  }
  
  if(padLength < 0) {
    maxVal = max(domain)
    domain = c(domain, rep(maxVal, -1*padLength) + seq(1, by=1, length.out=-1*padLength))
  } 
  if(padLength > 0 ) {
    minVal = min(domain)
    domain = c(rep(minVal, padLength)+ seq(1, by=1, length.out=padLength), domain)
  }
  
  return(sort(domain))
}

getEpiData = function(id, variable, numColors = 9) {
  # Pull the constants for this variable
  map_obj = constants %>% filter(id == id)
  
  map = clean_map(map_obj)
  
  url = str_c("https://api.outbreak.info/covid19/query?q=",map_obj$query,"&fields=location_id,date,", variable)
  
  df = fetchAll(url)
  
  # Classify the breaks
  domain = calcBreaks(df %>% pull(variable), numColors)
  
  break_limits = tibble(midpt = (domain + domain %>% lag())/2, lower = domain %>% lag(), upper =  domain, width = upper - lower)%>% filter(!is.na(midpt))
  
  
  df = df %>% mutate(ids = str_split(location_id, "_")) %>% 
    rowwise() %>% 
    mutate(date_time = readr::parse_date(date, format="%Y-%m-%d"),
           GEOID = str_replace(ids[[2]], "US-", ""),
           fill = cut(.data[[variable]], domain))
  
  counts = df %>% 
    group_by(date_time) %>% 
    filter(!is.na(.data[[variable]])) %>% 
    do(h = calcHist(.data[[variable]], breaks = domain)) %>% 
    unnest(cols = c(h)) %>% 
    mutate(fill = cut(midpt, domain))
  
  counts = counts %>% left_join(break_limits, by = "midpt")
  
  maps = map %>% left_join(df, by="GEOID")
  
  
  
  return(list(maps = maps, blank_map = map, breaks = domain, hist = counts))
}



variable = "confirmed_rolling_14days_ago_diff"
maps = getEpiData("US_states", variable)

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

barWidth = min(maps$hist %>% filter(width > 1) %>% pull(width), na.rm = TRUE) * 0.45
colorPalette = colorRampPalette(c("#313695", "#4575b4", "#74add1", "#abd9e9", "#e0f3f8", "#ffffbf", "#fee090", "#fdae61", "#f46d43", "#d73027", "#a50026"),
                                space="Lab")(length(maps$breaks) - 1)
p2 =
  ggplot(maps$hist, aes(xmin=midpt - barWidth, xmax = midpt+ barWidth, ymin=0, ymax=count, fill=fill)) +
  geom_rect(colour = "#2c3e50", size = 0.2) +
  scale_fill_manual(values=colorPalette, breaks = levels(maps$hist$fill), na.value = "white", drop=FALSE) +
  theme_minimal() +
  geom_rect(aes(ymin = -2.5, ymax=-1, xmin = lower, xmax = upper, fill = fill)) +
  geom_text(aes(y=-2.5, x=lower, label=round(lower)), nudge_y = -1) +
  labs(title = "{current_frame}") +
  ease_aes('sine-in-out') +
  transition_manual(date_time)


a_gif = animate(p1, fps=2, renderer = gifski_renderer(), end_pause = 20, width = 500, height=350)
b_gif = animate(p2, fps=2, detail= 2, nframes = length(unique(maps$hist$date_time)), renderer = gifski_renderer(), end_pause = 20)
a_mgif <- image_read(a_gif)
b_mgif <- image_read(b_gif)

new_gif <- image_append(c(b_mgif[1], c_mgif[1]), stack=T )
for(i in 2:100){
  combined <- image_append(c(b_mgif[i], c_mgif[i]), stack=T)
  new_gif <- c(new_gif, combined)
}

# new_gif

map_anim2 = animate(p2, fps=2, renderer = gifski_renderer(), end_pause = 20)
# image_animate(map_anim2, "test.gif", fps=2)


ggplot(mpg, aes(x = displ, y = hwy, colour = hwy)) + 
  geom_point(size = 3) + 
  scale_colour_stepsn(colours = c("#313695", "#4575b4", "#74add1", "#abd9e9", "#e0f3f8", "#ffffbf", "#fee090", "#fdae61", "#f46d43", "#d73027", "#a50026"), 
                      breaks=seq(-2.5, 50, by = 5), 
                      limits = c(-5,50),
                      guide="legend")

total = st_drop_geometry(maps$maps) %>% group_by(date_time) %>% summarise(total = sum(.data[[variable]], na.rm=TRUE))

yMax = max(total$total) * 1.1
yMin = min(total$total) * 1.1
p4 = ggplot(total) + 
  annotate(geom ="rect", xmin = as.Date("2020-01-21"), xmax=as.Date("2020-07-11"), ymin = 0, ymax=yMax, fill = "#fdae61", alpha = 0.25) + 
  annotate(geom ="rect", xmin = as.Date("2020-01-21"), xmax=as.Date("2020-07-11"), ymin = 0, ymax=yMin, fill = "#abd9e9", alpha = 0.3) + 
  annotate(geom="text", x = as.Date("2020-01-21"), y = yMax, label = "WORSE THAN 2 WEEKS BEFORE", colour = "#f46d43", hjust = -0.025, vjust = 1.5) + 
  annotate(geom="text", x = as.Date("2020-01-21"), y = yMin, label = "BETTER THAN 2 WEEKS BEFORE", colour = "#4575b4", hjust = -0.025, vjust = -0.5) + 
  geom_hline(yintercept = 0) +  
  geom_line(aes(x = date_time, y = total, group="USA"), colour = "#2c3e50", size = 1) + 
  geom_point(aes(x = date_time, y = total, group="USA"), colour = "#2c3e50", size = 2) + 
  ggtitle("Change in daily number of U.S. cases compared to two weeks prior") + 
  scale_y_continuous(label = scales::comma) + 
  theme_minimal() +
  theme(text = element_text(size=20), axis.title = element_blank(), title = element_text(size = 16)) +
  ease_aes('sine-in-out') +
  transition_reveal(date_time)
x = animate(p4, fps=5, nframes = length(unique(maps$hist$date_time)), renderer = gifski_renderer(), end_pause = 20, width = 700, height = 500)
anim_save("US_confirmed_rolling_diff.gif", x)
# anim_save("US_dead_rolling_diff.gif", x)
