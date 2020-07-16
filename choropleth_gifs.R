library(readr)
library(dplyr)
library(tidyr)
library(stringr)
library(classInt)
library(jsonlite)
library(sf)
library(ggplot2)
library(gganimate)
library(gifski)
library(magick)


# constants ---------------------------------------------------------------
INPUT_DIR = "Documents/2019-nCoV/data/epi/" # location where the Epidemiology .csvs are saved
OUTPUT_DIR = "Documents/2019-nCoV/data/epi/" # location where the .gifs are saved

# define variables to loop over
EPI_VARS = c("confirmed_rolling", "confirmed_rolling_per_100k", "confirmed_rolling_14days_ago_diff", "confirmed_rolling_14days_ago_diff_per_100k", "dead_rolling", "dead_rolling_per_100k", "dead_rolling_14days_ago_diff", "dead_rolling_14days_ago_diff_per_100k")

# define geographic regions to loop over
GEO_CONSTANTS = tribble(
  ~id, ~epi_file, ~map_file, ~proj4, 
  # Note: Equal earth projection requires Proj6 in GDAL (https://github.com/OSGeo/gdal/issues/870)
  # "admin0", "test_admin0.csv", "https://raw.githubusercontent.com/SuLab/outbreak.info/master/web/src/assets/geo/countries.json", "+proj=eqearth", 
  "admin0", "test_admin0.csv", "https://raw.githubusercontent.com/SuLab/outbreak.info/master/web/src/assets/geo/countries.json", "+proj=robin", 
  "US_states", "test_states.csv", "https://raw.githubusercontent.com/SuLab/outbreak.info/master/web/src/assets/geo/US_states.json", "+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +a=6370997 +b=6370997 +units=m +no_defs",
  "US_metros", "test_metros.csv", "https://raw.githubusercontent.com/SuLab/outbreak.info/master/web/src/assets/geo/US_metro.json", "+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +a=6370997 +b=6370997 +units=m +no_defs",
  "US_counties", "test_counties.csv", "https://raw.githubusercontent.com/SuLab/outbreak.info/master/web/src/assets/geo/US_counties.json", "+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +a=6370997 +b=6370997 +units=m +no_defs"
)


# main function -----------------------------------------------------------
generateGifs = function(numColors = 9) {
  # loop over locations
  locations = GEO_CONSTANTS %>% 
    rowwise() %>%
    mutate(breaks = list(processLocation(epi_file, map_file, proj4, id, numColors)))
  location_df = locations %>% select(id, breaks) %>% unnest(cols = c(breaks))
  return(jsonlite::toJSON(location_df))
}


# processLocation ---------------------------------------------------------
# 1. loads in the geographic shapefiles; transforms to correct projection, etc.
# 2. loads in the epidemiology data for that location
# 3. For each variable:
#     • calculates Fisher breaks for the color ramp
#     • calculates a histogram based on those breaks
#     • merges data with the geographic shape file
#     • generates and saves a .gif for each

processLocation = function(epi_file, map_file, proj4, id, numColors) {
  map = cleanMap(map_file, proj4, id)
  
  df = readData(epi_file)
  
  # loop over variables
  if(!is.na(df)) {
    breaks = sapply(EPI_VARS, function(x) processVariable(map, df, id, x, numColors))
    breaks_df = tibble(variable = names(breaks), breaks = breaks)
    return(breaks_df)
  }
}

readData = function(epi_file) {
  out <- tryCatch(
    {
      read_csv(str_c(INPUT_DIR, epi_file), col_types = cols(date = col_date(format = "%Y-%m-%d")))
    },
    error=function(cond) {
      message(paste("File does not exist:", INPUT_DIR, epi_file))
      message("Skipping this file \n")
      return(NA)
    },
    warning=function(cond) {
      return(NULL)
    },
    finally={
    }
  )    
  return(out)
}


# processVariable ---------------------------------------------------------
# Main workhorse to calculate the breaks, histograms, and generate the gifs
processVariable = function(map, df, location, variable, numColors, returnAll = FALSE) {
  print(str_c("processing variable ", variable, " for location ", location))
  # Classify the breaks
  domain = calcBreaks(df, variable, numColors)
  
  if(!is.na(domain)){
    break_limits = tibble(midpt = (domain + domain %>% lag())/2, lower = domain %>% lag(), upper =  domain, width = upper - lower)%>% filter(!is.na(midpt))
    
    
    df = df %>% 
      filter(!is.na(.data[[variable]])) %>% 
      mutate(ids = str_split(location_id, "_")) %>% 
      rowwise() %>% 
      mutate(GEOID = str_replace(ids[[2]], "US-", ""),
             fill = cut(.data[[variable]], domain))
    
    counts = df %>% 
      group_by(date) %>% 
      do(h = calcHist(.data[[variable]], breaks = domain)) %>% 
      unnest(cols = c(h)) %>% 
      mutate(fill = cut(midpt, domain))
    
    counts = counts %>% left_join(break_limits, by = "midpt")
    
    maps = map %>% left_join(df, by="GEOID")
    if(returnAll) {
      return(list(maps = maps, blank_map = map, breaks = domain, hist = counts))
    } else {
      return(domain)
    }
    
    # Create the gifs
    # createGif(maps)
  }
}




# calcBreaks --------------------------------------------------------------
calcBreaks = function(df, variable, numColors, style="fisher") {
  if(variable %in% colnames(df)) {
    values = df %>% pull(variable)
    breaks = classIntervals(values, numColors, style=style)
    
    
    if(str_detect(variable, "_diff")) {
      # Ensure the breaks are centered at 0 if it's a difference
      midpoint = which((breaks$brks < 0 & breaks$brks %>% lead() > 0) | breaks$brks == 0)
      
      padLength = length(breaks$brks) - 2 * midpoint; # changes from JS code, since .js 0-indexes, while R is 1-based.
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
    } else {
      domain = breaks$brks
    }
    
    return(sort(domain)) 
  } else {
    print(str_c("WARNING: variable ", variable, "is not found. Skipping calculating breaks"))
    return(NA)
  }
}



# Geoprocessing -----------------------------------------------------------
# • Projects to an appropriate projection
# • For the US, upscales Hawaii/Puerto Rico and downsizes Alaska (sorry, you're just too big) and rotates/translates to a nicer location
cleanMap = function(map_file, proj4, id) {
  map = sf::read_sf(map_file)
  # convert it to Albers equal area
  map = sf::st_transform(map, proj4)
  
  if(id %in% c("US_states", "US_metro", "US_counties")){
    
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


# calcHist ----------------------------------------------------------------
calcHist = function(values, breaks) {
  hist_values = hist(values, breaks = breaks, plot = FALSE)
  return(tibble(count = hist_values$counts, midpt = hist_values$mids))
}


# createGif ---------------------------------------------------------------
createGif = function(maps) {
  colorPalette = colorRampPalette(c("#313695", "#4575b4", "#74add1", "#abd9e9", "#e0f3f8", "#ffffbf", "#fee090", "#fdae61", "#f46d43", "#d73027", "#a50026"),
                                  space="Lab")(length(maps$breaks) - 1)
  num_frames = length(unique(maps$hist$date))
  if(length(unique(maps$maps$date)) != num_frames) {
    error("Mismatch in number of frames between histogram legend and map")
  }
  
  
  # DC screws things up, since it has no polygon; filter out places without geoms
  p_map =
    ggplot(maps$maps %>% filter(!st_is_empty(geometry))) +
    geom_sf(size = 0.2, data = maps$blank_map, fill = NA) +
    geom_sf(size = 0.2, aes(fill = fill, group=date)) + 
    # scale_fill_stepsn(colours = c("#313695", "#4575b4", "#74add1", "#abd9e9", "#e0f3f8", "#ffffbf", "#fee090", "#fdae61", "#f46d43", "#d73027", "#a50026"), limits=range(maps$breaks), breaks=maps$breaks[1:11], na.value = "white", show.limits=T, guide="colourbar") +
    scale_fill_manual(values=colorPalette, breaks = levels(maps$fill), na.value = "white", drop=FALSE) +
    labs(title = "{format(frame_time, '%d %B %Y')}") +
    theme_void() +
    theme(legend.position = "none", plot.title = element_text(size=18, hjust = 0.5)) +
    transition_time(date)
  
  barWidth = min(maps$hist %>% filter(width > 1) %>% pull(width), na.rm = TRUE) * 0.45
  p_legend =
    ggplot(maps$hist, aes(xmin=midpt - barWidth, xmax = midpt+ barWidth, ymin=0, ymax=count, fill=fill)) +
    geom_rect(colour = "#2c3e50", size = 0.2) +
    scale_fill_manual(values=colorPalette, breaks = levels(maps$hist$fill), na.value = "white", drop=FALSE) +
    theme_minimal() +
    geom_rect(aes(ymin = -2.5, ymax=-1, xmin = lower, xmax = upper, fill = fill)) +
    geom_text(aes(y=-2.5, x=lower, label=round(lower)), nudge_y = -1) +
    labs(title = "{current_frame}") +
    # ease_aes('sine-in-out') +
    transition_time(date)
  
  fps=2
  
  map_gif = animate(p_map, fps=fps, nframes = num_frames, renderer = magick_renderer(), width = 500, height=350)
  legend_gif = animate(p_legend, fps=fps, nframes = num_frames, renderer = magick_renderer())
  
  if(length(map_gif) != length(legend_gif)) {
    error("Mismatch in number of frames between histogram legend and map")
  }
  
  combined_gif <- image_append(c(map_gif[1], legend_gif[1]), stack=FALSE)
  for(i in 2:num_frames){
    combined <- image_append(c(map_gif[i], legend_gif[i]), stack=FALSE)
    combined_gif <- c(combined_gif, combined)
  }
  
  image_write(combined_gif, "tester.gif")
  
  total = st_drop_geometry(maps$maps) %>% group_by(date) %>% summarise(total = sum(.data[[variable]], na.rm=TRUE))
  
  yMax = max(total$total) * 1.1
  yMin = min(total$total) * 1.1
  p4 = ggplot(total) + 
    annotate(geom ="rect", xmin = as.Date("2020-01-21"), xmax=as.Date("2020-07-11"), ymin = 0, ymax=yMax, fill = "#fdae61", alpha = 0.25) + 
    annotate(geom ="rect", xmin = as.Date("2020-01-21"), xmax=as.Date("2020-07-11"), ymin = 0, ymax=yMin, fill = "#abd9e9", alpha = 0.3) + 
    annotate(geom="text", x = as.Date("2020-01-21"), y = yMax, label = "WORSE THAN 2 WEEKS BEFORE", colour = "#f46d43", hjust = -0.025, vjust = 1.5) + 
    annotate(geom="text", x = as.Date("2020-01-21"), y = yMin, label = "BETTER THAN 2 WEEKS BEFORE", colour = "#4575b4", hjust = -0.025, vjust = -0.5) + 
    geom_hline(yintercept = 0) +  
    geom_line(aes(x = date, y = total, group="USA"), colour = "#2c3e50", size = 1) + 
    geom_point(aes(x = date, y = total, group="USA"), colour = "#2c3e50", size = 2) + 
    ggtitle("Change in daily number of U.S. cases compared to two weeks prior") + 
    scale_y_continuous(label = scales::comma) + 
    theme_minimal() +
    theme(text = element_text(size=20), axis.title = element_blank(), title = element_text(size = 16)) +
    ease_aes('sine-in-out') +
    transition_reveal(date)
  x = animate(p4, fps=5, nframes = num_frames), renderer = gifski_renderer(), end_pause = 20, width = 700, height = 500)
anim_save("US_confirmed_rolling_diff.gif", x)
# anim_save("US_dead_rolling_diff.gif", x)
}

# invoke the function -----------------------------------------------------
breaks = generateGifs()
