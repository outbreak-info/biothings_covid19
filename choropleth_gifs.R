library(readr)
library(dplyr)
library(dtplyr)
library(tidyr)
library(stringr)
library(classInt)
library(jsonlite)
library(sf)
library(ggplot2)
library(gganimate)
library(magick)
library(optparse)
# library(forcats)

option_list <- list(
  ## make_option(c("-h", "--help"), action="store_true", default=TRUE, help="Create GIFs from epi data"),
  make_option(c("-e", "--epi"), action="store", default=NA, type = 'character', help = "Location where the epidemiology csvs are saved. [Default: %default]"),
  make_option(c("-o", "--out"), action="store", default = "./", type = 'character', help = "Location where the GIFs are saved. [Default: %default]")
)
opt <- parse_args(OptionParser(option_list=option_list))

if(is.na(opt$epi)){
    stop("Path to directory with epidemiology csvs must be provided. See usage (--help)")
}

# constants ---------------------------------------------------------------
INPUT_DIR = opt$epi
OUTPUT_DIR = opt$out
# INPUT_DIR = "Documents/2019-nCoV/data/epi/"
# OUTPUT_DIR = "Documents/2019-nCoV/data/epi/"

# define variables to loop over
EPI_VARS = c("confirmed_per_100k", "confirmed_rolling", "confirmed_rolling_per_100k", "confirmed_rolling_14days_ago_diff", "confirmed_rolling_14days_ago_diff_per_100k", "dead_per_100k", "dead_rolling", "dead_rolling_per_100k", "dead_rolling_14days_ago_diff", "dead_rolling_14days_ago_diff_per_100k")

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
generateGifs = function(numColors = 9, exportGif = TRUE) {
  # loop over locations
  locations = GEO_CONSTANTS %>% 
    rowwise() %>%
    mutate(breaks = list(processLocation(epi_file, map_file, proj4, id, numColors, exportGif)))
  location_df = locations %>% select(id, breaks) %>% unnest(cols = c(breaks))
  return(toJSON(location_df))
}


# processLocation ---------------------------------------------------------
# 1. loads in the geographic shapefiles; transforms to correct projection, etc.
# 2. loads in the epidemiology data for that location
# 3. For each variable:
#     • calculates Fisher breaks for the color ramp
#     • calculates a histogram based on those breaks
#     • merges data with the geographic shape file
#     • generates and saves a .gif for each
processLocation = function(epi_file, map_file, proj4, location, numColors, exportGif = TRUE) {
  # loop over variables
  breaks = lapply(EPI_VARS, function(variable) processVariable(epi_file, map_file, proj4, location, variable, numColors, exportGif = exportGif))
  breaks_df = breaks %>% bind_cols() %>% mutate(location = location)
  return(breaks_df)
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
processVariable = function(epi_file, map_file, proj4, location, variable, numColors, maxN = 25000, exportGif = TRUE, returnJson = FALSE, returnAll = FALSE) {
  print(str_c("processing variable ", variable, " for location ", location))
  
  map = cleanMap(map_file, proj4, location)
  
  df = readData(epi_file)
  
  # data.table manipulations are faster...
  dt = lazy_dt(df) %>% 
    filter(!is.na(.data[[variable]])) 
  
  # Classify the breaks
  domain = calcBreaks(dt, variable, numColors, maxN)
  
  if(all(!is.na(domain))) {
    break_limits = tibble(midpt = (domain + domain %>% lag())/2, lower = domain %>% lag(), upper =  domain, width = upper - lower) %>% 
      filter(!is.na(midpt))
    
    dt = dt %>% 
      mutate(fill = cut(.data[[variable]], domain))
    
    counts = dt %>% 
      group_by(date) %>% 
      do(h = calcHist(.data[[variable]], breaks = domain)) %>% 
      as_tibble() %>% 
      unnest(cols = c(h)) %>% 
      mutate(fill = cut(midpt, domain)) %>%
        left_join(break_limits, by = "midpt")
    
    # geo join data. data.table faster than dplyr...
    maps = dt %>% inner_join(map, by="location_id")  %>% as_tibble()
    # %>% 
    #   filter(!is.na(date))
    # maps = maps[!is.na(date),] # remove the counties w/ no data
    sf::st_geometry(maps) = "geometry"
    
    # Create the gifs
    if(exportGif) {
      createGif(maps, map, domain, counts, variable, location)
    }
    
    if(returnAll) {
      return(list(maps = maps, blank_map = map, breaks = domain, hist = counts))
    } else {
      if(returnJson) {
        return(toJSON(tibble(!!(paste0(variable, "_breaks")) := list(domain))))
      }
      return(tibble(!!(paste0(variable, "_breaks")) := list(domain)))
    }
  }
}




# calcBreaks --------------------------------------------------------------
calcBreaks = function(df, variable, numColors, maxN, style="fisher") {
  # Maximum value to sample to calculate breaks.
  # Necessary because a classification of 280,000 elements is insanely slow.
  # from classInt: "default 3000L, the QGIS sampling threshold; over 3000, the observations presented to "fisher" and "jenks" are either a samp_prop= sample or a sample of 3000, whichever is larger"
  # Doing this manually, since this MAY exclude the min/max values, AND the larger of 10% of 280,000 is REALLY slow (I assume classInt is doing some sort of sampling + replacement), and unclear if there are benefits to getting the precise breaks
  
  set.seed(25)
  if(variable %in% df$vars) {
    values = df %>% pull(.data[[variable]])
    # Manual sampling of the data so things don't blow up too much.
    # making sure to add the max and min value
    if(length(values) > maxN) {
      values = c(min(values), max(values), values[values != max(values) & values != min(values)] %>% sample(maxN))
    }
    
    breaks = classIntervals(values, numColors, style=style, warnLargeN = FALSE)
    
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
        domain = c(domain, rep(maxVal, -1*padLength) + seq(1, by=1, length.out=(-1*padLength)))
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
    print(str_c("    WARNING: variable ", variable, " is not found. Skipping calculating breaks"))
    return(NA)
  }
}



# Geoprocessing -----------------------------------------------------------
# • Projects to an appropriate projection
# • For the US, upscales Hawaii/Puerto Rico and downsizes Alaska (sorry, you're just too big) and rotates/translates to a nicer location
cleanMap = function(map_file, proj4, id) {
  # Make sure to remove empty polygons. DC disappears due to mapshaper smoothing
  # DC screws things up, since it has no polygon; filter out places without geoms
  map = sf::read_sf(map_file) %>% filter(!st_is_empty(geometry))
  # convert it to Albers equal area
  map = sf::st_transform(map, proj4) 
  
  if(id %in% c("US_states", "US_metro", "US_counties")) {
    
    # Based on https://github.com/hrbrmstr/rd3albers
    # and https://r-spatial.github.io/sf/articles/sf3.html#affine-transformations
    # extract, then rotate, shrink & move alaska (and reset projection)
    rot = function(a) matrix(c(cos(a), sin(a), -sin(a), cos(a)), 2, 2)
    
    if(id == "US_states") {
      alaska <- map[map$location_id == "USA_US-AK",]
      AK_ctr = st_centroid(alaska$geometry)
      AK_scale = 0.5
      AK = (alaska$geometry - AK_ctr) * rot((-50*pi)/180) * AK_scale + AK_ctr + c(0500000, -5000000)
      
      hawaii <- map[map$location_id == "USA_US-HI",]
      HI_ctr = st_centroid(alaska$geometry)
      HI_scale = 1.75
      HI = (hawaii$geometry - HI_ctr) * rot((-35*pi)/180) * HI_scale + HI_ctr + c(2.75e6, 3.5e6)
      
      puertorico <- map[map$location_id == "USA_US-PR",]
      PR_scale = 2
      PR_ctr = st_centroid(puertorico$geometry)
      PR = (puertorico$geometry) * rot((15*pi)/180) * PR_scale + PR_ctr + c(-6.8e6,6e6)
      
      map = map %>% mutate(geometry = st_sfc(ifelse(location_id == "USA_US-AK", AK[1], ifelse(location_id == "USA_US-HI", HI[1], ifelse(location_id == "USA_US-PR", PR[1], geometry)))))
    }  
    if(id == "US_counties") {
      # alaska <- map[map$STATEFP == "02",]
      # AK_ctr = st_centroid(alaska$geometry)
      # AK_scale = 0.5
      # AK = (alaska$geometry - AK_ctr) * rot((-50*pi)/180) * AK_scale + AK_ctr + c(0500000, -5000000)
      # 
      # hawaii <- map[map$location_id == "HI",]
      # HI_ctr = st_centroid(alaska$geometry)
      # HI_scale = 1.75
      # HI = (hawaii$geometry - HI_ctr) * rot((-35*pi)/180) * HI_scale + HI_ctr + c(2.75e6, 3.5e6)
      # 
      # puertorico <- map[map$location_id == "PR",]
      # PR_scale = 2
      # PR_ctr = st_centroid(puertorico$geometry)
      # PR = (puertorico$geometry) * rot((15*pi)/180) * PR_scale + PR_ctr + c(-6.8e6,6e6)
      # 
      # map = map %>% mutate(geometry = st_sfc(ifelse(STATEFP == "02", AK[1], ifelse(STATEFP == "15", HI[1], ifelse(STATEFP == "72", PR[1], geometry)))))}  
    }
  }
  return(map)
}


# calcHist ----------------------------------------------------------------
calcHist = function(values, breaks) {
  hist_values = hist(values, breaks = breaks, plot = FALSE)
  return(tibble(count = hist_values$counts, midpt = hist_values$mids))
}


# createGif ---------------------------------------------------------------
createGif = function(maps, blank_map, breaks, hist, variable, location) {
  fps=2
  
  # Labels for histogram
  variableLabels = tibble(confirmed_per_100k = "total cases per 100,000 residents",
                          confirmed_rolling="7 day average of daily cases", 
                          confirmed_rolling_per_100k = "7 day average of daily cases per 100,000 residents", 
                          confirmed_rolling_14days_ago_diff = "average cases vs. 2 weeks ago",
                          confirmed_rolling_14days_ago_diff_per_100k = "average cases per 100,000 residents vs. 2 weeks ago", 
                          dead_per_100k = "total deaths per 100,000 residents", 
                          dead_rolling = "7 day average of daily deaths", 
                          dead_rolling_per_100k = "7 day average of daily deaths per 100,00 residents", 
                          dead_rolling_14days_ago_diff = "average deaths vs. 2 weeks ago", 
                          dead_rolling_14days_ago_diff_per_100k = "average deaths vs. 2 weeks ago")
  geoLocations = tibble(US_states = "U.S. states", US_metros = "U.S. metropolitan areas", US_counties = "U.S. counties", admin0 = "countries")
  
  # Interpolate color palette
  if(str_detect(variable, "diff")) {
    colorPalette = colorRampPalette(c("#313695", "#4575b4", "#74add1", "#abd9e9", "#e0f3f8", "#ffffbf", "#fee090", "#fdae61", "#f46d43", "#d73027", "#a50026"),
                                    space="Lab")(length(breaks) - 1)
  } else {
    colorPalette = colorRampPalette(c("#ffffbf", "#fee090", "#fdae61", "#f46d43", "#d73027", "#a50026"),
                                    space="Lab")(length(breaks) - 1)
  }
  
  # Check the histogram and map have the same number of frames
  num_frames = length(unique(hist$date))
  if(length(unique(maps$date)) != num_frames) {
    stop("Mismatch in number of frames between histogram legend and map")
  }
  # --- MAP ---  
  p_map =
    ggplot(maps) +
    geom_sf(size = 0.1, aes(fill = fill, group=date)) + 
    geom_sf(size = 0.2, data = blank_map, fill = NA) +
    # scale_fill_stepsn(colours = c("#313695", "#4575b4", "#74add1", "#abd9e9", "#e0f3f8", "#ffffbf", "#fee090", "#fdae61", "#f46d43", "#d73027", "#a50026"), limits=range(maps$breaks), breaks=maps$breaks[1:11], na.value = "white", show.limits=T, guide="colourbar") +
    scale_fill_manual(values=colorPalette, breaks = levels(maps$fill), na.value = "white", drop=FALSE) +
    labs(title = "{format(frame_time, '%d %B %Y')}") +
    theme_void() +
    theme(legend.position = "none", plot.title = element_text(size=18, hjust = 0.5)) +
    transition_time(date)
  
  # --- HISTOGRAM LEGEND ---s
  barWidth = min(hist %>% filter(width > 1) %>% pull(width), na.rm = TRUE) * 0.45
  maxVal = hist %>% pull(upper) %>% max()
  # 
  # worstPlaces = st_drop_geometry(df) %>% 
  #   group_by(date) %>% 
  #   mutate(rank = row_number(desc(confirmed_rolling))) %>% 
  #   filter(rank <= 5)
  # 
  # ggplot(worstPlaces, aes(x = rank, y = confirmed_rolling, fill = name, group = name)) +
  # geom_col(width = 0.8, position="identity") + 
  #   geom_text(aes(label = name, y = 0)) +
  #   labs(title = "{format(frame_time, '%d %B %Y')}") +
  #   # scale_y_log10() +
  #   # scale_x_reverse() +
  #   coord_flip() + transition_states(date,4,1)
  # 
  # ggplot(worstPlaces %>% filter(date =="2020-03-01" || date =="2020-07-01"),
  #          aes(x = confirmed_rolling, y = rank, group = name, fill=fill)) + 
  #   enter_grow() +
  #   exit_shrink() +
  #   exit_fly(x_loc = 0, y_loc = 0) + enter_fly(x_loc = 0, y_loc = 0) +
  # ease_aes('cubic-in-out') +
  #   geom_point(size = 3, shape=21) +
  #   labs(title = "{format(frame_time, '%d %B %Y')}") +
  #   scale_fill_manual(values=colorPalette, breaks = levels(hist$fill), na.value = "white", drop=FALSE) +
  #   theme_minimal() +
  #   transition_time(date)
  
  p_legend =
    ggplot(hist)
  
  if(str_detect(variable, "diff")) {
    nudge = range(hist$midpt) %>% sapply(function(x) abs(x)) %>% min() * 0.05
    p_legend = p_legend +
      geom_vline(xintercept = 0, colour = "#aabdd1", size = 0.25, linetype = 2) +
      geom_text(aes(x = 0, y = pretty(hist$count) %>% last(), label = paste("\u2190","better")), hjust = 1, nudge_x = -1*nudge, data = tibble()) +
      geom_text(aes(x = 0, y = pretty(hist$count) %>% last(), label =paste("worse", "\u2192")), hjust = 0, nudge_x = nudge, data = tibble())
  }
  
  p_legend = p_legend +
    geom_hline(yintercept = 0, colour = "#2c3e50") + 
    geom_rect(aes(xmin=midpt - barWidth, xmax = midpt+ barWidth, ymin=0, ymax=count, fill=fill), colour = "#2c3e50", size = 0.2) +
    geom_rect(aes(ymin = -5, ymax=-2, xmin = lower, xmax = upper, fill = fill)) +
    geom_text(aes(y=-5, x=lower, label=scales::comma(round(lower), accuracy=1)), nudge_y = -4, check_overlap = TRUE) +
    geom_text(aes(y=-5, x=maxVal %>% max(), label=scales::comma(round(maxVal), accuracy=1)), nudge_y = -4, check_overlap = TRUE) +
    scale_fill_manual(values=colorPalette, breaks = levels(hist$fill), na.value = "white", drop=FALSE) +
    scale_y_continuous(breaks = pretty(hist$count)) +
    labs(title = paste("Number of", geoLocations[[location]]), subtitle= variableLabels[[variable]])+
    xlab(variableLabels[[variable]]) +
    enter_grow() +
    exit_shrink() +
    ease_aes('sine-in-out') +
    transition_time(date) +
    theme_minimal() + 
    theme(
      legend.position = "none",
      axis.line.x = element_blank(), 
      axis.text = element_blank(),
      axis.title.x = element_text(),
      axis.ticks.x = element_blank(), 
      panel.grid.major.x = element_blank(),
      panel.grid.minor.x = element_blank(),
      panel.grid.minor.y = element_blank(),
      panel.grid.major.y = element_line(size = 0.25, colour="#aabdd1"),
      axis.text.y = element_text(size = 14),
      axis.title = element_blank())
  
  # Create the animation frames  
  map_gif = animate(p_map, fps=fps, nframes = num_frames, renderer = magick_renderer(), width = 500, height=350)
  legend_gif = animate(p_legend, fps=fps, nframes = num_frames, renderer = magick_renderer(), width = 300, height=200)
  
  if(length(map_gif) != length(legend_gif)) {
    error("Mismatch in number of frames between histogram legend and map")
  }
  
  # Combine together 
  combined_gif <- image_append(c(map_gif[1], legend_gif[1]), stack=FALSE)
  for(i in 2:num_frames){
    combined <- image_append(c(map_gif[i], legend_gif[i]), stack=FALSE)
    combined_gif <- c(combined_gif, combined)
  }
  
  # Export!
  # Note: .mp4 is ~ 200 KB while .gif is 2-4 MB so going with the smaller file.
  # image_write_gif(combined_gif, "testergif.gif", delay=1/fps)
  image_write_video(combined_gif, paste0(OUTPUT_DIR, location, "_", variable, "_", format(Sys.Date(), "%Y-%m-%d"), ".mp4"), framerate=fps)
  
  
  
  # Line trace of change over time  
  # total = st_drop_geometry(maps$maps) %>% group_by(date) %>% summarise(total = sum(.data[[variable]], na.rm=TRUE))
  # yMax = max(total$total) * 1.1
  # yMin = min(total$total) * 1.1
  # p4 = ggplot(total) + 
  #   annotate(geom ="rect", xmin = as.Date("2020-01-21"), xmax=as.Date("2020-07-11"), ymin = 0, ymax=yMax, fill = "#fdae61", alpha = 0.25) + 
  #   annotate(geom ="rect", xmin = as.Date("2020-01-21"), xmax=as.Date("2020-07-11"), ymin = 0, ymax=yMin, fill = "#abd9e9", alpha = 0.3) + 
  #   annotate(geom="text", x = as.Date("2020-01-21"), y = yMax, label = "WORSE THAN 2 WEEKS BEFORE", colour = "#f46d43", hjust = -0.025, vjust = 1.5) + 
  #   annotate(geom="text", x = as.Date("2020-01-21"), y = yMin, label = "BETTER THAN 2 WEEKS BEFORE", colour = "#4575b4", hjust = -0.025, vjust = -0.5) + 
  #   geom_hline(yintercept = 0) +  
  #   geom_line(aes(x = date, y = total, group="USA"), colour = "#2c3e50", size = 1) + 
  #   geom_point(aes(x = date, y = total, group="USA"), colour = "#2c3e50", size = 2) + 
  #   ggtitle("Change in daily number of U.S. cases compared to two weeks prior") + 
  #   scale_y_continuous(label = scales::comma) + 
  #   theme_minimal() +
  #   theme(text = element_text(size=20), axis.title = element_blank(), title = element_text(size = 16)) +
  #   ease_aes('sine-in-out') +
  #   transition_reveal(date)
  # x = animate(p4, fps=5, nframes = num_frames), renderer = gifski_renderer(), end_pause = 20, width = 700, height = 500)
}

# invoke the function -----------------------------------------------------
breaks = generateGifs(exportGif = FALSE)
# Can also be run individually, returning a dataframe or JSON
# microbenchmark(breaks = processVariable(GEO_CONSTANTS$epi_file[4], GEO_CONSTANTS$map_file[4], GEO_CONSTANTS$proj4[4], GEO_CONSTANTS$id[4], "confirmed_rolling_14days_ago_diff", 9, returnJson = TRUE, exportGif = F), times = 1)
breaks = processVariable(GEO_CONSTANTS$epi_file[4], GEO_CONSTANTS$map_file[4], GEO_CONSTANTS$proj4[4], GEO_CONSTANTS$id[4], "confirmed_rolling_14days_ago_diff", 9, returnJson = TRUE, exportGif = T)
breaks2 = processVariable(GEO_CONSTANTS$epi_file[2], GEO_CONSTANTS$map_file[2], GEO_CONSTANTS$proj4[2], GEO_CONSTANTS$id[2], "confirmed_rolling", 9, returnAll = TRUE, exportGif = T)
# breaks = processLocation(GEO_CONSTANTS$epi_file[2], GEO_CONSTANTS$map_file[2], GEO_CONSTANTS$proj4[2], GEO_CONSTANTS$id[2], 9, exportGif = T)
