import pandas as pd
import os
from datetime import datetime as dt
import fiona
from shapely.geometry import shape, Point, LinearRing
import multiprocessing
from itertools import repeat
import matplotlib.pyplot as plt
import numpy as np

global_confirmed_path = "../outbreak_db/COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
global_dead_path = "../outbreak_db/COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"

global_confirmed= pd.read_csv(global_confirmed_path)
global_dead= pd.read_csv(global_dead_path)

#######################
# Parse daily reports #
#######################

daily_reports_path = "../outbreak_db/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/"

def read_daily_report(path):
    df = pd.read_csv(path)
    df.columns = [i.replace("/", "_").replace(" ", "_").strip() for i in df.columns]
    df.columns = [i if i not in ["Lat", "Latitude"] else "Lat" for i in df.columns]
    df.columns = [i if i not in ["Long_", "Longitude"] else "Long"for i in df.columns]
    df["date"] = dt.strptime(os.path.basename(path)[:-4], "%m-%d-%Y")
    if "Lat" in df.columns:
        df["Lat"] = df["Lat"].apply(lambda x: np.nan if x == 0 else x)
        df["Long"] = df["Long"].apply(lambda x: np.nan if x == 0 else x)
    df = df.apply(lambda x: x.str.strip() if x.dtype == 'O' else x)
    return df

daily_reports = [read_daily_report(os.path.join(daily_reports_path, i)) for i in os.listdir(daily_reports_path) if i[-4:] == ".csv"]

daily_df = pd.concat(daily_reports, ignore_index = True)

# Add lat lng from countries already set
def add_lat_long(daily_df, key):
    mean_lat_long = daily_df[~daily_df["Lat"].isna() & ~daily_df["Long"].isna()].groupby(key).mean()
    for ind, row in daily_df.iterrows():
        if pd.isna(row[key]) or row[key] == None:
            continue
        if not pd.isna(row["Lat"]) and not pd.isna(row["Long"]):
            continue
        if row[key] not in mean_lat_long.index:
            print("Lat, Long not found for {}: {}".format(key, row[key]))
            continue
        daily_df.loc[ind, "Lat"] = mean_lat_long.loc[row[key]]["Lat"]
        daily_df.loc[ind, "Long"] = mean_lat_long.loc[row[key]]["Long"]

# First get lat_long long for state and then for country
add_lat_long(daily_df, "Province_State")
add_lat_long(daily_df, "Country_Region")

daily_df = daily_df[~daily_df["Lat"].isna()]

#####################
# Compute geo joins #
#####################

# Return centroid of polygon or largest polygon in set
def get_centroid(geom):
    p = []
    if geom["type"] == "MultiPolygon":
        polys = shape(geom)
        max_area = -1
        max_poly = None
        for poly in polys:
            if poly.area > max_area:
                max_poly = poly
                max_area = poly.area
        p = max_poly.centroid.xy
    else:
        p = shape(geom).centroid.xy
    return p[0][0], p[1][0]

def check_point_in_polygon(geom, lat ,lng):
    p = Point(lng, lat)
    if p.within(shape(geom)):
        return True
    return False

def get_distance_from_polygon(poly, lat, lng):
    p = Point(lng, lat)
    pol_ext = LinearRing(poly.exterior.coords)
    d = pol_ext.project(p)
    pp = pol_ext.interpolate(d)
    closest_point = [pp.coords.xy[0][0], pp.coords.xy[1][0]]
    dist = ((lng - closest_point[0]) ** 2 + (lat - closest_point[1])**2) ** 0.5
    return dist

# Returns feat that contains point or closest feat
def get_closest_polygon(coords, shp):
    lat, lng = coords
    closest_feat = None
    min_dist = float("Inf")
    for feat in shp:
        if check_point_in_polygon(feat["geometry"], lat, lng):
            return feat
    for feat in shp:
        if feat["geometry"]["type"] == "MultiPolygon":
            dists = []
            polys = shape(feat["geometry"])
            for poly in polys:
                d = get_distance_from_polygon(poly, lat, lng)
                dists.append(d)
            dist = min(dists)
        else:
            dist = get_distance_from_polygon(shape(feat["geometry"]), lat, lng)
        if dist < min_dist:
            closest_feat = feat
            min_dist = dist
    return closest_feat

# Read shapefiles
admn0_path = os.path.join("./data","ne_10m_admin_0_countries.shp")
admn0_shp = fiona.open(admn0_path)
admn1_path = os.path.join("./data","ne_10m_admin_1_states_provinces.shp")
admn1_shp = fiona.open(admn1_path)
admn2_path = os.path.join("./data","tl_2019_us_county.shp")
usa_admn2_shp = fiona.open(admn2_path)


state_feats = {}
country_feats = {}
usa_admn2_feats = {}

with multiprocessing.Pool(processes = 8) as pool:
    lat_lng = [i[0] for i in daily_df.groupby(["Lat", "Long"])]
    feats = pool.starmap(get_closest_polygon, zip(lat_lng, repeat(list(admn1_shp))))
    state_feats = dict(zip(lat_lng, feats))
    feats = pool.starmap(get_closest_polygon, zip(lat_lng, repeat(list(admn0_shp))))
    country_feats = dict(zip(lat_lng, feats))
    # Get US admin2
    lat_lng = [i[0] for i in daily_df[(daily_df["Country_Region"] == "US") & ~daily_df["Province_State"].isna()].groupby(["Lat", "Long"])]
    feats = pool.starmap(get_closest_polygon, zip(lat_lng, repeat(list(usa_admn2_shp))))
    usa_admn2_feats = dict(zip(lat_lng, feats))
    pool.close()
    pool.join()

for ind, row in daily_df.iterrows():
    country_feat = country_feats[(row["Lat"], row["Long"])]
    state_feat = state_feats[(row["Lat"], row["Long"])]
    daily_df.loc[ind, "computed_country_name"] = country_feat["properties"]["NAME"]
    daily_df.loc[ind, "computed_country_iso3"] = country_feat["properties"]["ADM0_A3"]
    if not pd.isna(row["Province_State"]):
        daily_df.loc[ind, "computed_state_name"] = state_feat["properties"]["name"]
        daily_df.loc[ind, "computed_state_iso3"] = state_feat["properties"]["iso_3166_2"]
    if not pd.isna(row["Admin2"]):
        county_feat = usa_admn2_feats[(row["Lat"], row["Long"])]
        daily_df.loc[ind, "computed_county_name"] = county_feat["properties"]["NAME"]
        daily_df.loc[ind, "computed_county_iso3"] = county_feat["properties"]["STATEFP"] + county_feat["properties"]["COUNTYFP"]

# Write admin_level
def get_admin_level(row):
    if not pd.isna(row["Admin2"]):
        return 2
    elif not pd.isna(row["Province_State"]):
        return 1
    return 0

daily_df["admin_level"] = daily_df.apply(get_admin_level, axis = 1)

country_sum = daily_df.groupby(["computed_country_iso3", "date"]).sum()
state_sum = daily_df.groupby(["computed_state_name", "date"]).sum()
county_sum = daily_df.groupby(["computed_county_name", "date"]).sum()

# Plot for country
country_sum.loc["USA"]

# daily_df.to_csv("./data/summed_daily_reports.csv")
