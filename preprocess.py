import pandas as pd
import os
from datetime import datetime as dt
from datetime import timedelta
import fiona
from shapely.geometry import shape, Point, LinearRing
import multiprocessing
from itertools import repeat
import matplotlib.pyplot as plt
import numpy as np
import json

# Read shapefiles
admn0_path = os.path.join("./data","ne_10m_admin_0_countries.shp")
admn0_shp = fiona.open(admn0_path)
admn1_path = os.path.join("./data","ne_10m_admin_1_states_provinces.shp")
admn1_shp = fiona.open(admn1_path)
admn2_path = os.path.join("./data","tl_2019_us_county.shp")
usa_admn2_shp = fiona.open(admn2_path)

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

# Correct wrong lat_lng longs. Check if name matches shapefile nad populate list below.
wrong_lat_long = ["Belize", "Malaysia"]
for cntry in wrong_lat_long:
    # Check if only country_feat has admin1 or admin2.
    n_admin_lower = daily_df[daily_df["Country_Region"] == cntry][["Province_State"]].dropna().shape[0] + daily_df[daily_df["Country_Region"] == cntry][["Admin2"]].dropna().shape[0]
    if n_admin_lower > 0:
        print("{} has admin 1 or 2. Verify if centroid of country is correct lat long!")
    feat = [i for i in admn0_shp if i["properties"]["NAME"] == cntry][0]
    centroid = get_centroid(feat["geometry"])
    daily_df.loc[daily_df["Country_Region"] == cntry, "Long"] = centroid[0]
    daily_df.loc[daily_df["Country_Region"] == cntry, "Lat"] = centroid[1]

# Remove rows with 0 confirmed, deaths and recoveries
daily_df = daily_df[daily_df.apply(lambda x: x["Confirmed"] + x["Recovered"] +x["Deaths"], axis = 1) != 0]

# Remove lat, long for cruises
for w in ["diamond princess", "grand princess", "cruise", "ship"]:
    daily_df.loc[daily_df["Country_Region"].apply(lambda x: False if pd.isna(x) else w in x.lower()), "Lat"] = np.nan
    daily_df.loc[daily_df["Province_State"].apply(lambda x: False if pd.isna(x) else w in x.lower()), "Lat"] = np.nan

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

# Remove nan lat long : cruises mostly
unknown = daily_df[daily_df["Lat"].isna()]
print("Unknown lat longs for {} rows".format(unknown.shape[0]))
unknown_confirmed = unknown.sort_values("date", ascending = False).groupby("Province_State").head(1)["Confirmed"].sum()
print("Unaccounted cases due to missing lat long: {}".format(unknown_confirmed))
print("\n".join(daily_df[daily_df["Lat"].isna()]["Country_Region"].unique()))
daily_df = daily_df[~daily_df["Lat"].isna()]

# Set lat long to the most frequent used values. To deal with cases like French Polynesia
for i, grp in daily_df[daily_df["Province_State"].isna() & daily_df["Admin2"].isna()].groupby("Country_Region"):
    daily_df.loc[(daily_df["Country_Region"] == i) & daily_df["Province_State"].isna() & daily_df["Admin2"].isna(), "Lat"] = grp["Lat"].dropna().mode().values[0]
    daily_df.loc[(daily_df["Country_Region"] == i) & daily_df["Province_State"].isna() & daily_df["Admin2"].isna(), "Long"] = grp["Long"].dropna().mode().values[0]

for i, grp in daily_df[daily_df["Admin2"].isna()].groupby(["Country_Region", "Province_State"]):
    daily_df.loc[(daily_df["Country_Region"] == i[0]) & (daily_df["Province_State"] == i[1]) & daily_df["Admin2"].isna(), "Lat"] = grp["Lat"].dropna().mode().values[0]
    daily_df.loc[(daily_df["Country_Region"] == i[0]) & (daily_df["Province_State"] == i[1]) & daily_df["Admin2"].isna(), "Long"] = grp["Long"].dropna().mode().values[0]

for i, grp in daily_df.groupby(["Country_Region", "Province_State", "Admin2"]):
    daily_df.loc[(daily_df["Country_Region"] == i[0]) & (daily_df["Province_State"] == i[1]) & (daily_df["Admin2"] == i[2]), "Lat"] = grp["Lat"].dropna().mode().values[0]
    daily_df.loc[(daily_df["Country_Region"] == i[0]) & (daily_df["Province_State"] == i[1]) & (daily_df["Admin2"] == i[2]), "Long"] = grp["Long"].dropna().mode().values[0]

# Round Lat Long to 4 decimal places
daily_df["Lat"] = daily_df["Lat"].round(4)
daily_df["Long"] = daily_df["Long"].round(4)

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

state_feats = {}
country_feats = {}
usa_admn2_feats = {}

print("Computing geo joins ... ")

with multiprocessing.Pool(processes = 8) as pool:
    # Country
    lat_lng = [i[0] for i in daily_df.groupby(["Lat", "Long"])]
    feats = pool.starmap(get_closest_polygon, zip(lat_lng, repeat(list(admn0_shp))))
    country_feats = dict(zip(lat_lng, feats))
    # State
    lat_lng = [i[0] for i in daily_df[~daily_df["Province_State"].isna()].groupby(["Lat", "Long"])]
    feats = pool.starmap(get_closest_polygon, zip(lat_lng, repeat(list(admn1_shp))))
    state_feats = dict(zip(lat_lng, feats))
    # Get US county
    lat_lng = [i[0] for i in daily_df[(daily_df["Country_Region"] == "US") & (~daily_df["Admin2"].isna() | (daily_df["Province_State"].str.contains(", ")) | (daily_df["Province_State"].str.lower().str.contains("county")))].groupby(["Lat", "Long"])]
    feats = pool.starmap(get_closest_polygon, zip(lat_lng, repeat(list(usa_admn2_shp))))
    usa_admn2_feats = dict(zip(lat_lng, feats))
    pool.close()
    pool.join()

print("Completed geo joins.")

print("Populating dataframe ... ")

for ind, row in daily_df.iterrows():
    county_feat = None
    state_feat = None
    country_feat = country_feats[(row["Lat"], row["Long"])]
    daily_df.loc[ind, "computed_country_name"] = country_feat["properties"]["NAME"]
    daily_df.loc[ind, "computed_country_iso3"] = country_feat["properties"]["ADM0_A3"]
    daily_df.loc[ind, "computed_country_pop"] = country_feat["properties"]["POP_EST"]
    daily_df.loc[ind, "computed_region_wb"] = country_feat["properties"]["REGION_WB"] if country_feat["properties"]["ADM0_A3"] != "CHN" else country_feat["properties"]["REGION_WB"] + ": China"
    admin_level = 0
    if not pd.isna(row["Province_State"]):
        state_feat = state_feats[(row["Lat"], row["Long"])]
        daily_df.loc[ind, "computed_state_name"] = state_feat["properties"]["name"]
        daily_df.loc[ind, "computed_state_iso3"] = state_feat["properties"]["iso_3166_2"]
        admin_level = 1
    if country_feat["properties"]["ADM0_A3"] == "USA" and (not pd.isna(row["Admin2"]) or (", " in row["Province_State"] or "county" in row["Province_State"].lower())):
        county_feat = usa_admn2_feats[(row["Lat"], row["Long"])]
        daily_df.loc[ind, "computed_county_name"] = county_feat["properties"]["NAME"]
        daily_df.loc[ind, "computed_county_iso3"] = county_feat["properties"]["STATEFP"] + county_feat["properties"]["COUNTYFP"]
        admin_level = 2
    daily_df.loc[ind, "computed_admin_level"] = admin_level
    daily_df.loc[ind, "JHU_Lat"] = daily_df.loc[ind, "Lat"]
    daily_df.loc[ind, "JHU_Long"] = daily_df.loc[ind, "Long"]
    if admin_level == 0:
        centroid_feat = country_feat
    elif admin_level == 1:
        centroid_feat = state_feat
    elif admin_level == 2:
        centroid_feat = county_feat
    centroid = get_centroid(centroid_feat["geometry"])
    daily_df.loc[ind, "Long"] = centroid[0]
    daily_df.loc[ind, "Lat"] = centroid[1]

print("Dataframe ready")

country_sum = daily_df.groupby(["computed_country_iso3", "date"]).sum()
state_sum = daily_df.groupby(["computed_state_name", "date"]).sum()
county_sum = daily_df.groupby(["computed_county_name", "date"]).sum()

# Plot for country
country_sum.loc["USA"]

daily_df.to_csv("./data/summed_daily_reports.csv")

############################
# Generate items and stats #
############################


def compute_stats(item, grp, grouped_sum, iso3, current_date):
    keys = ["Confirmed", "Recovered", "Deaths"]
    api_keys = ["confirmed", "recovered", "dead"]
    for key,api_key in zip(keys, api_keys):
        sorted_group_sum = grouped_sum.loc[iso3][key].sort_index()
        item[api_key] = grp[key].sum()
        item[api_key+"_currentCases"] = sorted_group_sum.iloc[-1]
        item[api_key+"_curentIncrease"] = sorted_group_sum.iloc[-1] - sorted_group_sum.iloc[-2] if len(sorted_group_sum) > 1 else sorted_group_sum.iloc[-1]
        item[api_key+"_currentPctIncrease"] = ((sorted_group_sum.iloc[-1] - sorted_group_sum.iloc[-2])/sorted_group_sum.iloc[-2]) * 100 if len(sorted_group_sum) > 1 and sorted_group_sum.iloc[-2] !=0 else ""
        item[api_key+"_currentToday"] = sorted_group_sum.index[-1].strftime("%Y-%m-%d")
        item[api_key+"_firstDate"] = sorted_group_sum[sorted_group_sum > 0].index[0].strftime("%Y-%m-%d") if sorted_group_sum[sorted_group_sum > 0].shape[0] > 0 else ""
        item[api_key+"_newToday"] = True if len(sorted_group_sum) > 1 and sorted_group_sum.iloc[-1] - sorted_group_sum.iloc[-2] > 0 else False
        item[api_key+"_numIncrease"] = sorted_group_sum[current_date] - sorted_group_sum[current_date - timedelta(days = 1)] if current_date - timedelta(days = 1) in sorted_group_sum.index else sorted_group_sum[current_date]

# Countries
items = []
grouped_sum = daily_df.groupby(["computed_country_iso3", "date"]).sum()
for ind, grp in daily_df.groupby(["computed_country_iso3", "date"]):
    item = {
        "date": ind[1].strftime("%Y-%m-%d"),
        "name": grp["computed_country_name"].iloc[0],
        "country_name": grp["computed_country_name"].iloc[0],
        "iso3": grp["computed_country_iso3"].iloc[0],
        "lat": grp["Lat"].iloc[0],
        "long": grp["Long"].iloc[0],
        "population": grp["computed_country_pop"].iloc[0],
        "region_wb": grp["computed_region_wb"].iloc[0],
        "location_id" : grp["computed_country_iso3"].iloc[0],
        "_id": grp["computed_country_iso3"].iloc[0] + "_" + ind[1].strftime("%Y-%m-%d"),
        "admin_level": 0
    }
    compute_stats(item, grp, grouped_sum, ind[0], ind[1])
    items.append(item)

# States
grouped_sum = daily_df.groupby(["computed_state_iso3", "date"]).sum()
for ind, grp in daily_df.groupby(["computed_state_iso3", "date"]):
    item = {
        "date": ind[1].strftime("%Y-%m-%d"),
        "name": grp["computed_state_name"].iloc[0],
        "country_name": grp["computed_country_name"].iloc[0],
        "iso3": grp["computed_state_iso3"].iloc[0],
        "country_iso3": grp["computed_country_iso3"].iloc[0],
        "lat": grp["Lat"].iloc[0],
        "long": grp["Long"].iloc[0],
        "country_population": grp["computed_country_pop"].iloc[0],
        "country_region_wb": grp["computed_region_wb"].iloc[0],
        "location_id" : grp["computed_country_iso3"].iloc[0] +"_" + grp["computed_state_iso3"].iloc[0],
        "_id": grp["computed_country_iso3"].iloc[0] +"_" + grp["computed_state_iso3"].iloc[0] + "_" + ind[1].strftime("%Y-%m-%d"),
        "admin_level": 1
    }
    # Compute case stats
    compute_stats(item, grp, grouped_sum, ind[0], ind[1])
    items.append(item)

# Counties
grouped_sum = daily_df.groupby(["computed_county_iso3", "date"]).sum()
for ind, grp in daily_df.groupby(["computed_county_iso3", "date"]):
    item = {
        "date": ind[1].strftime("%Y-%m-%d"),
        "name": grp["computed_county_name"].iloc[0],
        "iso3": grp["computed_county_iso3"].iloc[0],
        "state_name": grp["computed_state_name"].iloc[0],
        "country_name": grp["computed_country_name"].iloc[0],
        "state_iso3": grp["computed_state_iso3"].iloc[0],
        "country_iso3": grp["computed_country_iso3"].iloc[0],
        "lat": grp["Lat"].iloc[0],
        "long": grp["Long"].iloc[0],
        "country_population": grp["computed_country_pop"].iloc[0],
        "country_region_wb": grp["computed_region_wb"].iloc[0],
        "location_id" : grp["computed_country_iso3"].iloc[0] +"_" + grp["computed_state_iso3"].iloc[0],
        "_id": grp["computed_country_iso3"].iloc[0] +"_" + grp["computed_state_iso3"].iloc[0] + "_" + ind[1].strftime("%Y-%m-%d"),
        "admin_level": 2
    }
    compute_stats(item, grp, grouped_sum, ind[0], ind[1])
    items.append(item)

with open("./data/biothings_items.json", "w") as fout:
    json.dump(items, fout)
    fout.close()
