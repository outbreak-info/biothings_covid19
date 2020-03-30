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
import re

# Read shapefiles
admn0_path = os.path.join("./data","ne_10m_admin_0_countries.shp")
admn0_shp = fiona.open(admn0_path)
admn1_path = os.path.join("./data","ne_10m_admin_1_states_provinces.shp")
admn1_shp = fiona.open(admn1_path)
admn2_path = os.path.join("./data","tl_2019_us_county.shp")
usa_admn2_shp = fiona.open(admn2_path)

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
    daily_df.loc[daily_df["Country_Region"].str.lower().str.contains(w, na= False), "Lat"] = 91  # Add +91 for cruises
    daily_df.loc[daily_df["Country_Region"].str.lower().str.contains(w, na= False), "Long"] = 181
    daily_df.loc[daily_df["Province_State"].str.lower().str.contains(w, na= False), "Lat"] = 91  # Add +91 for cruises
    daily_df.loc[daily_df["Province_State"].str.lower().str.contains(w, na= False), "Long"] = 181

# Add lat lng from countries already set
def add_lat_long(daily_df, key, key_null = None):
    mean_lat_long = None
    if isinstance(key_null, list):
        mean_lat_long = daily_df[~daily_df["Lat"].isna() & ~daily_df["Long"].isna() & daily_df[key_null[0]].isna() & daily_df[key_null[1]].isna()].groupby(key).mean()
    elif key_null == None:
        mean_lat_long = daily_df[~daily_df["Lat"].isna() & ~daily_df["Long"].isna()].groupby(key).mean()
    else:
        mean_lat_long = daily_df[~daily_df["Lat"].isna() & ~daily_df["Long"].isna() & daily_df[key_null].isna()].groupby(key).mean()
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
add_lat_long(daily_df, "Province_State", "Admin2")
add_lat_long(daily_df, "Admin2")
add_lat_long(daily_df, "Country_Region", ["Province_State", "Admin2"])

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
daily_df["Lat"] = daily_df["Lat"].round(6)
daily_df["Long"] = daily_df["Long"].round(6)

#####################
# Compute geo joins #
#####################

state_feats = {}
country_feats = {}
usa_admn2_feats = {}

print("Computing geo joins ... ")

with multiprocessing.Pool(processes = 40) as pool:
    # Country
    lat_lng = [i[0] for i in daily_df.groupby(["Lat", "Long"])]
    feats = pool.starmap(get_closest_polygon, zip(lat_lng, repeat(list(admn0_shp))))
    country_feats = dict(zip(lat_lng, feats))
    # State
    lat_lng = [i[0] for i in daily_df[~daily_df["Province_State"].isna()].groupby(["Lat", "Long"])]
    feats = pool.starmap(get_closest_polygon, zip(lat_lng, repeat(list(admn1_shp))))
    state_feats = dict(zip(lat_lng, feats))
    # Get US county
    lat_lng = [i[0] for i in daily_df[(daily_df["Country_Region"] == "US") & ((~daily_df["Admin2"].isna() & ~daily_df["Admin2"].str.lower().str.contains("unassigned", na=False)) | (~daily_df["Province_State"].str.lower().str.contains("unassigned", na=False) & (daily_df["Province_State"].str.contains(", ")) | (daily_df["Province_State"].str.lower().str.contains("county"))))].groupby(["Lat", "Long"])]
    feats = pool.starmap(get_closest_polygon, zip(lat_lng, repeat(list(usa_admn2_shp))))
    usa_admn2_feats = dict(zip(lat_lng, feats))
    pool.close()
    pool.join()

print("Completed geo joins.")

print("Populating dataframe ... ")

def get_cruise_ship_name(val):  # Supply "Country_Region" + " " + "Province_State"
    res = re.search("[a-z]+ princess", val.lower())
    if res == None:
        return "Diamond Princess"
    return " ".join([i.capitalize() for i in res.group(0).split(" ")])

cruises_capacity = {
    "Diamond Princess": 3700,
    "Grand Princess": 3533
}

for ind, row in daily_df.iterrows():
    if row["Lat"] == 91 and row["Long"] == 181:  # Cruises: wb_region: Cruises, admin0: Cruises, admin1: Diamond/Grand/princess
        region_name = "Cruises"
        state_name = get_cruise_ship_name(str(row["Country_Region"]) + " " + str(row["Province_State"]))
        daily_df[ind, "name"] = state_name
        daily_df.loc[ind, "computed_country_name"] = region_name
        daily_df.loc[ind, "computed_country_pop"] = cruises_capacity[state_name]
        daily_df.loc[ind, "computed_country_iso3"] = region_name.lower()
        daily_df.loc[ind, "computed_state_name"] = state_name
        daily_df.loc[ind, "computed_state_iso3"] = state_name.lower().replace(" ","_")
        daily_df.loc[ind, "computed_region_wb"] = region_name
        daily_df.loc[ind, "Province_State"] = state_name
        daily_df.loc[ind, "Country_Region"] = region_name
        daily_df.loc[ind, "computed_country_lat"] = 91
        daily_df.loc[ind, "computed_country_long"] = 181
        daily_df.loc[ind, "computed_state_lat"] = 91
        daily_df.loc[ind, "computed_state_long"] = 181
        continue
    country_feat = country_feats[(row["Lat"], row["Long"])]
    daily_df.loc[ind, "computed_country_name"] = country_feat["properties"]["NAME"]
    daily_df.loc[ind, "computed_country_iso3"] = country_feat["properties"]["ADM0_A3"]
    daily_df.loc[ind, "computed_country_pop"] = country_feat["properties"]["POP_EST"]
    daily_df.loc[ind, "computed_region_wb"] = country_feat["properties"]["REGION_WB"] if country_feat["properties"]["ADM0_A3"] != "CHN" else country_feat["properties"]["REGION_WB"] + ": China"
    centroid = get_centroid(country_feat["geometry"])
    daily_df.loc[ind, "computed_country_long"] = centroid[0]
    daily_df.loc[ind, "computed_country_lat"] = centroid[1]
    if not pd.isna(row["Province_State"]):
        state_feat = state_feats[(row["Lat"], row["Long"])]
        daily_df.loc[ind, "computed_state_name"] = state_feat["properties"]["name"]
        daily_df.loc[ind, "computed_state_iso3"] = state_feat["properties"]["iso_3166_2"]
        centroid = get_centroid(state_feat["geometry"])
        daily_df.loc[ind, "computed_state_long"] = centroid[0]
        daily_df.loc[ind, "computed_state_lat"] = centroid[1]
    if country_feat["properties"]["ADM0_A3"] == "USA" and ((not pd.isna(row["Admin2"]) and not "unassigned" in row["Admin2"].lower()) or (not "unassigned"in row["Province_State"].lower() and (", " in row["Province_State"] or "county" in row["Province_State"].lower()))):
        county_feat = usa_admn2_feats[(row["Lat"], row["Long"])]
        daily_df.loc[ind, "computed_county_name"] = county_feat["properties"]["NAMELSAD"]
        daily_df.loc[ind, "computed_county_iso3"] = county_feat["properties"]["STATEFP"] + county_feat["properties"]["COUNTYFP"]
        centroid = get_centroid(county_feat["geometry"])
        daily_df.loc[ind, "computed_county_long"] = centroid[0]
        daily_df.loc[ind, "computed_county_lat"] = centroid[1]
    daily_df.loc[ind, "JHU_Lat"] = daily_df.loc[ind, "Lat"]
    daily_df.loc[ind, "JHU_Long"] = daily_df.loc[ind, "Long"]

print("Dataframe ready")

gdp_data_df = pd.read_csv(os.path.join("./data/API_NY.GDP.PCAP.CD_DS2_en_csv_v2_887243.csv"),header = 2)

#creates a dataframe that has Country_name, country_code, lastest_year_gdp_is_available, country_gdp(wrt to that year)
new_rows=[]
for i,row in gdp_data_df.iterrows():
  year = "2018"
  while year != "1960":
    if pd.notnull(row[year]):
      new_rows.append([row["Country Code"],year,row[year]])
      break
    else:
      year = str(int(year)-1)

gdp_trim_df = pd.DataFrame(new_rows, columns=["computed_country_iso3","year","country_gdp"])

del gdp_data_df

daily_df = pd.merge(daily_df,gdp_trim_df,on="computed_country_iso3")

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
    first_date = {}
    for key,api_key in zip(keys, api_keys):
        sorted_group_sum = grouped_sum.loc[iso3][key].sort_index()
        item[api_key] = grp[key].sum()
        item[api_key+"_currentCases"] = sorted_group_sum.iloc[-1]
        item[api_key+"_currentIncrease"] = sorted_group_sum.iloc[-1] - sorted_group_sum.iloc[-2] if len(sorted_group_sum) > 1 else sorted_group_sum.iloc[-1]
        if len(sorted_group_sum) > 1 and sorted_group_sum.iloc[-2] !=0:
            item[api_key+"_currentPctIncrease"] = ((sorted_group_sum.iloc[-1] - sorted_group_sum.iloc[-2])/sorted_group_sum.iloc[-2])
        item[api_key+"_currentToday"] = sorted_group_sum.index[-1].strftime("%Y-%m-%d")
        first_date[key] = sorted_group_sum[sorted_group_sum > 0].index[0] if sorted_group_sum[sorted_group_sum > 0].shape[0] > 0 else ""
        item[api_key+"_firstDate"] = first_date[key].strftime("%Y-%m-%d") if first_date[key] != "" else ""
        item[api_key+"_newToday"] = True if len(sorted_group_sum) > 1 and sorted_group_sum.iloc[-1] - sorted_group_sum.iloc[-2] > 0 else False
        item[api_key+"_numIncrease"] = sorted_group_sum[current_date] - sorted_group_sum[current_date - timedelta(days = 1)] if current_date - timedelta(days = 1) in sorted_group_sum.index else sorted_group_sum[current_date]
    if first_date["Confirmed"] != "" and first_date["Deaths"] != "":
        item["first_dead-first_confirmed"] = (first_date["Deaths"] - first_date["Confirmed"]).days

format_id = lambda x: x.replace(" ", "_").replace("&", "_")

# Countries
# Compute sub_national from latest dates for all countries
country_sub_national = daily_df.sort_values("date").groupby(["computed_country_iso3"]).apply(lambda x: len(x[x["date"] == x["date"].max()]["computed_state_iso3"].unique())).sort_values()
items = []
grouped_sum = daily_df.groupby(["computed_country_iso3", "date"]).sum()
for ind, grp in daily_df.sort_values("date").groupby(["computed_country_iso3", "date"]):
    item = {
        "date": ind[1].strftime("%Y-%m-%d"),
        "name": grp["computed_country_name"].iloc[0],
        "country_name": grp["computed_country_name"].iloc[0],
        "iso3": grp["computed_country_iso3"].iloc[0],
        "lat": grp["Lat"].iloc[0],
        "long": grp["Long"].iloc[0],
        "population": grp["computed_country_pop"].iloc[0],
        "wb_region": grp["computed_region_wb"].iloc[0],
        "location_id" : format_id(grp["computed_country_iso3"].iloc[0]),
        "_id": format_id(grp["computed_country_iso3"].iloc[0] + "_" + ind[1].strftime("%Y-%m-%d")),
        "admin_level": 0,
        "lat": grp["computed_country_lat"].iloc[0],
        "long": grp["computed_country_long"].iloc[0],
        "num_subnational": int(country_sub_national[ind[0]])  # For every date number of admin1 regions in country with reported cases.
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
        "wb_region": grp["computed_region_wb"].iloc[0],
        "location_id" : format_id(grp["computed_country_iso3"].iloc[0] +"_" + grp["computed_state_iso3"].iloc[0]),
        "_id": format_id(grp["computed_country_iso3"].iloc[0] +"_" + grp["computed_state_iso3"].iloc[0] + "_" + ind[1].strftime("%Y-%m-%d")),
        "admin_level": 1,
        "lat": grp["computed_state_lat"].iloc[0],
        "long": grp["computed_state_long"].iloc[0]
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
        "wb_region": grp["computed_region_wb"].iloc[0],
        "location_id" : format_id(grp["computed_country_iso3"].iloc[0] +"_" + grp["computed_state_iso3"].iloc[0] + "_" + grp["computed_county_iso3"].iloc[0]),
        "_id": format_id(grp["computed_country_iso3"].iloc[0] +"_" + grp["computed_state_iso3"].iloc[0] + "_" + grp["computed_county_iso3"].iloc[0] + "_" + ind[1].strftime("%Y-%m-%d")),
        "admin_level": 2,
        "lat": grp["computed_county_lat"].iloc[0],
        "long": grp["computed_county_long"].iloc[0]
    }
    compute_stats(item, grp, grouped_sum, ind[0], ind[1])
    items.append(item)

# wb_region
grouped_sum = daily_df.groupby(["computed_region_wb", "date"]).sum()
for ind, grp in daily_df.groupby(["computed_region_wb", "date"]):
    item = {
        "date": ind[1].strftime("%Y-%m-%d"),
        "name": grp["computed_region_wb"].iloc[0],
        "iso3": grp["computed_region_wb"].iloc[0],
        "wb_region": grp["computed_region_wb"].iloc[0],
        "location_id" : format_id(grp["computed_region_wb"].iloc[0]),
        "_id": format_id(grp["computed_region_wb"].iloc[0] + "_" + ind[1].strftime("%Y-%m-%d")),
        "admin_level": -1
    }
    compute_stats(item, grp, grouped_sum, ind[0], ind[1])
    items.append(item)

print("Wrote {} items to file".format(len(items)))
with open("./data/biothings_items.json", "w") as fout:
    json.dump(items, fout)
    fout.close()

