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
import requests
import copy
import configparser

# Get paths
config = configparser.ConfigParser()
config.read("config.ini")
# Shapefiles
admn0_path = config["shapefiles"]["admn0_path"]
admn1_path = config["shapefiles"]["admn1_path"]
admn2_path = config["shapefiles"]["admn2_path"]
usa_metro_path = config["shapefiles"]["usa_metro_path"]

# Data
census_regions_path = config["data"]["census_regions_path"]
gdp_path = config["data"]["gdp_path"]

# Repos
daily_reports_path = config["repos"]["daily_reports_path"]
nyt_county_path = config["repos"]["nyt_county_path"]
nyt_state_path = config["repos"]["nyt_state_path"]

# Output
export_df_path = config["output"]["export_df_path"]
out_json_path = config["output"]["out_json_path"]

# Processes
nprocess = int(config["process"]["nprocess"])

# Read shapefiles
admn0_shp = fiona.open(admn0_path)
admn1_shp = fiona.open(admn1_path)
usa_admn2_shp = fiona.open(admn2_path)
usa_metro_shp = fiona.open(usa_metro_path)

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

# Replace US counts with data from NYT
state_feats = {}
usa_admn2_feats = {}
nyt_county = pd.read_csv(nyt_county_path, dtype = {
    "fips": str
})
nyt_state = pd.read_csv(nyt_state_path, dtype = {
    "fips": str
})

# Map territory fips to adm0_a3 code
fips_iso3 = {
    "VIR": "52",
    "GUM": "66",
    "MNP": "69",
    "VIR": "78",
    "PRI": "72",
    "ASM": "60"
}

# Extract matching features from NE shapefiles
usa_admn1_shp = [i for i in admn1_shp if i["properties"]["adm0_a3"] in fips_iso3.keys() or i["properties"]["adm0_a3"] == "USA"]

us_state_feats = []
for fips in nyt_state["fips"].unique():
    feats = [i for i in usa_admn1_shp if (i["properties"]["fips"] != None and i["properties"]["fips"][2:] == fips) or (i["properties"]["adm0_a3"] != "USA" and fips_iso3[i["properties"]["adm0_a3"]] == fips)]
    if len(feats) == 0:
        print("NYT Data doesn't have matching for state with fips {}".format(fips))
        assert False, "FIPS for NYT data missing. Please add iso3 code to fips_iso3 dict on line 205"
    us_state_feats.append([fips, feats[0]])

us_state_feats = dict(us_state_feats)

def get_us_admn2_feat(fips, shp):
    feats = [i for i in shp if str(i["properties"]["STATEFP"]) + str(i["properties"]["COUNTYFP"]) == fips]
    if len(feats) == 0:
        print("NYT Data doesn't have matching for county with fips {}".format(fips))
        return None
    return feats[0]

county_fips_list = nyt_county["fips"].dropna().unique().tolist()
with multiprocessing.Pool(processes = nprocess) as pool:
    usa_admn2_feats = pool.starmap(get_us_admn2_feat, zip(county_fips_list, repeat(list(usa_admn2_shp))))
    pool.close()
    pool.join()

usa_admn2_feats = dict(zip(county_fips_list, usa_admn2_feats))

# Add lat long from extracted features
# For NYC and KC add admin_level = 1.7 and a new FIPS code called 
# Drop county level data for non mainland US counties except for New York city and Kansas city
nyt_county = nyt_county[~nyt_county["fips"].isna() | (nyt_county["county"].isin(["New York City", "Kansas City"]))]
for ind, row in nyt_county.iterrows():
    if pd.isna(row["fips"]):
        if row["county"] == "New York City" and row["state"] == "New York":
            nyt_county.loc[ind, "Lat"] = 40.730610
            nyt_county.loc[ind, "Long"] = 73.935242
            continue
        if row["county"] == "Kansas City" and row["state"] == "Missouri":
            nyt_county.loc[ind, "Lat"] = 39.09973
            nyt_county.loc[ind, "Long"] = -94.57857
            continue
        continue
    nyt_county.loc[ind, "Lat"], nyt_county.loc[ind, "Long"] = get_centroid(usa_admn2_feats[row["fips"]]["geometry"])

nyt_county = nyt_county.rename(columns={
    "state": "Province_State",
    "county": "Admin2",
    "cases": "Confirmed",
    "deaths": "Deaths"
})
nyt_county.loc[:,"Country_Region"] = "USA_NYT"  # To differentiate between cruises with country_region US

# Add state data. If county counts is less than total state counts then add difference as Admin2 "Unassigned" and Admin1 as "State"
county_group = nyt_county.groupby(["Province_State", "date"]).sum()
for ind, row in nyt_state.iterrows():
    nyt_state.loc[ind, "Lat"], nyt_state.loc[ind, "Long"] = get_centroid(us_state_feats[row["fips"]]["geometry"])
    if row["state"] not in county_group.index.get_level_values(0):  # State not found in county
        continue
    if row["date"] in county_group.loc[row["state"],:].index:
        nyt_state.loc[ind, "cases"] = row["cases"] - county_group.loc[(row["state"], row["date"]), "Confirmed"]
        nyt_state.loc[ind, "deaths"] = row["deaths"] - county_group.loc[(row["state"], row["date"]), "Deaths"]

nyt_state["cases"] = nyt_state["cases"].apply(lambda x: x if x>0 else 0)
nyt_state["death"] = nyt_state["deaths"].apply(lambda x: x if x>0 else 0)
nyt_state = nyt_state[~((nyt_state["cases"] == 0) & (nyt_state["deaths"] == 0))]
nyt_state.loc[:,"Admin2"] = "Unassigned"
nyt_state.loc[:,"Country_Region"] = "USA_NYT"
nyt_state = nyt_state.rename(columns = {
    "state": "Province_State",
    "cases": "Confirmed",
    "deaths": "Deaths"
})

# Covnert date to datetime object
nyt_county["date"] = nyt_county["date"].apply(lambda x: dt.strptime(x,  "%Y-%m-%d"))
nyt_state["date"] = nyt_state["date"].apply(lambda x: dt.strptime(x,  "%Y-%m-%d"))

# Remove US data from daily_df except for cruise ships by checking lat == 91 and long == 181
daily_df = daily_df[(daily_df["Country_Region"] != "US") | ((daily_df["Lat"] == 91) & (daily_df["Long"] != 181))]
daily_df = pd.concat([daily_df, nyt_state, nyt_county], ignore_index = True)

# Add metro politan CBSA codes
metro = pd.read_csv(census_regions_path, skiprows = 2, dtype = {
    "FIPS State Code": str,
    "FIPS County Code": str,
    "CBSA Code": str
})
metro = metro[~metro["FIPS County Code"].isna()]  # Gets rid of bottom 3 rows in file
metro["fips"] = metro["FIPS State Code"] + metro["FIPS County Code"].apply(lambda x: x.zfill(3))
daily_df = pd.merge(daily_df, metro, on = "fips", how="left")

# Extract metropolitan area features
def get_metro_feat(cbsa, shp):
    feats = [i for i in shp if i["properties"]["CBSAFP"] == cbsa]
    if len(feats) == 0:
        print("Couldn't find metro feature for CBSA code: {}".format(cbsa))
        return None
    return feats[0]

metro_feats = []
metro_list = daily_df["CBSA Code"].dropna().unique()
with multiprocessing.Pool(processes = nprocess) as pool:
    metro_feats = pool.starmap(get_metro_feat, zip(metro_list, repeat(list(usa_metro_shp))))
    pool.close()
    pool.join()

metro_feats = dict(zip(metro_list, metro_feats))

# Add testing data
def get_us_testing_data(admn1_shp):
    testing_api_url = "https://covidtracking.com/api/states/daily"
    us_states = [i for i in admn1_shp if i["properties"]["adm0_a3"] == "USA"]
    resp = requests.get(testing_api_url)
    us_testing = {}
    if resp.status_code != 200:
        print("US testing data could not be obtained from https://covidtracking.com/api/states/daily.")
        return us_testing
    testing = resp.json()
    for feat in us_states:
        state_tests = [i for i in testing if i["state"] == feat["properties"]["iso_3166_2"][-2:]]
        if len(state_tests) > 0:
            for state_test in state_tests:
                d = {}
                current_date = None
                for k,v in state_test.items():
                    if v == None or k == "state":
                        continue
                    if k in ["lastUpdateEt", "checkTimeEt"] and type(v) != int and "/" in v:
                        v = dt.strptime("2020/"+v, "%Y/%m/%d %H:%M") if len(v.split("/")) == 2 else dt.strptime(v, "%m/%d/%Y %H:%M") # Deals with 1900 being default year for Feb 29th without year
                        v = v.strftime("2020-%m-%d %H:%M") 
                    if k  == "date":
                        current_date = dt.strptime(str(v), "%Y%m%d").strftime("%Y-%m-%d")
                    d[k] = v
                us_testing[current_date + "_" + feat["properties"]["iso_3166_2"]] = copy.deepcopy(d)
        else:
            logging.warning("No testing data for US State: {}".format(feat["properties"]["iso_3166_2"]))
    return us_testing

us_testing = get_us_testing_data(admn1_shp)

#################################################
# Compute geo joins for countries other than US #
#################################################

state_feats = {}
country_feats = {}

print("Computing geo joins ... ")

with multiprocessing.Pool(processes = nprocess) as pool:
    # Country
    lat_lng = [i[0] for i in daily_df[daily_df["Country_Region"]!="USA_NYT"].groupby(["Lat", "Long"])]
    feats = pool.starmap(get_closest_polygon, zip(lat_lng, repeat(list(admn0_shp))))
    country_feats = dict(zip(lat_lng, feats))
    # State
    lat_lng = [i[0] for i in daily_df[daily_df["Country_Region"]!="USA_NYT"][~daily_df["Province_State"].isna()].groupby(["Lat", "Long"])]
    feats = pool.starmap(get_closest_polygon, zip(lat_lng, repeat(list(admn1_shp))))
    state_feats = dict(zip(lat_lng, feats))
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

usa_country_feat = [i for i in admn0_shp if i["properties"]["ADM0_A3"]=="USA"][0]

daily_df.columns = daily_df.columns.str.replace(" ", "_").str.replace("/", "")

# Cruises: wb_region: Cruises, admin0: Cruises, admin1: Diamond/Grand/princess
print("Populating cruises ... ")
region_name = "Cruises"
state_name = daily_df.loc[(daily_df["Lat"] == 91) & (daily_df["Long"] == 181)].apply(lambda x: get_cruise_ship_name(str(x["Country_Region"]) + " " + str(x["Province_State"])), axis = 1)
sub_df = daily_df.loc[(daily_df["Lat"] == 91) & (daily_df["Long"] == 181)]
daily_df.loc[sub_df.index, "name"] = state_name
daily_df.loc[sub_df.index, "computed_country_name"] = region_name
daily_df.loc[sub_df.index, "computed_country_pop"] = [cruises_capacity[i] for i in state_name]
daily_df.loc[sub_df.index, "computed_country_iso3"] = region_name.lower()
daily_df.loc[sub_df.index, "computed_state_name"] = state_name
daily_df.loc[sub_df.index, "computed_state_iso3"] = state_name.str.lower().str.replace(" ","_")
daily_df.loc[sub_df.index, "computed_region_wb"] = region_name
daily_df.loc[sub_df.index, "Province_State"] = state_name
daily_df.loc[sub_df.index, "Country_Region"] = region_name
daily_df.loc[sub_df.index, "computed_country_lat"] = 91
daily_df.loc[sub_df.index, "computed_country_long"] = 181
daily_df.loc[sub_df.index, "computed_state_lat"] = 91
daily_df.loc[sub_df.index, "computed_state_long"] = 181

# Country
print("Populating countries ... ")
country_attr = {
    "computed_country_name": "NAME",
    "computed_country_pop": "POP_EST",
    "computed_country_iso3": "ADM0_A3"
}
for k,v in country_attr.items():
    daily_df.loc[:, k] = daily_df.apply(lambda x: usa_country_feat["properties"][v] if x["Country_Region"] == "USA_NYT" else country_feats[(x["Lat"], x["Long"])]["properties"][v], axis =  1)

daily_df.loc[:, "computed_region_wb"] = daily_df.apply(lambda x: country_feats[(x["Lat"], x["Long"])]["properties"]["REGION_WB"] + ": China" if x["computed_country_iso3"] == "CHN" else usa_country_feat["properties"]["REGION_WB"] if x["Country_Region"] == "USA_NYT" else country_feats[(x["Lat"], x["Long"])]["properties"]["REGION_WB"], axis = 1)

centroids = daily_df.apply(lambda x: get_centroid(usa_country_feat["geometry"]) if x["Country_Region"] == "USA_NYT" else get_centroid(country_feats[(x["Lat"], x["Long"])]["geometry"]), axis = 1)
daily_df.loc[:, "computed_country_long"] = [i[0] for i in centroids]
daily_df.loc[:, "computed_country_lat"] = [i[1] for i in centroids]

# US States set lat. For New York City and Kansas City, lat_lng already set
print("Populating US States ... ")
us_states = daily_df.loc[~daily_df["Province_State"].isna() & (daily_df["Country_Region"] == "USA_NYT") & (~daily_df["Admin2"].isin(["New York City", "Kansas City"]))]
centroids = us_states["fips"].apply(lambda x: get_centroid(us_state_feats[x[:2]]["geometry"]))
daily_df.loc[us_states.index, "computed_state_long"] = [i[0] for i in centroids]
daily_df.loc[us_states.index, "computed_state_lat"] = [i[1] for i in centroids]
daily_df.loc[us_states.index, "computed_state_name"] = us_states["fips"].apply(lambda x: us_state_feats[x[:2]]["properties"]["name"])
daily_df.loc[us_states.index, "computed_state_iso3"] = us_states["fips"].apply(lambda x: us_state_feats[x[:2]]["properties"]["iso_3166_2"])
# Add testing data to states in US
us_states = daily_df.loc[~daily_df["Province_State"].isna() & (daily_df["Country_Region"] == "USA_NYT") & (~daily_df["Admin2"].isin(["New York City", "Kansas City"]))]
check_testing_states = us_states.apply(lambda x: (x["date"].strftime("%Y-%m-%d") + "_" + x["computed_state_iso3"]) in us_testing, axis = 1)
us_testing_states = us_states.loc[check_testing_states]

# Iterate and add all testing keys
testing_keys = list(us_testing.values())[0].keys()
for k in testing_keys:
    if k in ["date", "hash"]:
        continue
    daily_df.loc[us_testing_states.index, "testing_"+k] = us_testing_states.apply(lambda x: us_testing[(x["date"].strftime("%Y-%m-%d") + "_" + x["computed_state_iso3"])][k] if k in us_testing[(x["date"].strftime("%Y-%m-%d") + "_" + x["computed_state_iso3"])] else np.nan, axis = 1)

# Non US states set lat_lng
print("Populating Admin1 regions outside US ... ")
non_us_states = daily_df.loc[~daily_df["Province_State"].isna() & (daily_df["Country_Region"] != "USA_NYT") & (~daily_df["Admin2"].isin(["New York City", "Kansas City"]))]
centroids = non_us_states.apply(lambda x: get_centroid(state_feats[(x["Lat"], x["Long"])]["geometry"]), axis = 1)
daily_df.loc[non_us_states.index, "computed_state_long"] = [i[0] for i in centroids]
daily_df.loc[non_us_states.index, "computed_state_lat"] = [i[1] for i in centroids]
daily_df.loc[non_us_states.index, "computed_state_name"] = non_us_states.apply(lambda x: state_feats[(x["Lat"], x["Long"])]["properties"]["name"], axis = 1)
daily_df.loc[non_us_states.index, "computed_state_iso3"] = non_us_states.apply(lambda x: state_feats[(x["Lat"], x["Long"])]["properties"]["iso_3166_2"], axis = 1)

# Admin2
print("Populating US counties ... ")
us_county_df = daily_df[~daily_df["Province_State"].isna() & (daily_df["Country_Region"] == "USA_NYT") & ~(daily_df["Admin2"] == "Unassigned") & ~(pd.isna(daily_df["Admin2"])) & ~(daily_df["Admin2"].isin(["New York City", "Kansas City"]))]
daily_df.loc[us_county_df.index, "computed_county_name"] = us_county_df["fips"].apply(lambda x: usa_admn2_feats[x]["properties"]["NAMELSAD"])
daily_df.loc[us_county_df.index, "computed_county_iso3"] = us_county_df["fips"].apply(lambda x: usa_admn2_feats[x]["properties"]["STATEFP"] + usa_admn2_feats[x]["properties"]["COUNTYFP"])
centroids = us_county_df["fips"].apply(lambda x: get_centroid(usa_admn2_feats[x]["geometry"]))
daily_df.loc[us_county_df.index, "computed_county_long"] = [i[0] for i in centroids]
daily_df.loc[us_county_df.index, "computed_county_lat"] = [i[1] for i in centroids]

# Add metropolitan areas
print("Populating metropolitan areas ...")
us_metro_df = us_county_df[~us_county_df["CBSA_Code"].isna()]
daily_df.loc[us_metro_df.index, "computed_metro_cbsa"] = us_metro_df["CBSA_Code"].apply(lambda x: metro_feats[x]["properties"]["CBSAFP"] if metro_feats[x] != None else None)
daily_df.loc[us_metro_df.index, "computed_metro_name"] = us_metro_df["CBSA_Code"].apply(lambda x: metro_feats[x]["properties"]["NAME"] if metro_feats[x] != None else None)
centroids = us_metro_df["CBSA_Code"].apply(lambda x: get_centroid(metro_feats[x]["geometry"]) if metro_feats[x] != None else [None, None])
daily_df.loc[us_metro_df.index, "computed_metro_long"] = [i[0] for i in centroids]
daily_df.loc[us_metro_df.index, "computed_metro_lat"] = [i[1] for i in centroids]

# Add admin2 codes for cities: NYC and KC
print("Populating cities (NYC + KC)")
nyc_df = daily_df[~daily_df["Province_State"].isna() & (daily_df["Country_Region"] == "USA_NYT") & (daily_df["Admin2"] == "New York City")]
daily_df.loc[nyc_df.index, "computed_city_name"] = "New York City"
daily_df.loc[nyc_df.index, "computed_city_iso3"] = "US-NY_NYC"
daily_df.loc[nyc_df.index, "computed_city_lat"] = nyc_df["Lat"]
daily_df.loc[nyc_df.index, "computed_city_long"] = nyc_df["Long"]
metro_feat = metro_feats["35620"]
daily_df.loc[nyc_df.index, "computed_metro_cbsa"] = metro_feat["properties"]["CBSAFP"]
daily_df.loc[nyc_df.index, "computed_metro_name"] = metro_feat["properties"]["NAME"]
centroid = get_centroid(metro_feat["geometry"])
daily_df.loc[nyc_df.index, "computed_metro_long"] = centroid[0]
daily_df.loc[nyc_df.index, "computed_metro_lat"] = centroid[1]
# Add state for city_df records
ny_state_feature = [i for i in admn1_shp if i["properties"]["iso_3166_2"] == "US-NY"][0]
centroid = get_centroid(ny_state_feature["geometry"])
daily_df.loc[nyc_df.index, "computed_state_long"] = centroid[0]
daily_df.loc[nyc_df.index, "computed_state_lat"] = centroid[1]
daily_df.loc[nyc_df.index, "computed_state_iso3"] = "US-NY"
daily_df.loc[nyc_df.index, "computed_state_name"] = ny_state_feature["properties"]["name"]

kc_df = daily_df[~daily_df["Province_State"].isna() & (daily_df["Country_Region"] == "USA_NYT") & (daily_df["Admin2"] == "Kansas City")]
daily_df.loc[kc_df.index, "computed_city_name"] = "Kansas City"
daily_df.loc[kc_df.index, "computed_city_iso3"] = "US-MO_KC"
daily_df.loc[kc_df.index, "computed_city_lat"] = kc_df["Lat"]
daily_df.loc[kc_df.index, "computed_city_long"] = kc_df["Long"]
metro_feat = metro_feats["28140"]
daily_df.loc[kc_df.index, "computed_metro_cbsa"] = metro_feat["properties"]["CBSAFP"]
daily_df.loc[kc_df.index, "computed_metro_name"] = metro_feat["properties"]["NAME"]
centroid = get_centroid(metro_feat["geometry"])
daily_df.loc[kc_df.index, "computed_metro_long"] = centroid[0]
daily_df.loc[kc_df.index, "computed_metro_lat"] = centroid[1]
# Add state for city_df records
mo_state_feature = [i for i in admn1_shp if i["properties"]["iso_3166_2"] == "US-MO"][0]
centroid = get_centroid(mo_state_feature["geometry"])
daily_df.loc[kc_df.index, "computed_state_long"] = centroid[0]
daily_df.loc[kc_df.index, "computed_state_lat"] = centroid[1]
daily_df.loc[kc_df.index, "computed_state_iso3"] = "US-MO"
daily_df.loc[kc_df.index, "computed_state_name"] = mo_state_feature["properties"]["name"]

# Add GDP data
print("Adding GDP per capita for countries")
gdp_data_df = pd.read_csv(gdp_path,header = 2)

#Creates a dataframe that has Country_name, country_code, lastest_year_gdp_is_available, country_gdp(wrt to that year)
new_rows=[]
for i,row in gdp_data_df.iterrows():
  year = "2018"  #From which year does the gdp_per_capita check begin
  while year != "1960":
    if pd.notnull(row[year]):
      new_rows.append([row["Country Code"],year,row[year]])
      break
    else:
      year = str(int(year)-1)

gdp_trim_df = pd.DataFrame(new_rows, columns=["computed_country_iso3","gdp_update_year","country_gdp"])

daily_df = pd.merge(daily_df,gdp_trim_df,on="computed_country_iso3")


print("Dataframe ready")

# Export dataframe
daily_df.to_csv(export_df_path)

############################
# Generate items and stats #
############################

def compute_doubling_rate(cases):
    x = np.arange(len(cases))
    y = np.log(cases)
    m,b = np.polyfit(x, y, 1)
    m = np.round(m,3)
    if m <= 0:
        return np.nan
    dr = np.log(2)/m
    dr = np.round(dr, 3)
    return dr if not np.isposinf(dr) and not np.isneginf(dr) else np.nan

def compute_days_since(cases, ncases, current_date):
    if cases[cases >= ncases].shape[0] == 0:
        return None
    first_gte_ncases = cases[cases >= ncases].index[0]
    if cases[cases < ncases].shape[0] == 0:
        return None
    last_lt_ncases = cases[cases < ncases].index[-1]
    offset_cases = 1 - ((ncases - cases.loc[last_lt_ncases])/(cases.loc[first_gte_ncases] - cases.loc[last_lt_ncases]))
    days_since_ncases = (current_date - first_gte_ncases).days + offset_cases
    return np.round(days_since_ncases, 3)

def compute_stats(item, grp, grouped_sum, iso3, current_date):
    keys = ["Confirmed", "Recovered", "Deaths"]
    api_keys = ["confirmed", "recovered", "dead"]
    sorted_group_sum = grouped_sum.loc[iso3]["Confirmed"].sort_index()
    item["mostRecent"] = (current_date == sorted_group_sum.index[-1])
    first_date = {}
    compute_num_increase = lambda x: sorted_group_sum[x] - sorted_group_sum[x - timedelta(days = 1)] if x - timedelta(days = 1) in sorted_group_sum.index else sorted_group_sum[x]
    for key,api_key in zip(keys, api_keys):
        sorted_group_sum = grouped_sum.loc[iso3][key].sort_index()
        item[api_key] = grp[key].sum()
        # Rolling mean
        tmp_grp = sorted_group_sum.reset_index()
        rolling_average = tmp_grp[(tmp_grp["date"]<=(current_date + timedelta(days = 3))) & (tmp_grp["date"] >= current_date - timedelta(days = 3))]["date"].apply(compute_num_increase).mean()
        if current_date in sorted_group_sum.index and not np.isnan(rolling_average):
            item[api_key+"_rolling"] = rolling_average
        # Doubling rate
        val_dr = tmp_grp[(tmp_grp["date"]<= current_date) & (tmp_grp["date"] >= current_date - timedelta(days = 4))][key].tolist()
        val_dr = [i for i in val_dr if i > 0]
        dr = compute_doubling_rate(val_dr) if len(val_dr) > 1 else np.nan
        if current_date in sorted_group_sum.index and not np.isnan(dr):
            item[api_key+"_doublingRate"] = dr
        # item[api_key+"_currentCases"] = sorted_group_sum.iloc[-1]
        # item[api_key+"_currentIncrease"] = sorted_group_sum.iloc[-1] - sorted_group_sum.iloc[-2] if len(sorted_group_sum) > 1 else sorted_group_sum.iloc[-1]
        # if len(sorted_group_sum) > 1 and sorted_group_sum.iloc[-2] !=0:
        #     item[api_key+"_currentPctIncrease"] = ((sorted_group_sum.iloc[-1] - sorted_group_sum.iloc[-2])/sorted_group_sum.iloc[-2])
        # item[api_key+"_currentToday"] = sorted_group_sum.index[-1].strftime("%Y-%m-%d")
        first_date[key] = sorted_group_sum[sorted_group_sum > 0].index[0] if sorted_group_sum[sorted_group_sum > 0].shape[0] > 0 else ""
        item[api_key+"_firstDate"] = first_date[key].strftime("%Y-%m-%d") if first_date[key] != "" else ""
        item[api_key+"_newToday"] = True if len(sorted_group_sum) > 1 and sorted_group_sum.iloc[-1] - sorted_group_sum.iloc[-2] > 0 else False
        item[api_key+"_numIncrease"] = compute_num_increase(current_date)
        if current_date - timedelta(days = 1) in sorted_group_sum.index and sorted_group_sum[current_date - timedelta(days = 1)] > 0:
            item[api_key+"_pctIncrease"] = (sorted_group_sum[current_date] - sorted_group_sum[current_date - timedelta(days = 1)])/sorted_group_sum[current_date - timedelta(days = 1)]
    if first_date["Confirmed"] != "" and first_date["Deaths"] != "":
        item["first_dead-first_confirmed"] = (first_date["Deaths"] - first_date["Confirmed"]).days
    # daysSince100Cases
    confirmed_cases = grouped_sum.loc[iso3]["Confirmed"].sort_index()
    days_since_100_cases = compute_days_since(confirmed_cases, 100, current_date)
    if days_since_100_cases != None and days_since_100_cases >= 0:
        item["daysSince100Cases"] = days_since_100_cases
    # daysSince10Deaths
    deaths = grouped_sum.loc[iso3]["Deaths"].sort_index()
    days_since_10_deaths = compute_days_since(deaths, 10, current_date)
    if days_since_10_deaths != None and days_since_10_deaths >= 0:
        item["daysSince10Deaths"] = days_since_10_deaths
    # daysSince50Deaths
    deaths = grouped_sum.loc[iso3]["Deaths"].sort_index()
    days_since_50_deaths = compute_days_since(deaths, 50, current_date)
    if days_since_50_deaths != None and days_since_50_deaths>=0:
        item["daysSince50Deaths"] = days_since_50_deaths

format_id = lambda x: x.replace(" ", "_").replace("&", "_")

items = []

# Countries
def generate_country_item(ind_grp, grouped_sum, country_sub_national):
    (ind, grp) = ind_grp
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
        "num_subnational": int(country_sub_national[ind[0]]),
        "gdp_last_updated":grp["gdp_update_year"].iloc[0],
        "gdp_per_capita":grp["country_gdp"].iloc[0]  # For every date number of admin1 regions in country with reported cases.
    }
    compute_stats(item, grp, grouped_sum, ind[0], ind[1])
    return item

# Compute sub_national from latest dates for all countries
print("Generating admin0 items ... ")
country_sub_national = daily_df.sort_values("date").groupby(["computed_country_iso3"]).apply(lambda x: len(x[x["date"] == x["date"].max()]["computed_state_iso3"].unique())).sort_values()
grouped_sum = daily_df.groupby(["computed_country_iso3", "date"]).sum()

with multiprocessing.Pool(processes = nprocess) as pool:
    country_items = pool.starmap(generate_country_item, zip(daily_df.sort_values("date").groupby(["computed_country_iso3", "date"]), repeat(grouped_sum), repeat(country_sub_national)))
    pool.close()
    pool.join()
    items.extend(country_items)
    print("Completed generation of {} admin0 items.".format(len(country_items)))

# States
def generate_state_item(ind_grp, grouped_sum):
    ind,grp = ind_grp
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
        "long": grp["computed_state_long"].iloc[0],
        "gdp_last_updated":grp["gdp_update_year"].iloc[0],
        "country_gdp_per_capita":grp["country_gdp"].iloc[0]
    }
    if grp["computed_country_iso3"].iloc[0] == "USA":
        testing_columns = [i for i in daily_df.columns if "testing_" in i]
        for i in testing_columns:
            if pd.isna(grp[i].iloc[0]):
                continue
            item[i] = grp[i].iloc[0]
            # Compute case stats
    compute_stats(item, grp, grouped_sum, ind[0], ind[1])
    return item

print("Generating admin1 items ... ")

grouped_sum = daily_df.groupby(["computed_state_iso3", "date"]).sum()

with multiprocessing.Pool(processes = nprocess) as pool:
    state_items = pool.starmap(generate_state_item, zip(daily_df.groupby(["computed_state_iso3", "date"]), repeat(grouped_sum)))
    pool.close()
    pool.join()
    items.extend(state_items)
    print("Completed generation of {} admin1 items".format(len(state_items)))

# Counties
def generate_county_item(ind_grp, grouped_sum):
    ind,grp = ind_grp
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
        "long": grp["computed_county_long"].iloc[0],
        "gdp_last_updated":grp["gdp_update_year"].iloc[0],
        "country_gdp_per_capita":grp["country_gdp"].iloc[0]
    }
    compute_stats(item, grp, grouped_sum, ind[0], ind[1])
    return item

print("Generating admin2 items ... ")
grouped_sum = daily_df.groupby(["computed_county_iso3", "date"]).sum()

with multiprocessing.Pool(processes = nprocess) as pool:
    county_items = pool.starmap(generate_county_item, zip(daily_df.groupby(["computed_county_iso3", "date"]), repeat(grouped_sum)))
    pool.close()
    pool.join()
    items.extend(county_items)
    print("Completed generation of {} admin2 items.".format(len(county_items)))

# wb_region
def generate_region_item(ind_grp, grouped_sum):
    ind,grp = ind_grp
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
    return item

print("Generating region_wb items ... ")
grouped_sum = daily_df.groupby(["computed_region_wb", "date"]).sum()
with multiprocessing.Pool(processes = nprocess) as pool:
    region_items = pool.starmap(generate_region_item, zip(daily_df.groupby(["computed_region_wb", "date"]), repeat(grouped_sum)))
    pool.close()
    pool.join()
    items.extend(region_items)
    print("Completed generation of {} region_wb items".format(len(region_items)))

# Aggregate cities: KC and NYC
# Ignore multiprocessing because only 2 cities
print("Generating city items ... ")
city_items = []
grouped_sum = daily_df.groupby(["computed_city_iso3", "date"]).sum()
for ind, grp in daily_df.groupby(["computed_city_iso3", "date"]):
    item = {
        "date": ind[1].strftime("%Y-%m-%d"),
        "name": grp["computed_city_name"].iloc[0],
        "cbsa": grp["computed_city_iso3"].iloc[0],
        "location_id" : format_id("CITY_"+grp["computed_city_iso3"].iloc[0]),
        "lat": grp["Lat"].iloc[0],
        "long": grp["Long"].iloc[0],
        "_id": format_id("CITY_"+grp["computed_city_iso3"].iloc[0] + "_" + ind[1].strftime("%Y-%m-%d")),
        "admin_level": 1.7,
        "country_name": grp["computed_country_name"].iloc[0]
    }
    compute_stats(item, grp, grouped_sum, ind[0], ind[1])
    city_items.append(item)

items.extend(city_items)
print("Completed generation of {} city items ... ".format(len(city_items)))

# metropolitan areas
def generate_metro_item(ind_grp, grouped_sum, metro):
    ind,grp = ind_grp
    get_metro_counties = lambda x: metro[metro["CBSA Code"] == x][["County/County Equivalent", "State Name", "fips"]].rename(columns={"County/County Equivalent": "county_name", "State Name": "state_name"}).to_dict("records")
    item = {
        "date": ind[1].strftime("%Y-%m-%d"),
        "name": grp["computed_metro_name"].iloc[0],
        "cbsa": grp["computed_metro_cbsa"].iloc[0],
        "lat": grp["computed_metro_lat"].iloc[0],
        "long": grp["computed_metro_long"].iloc[0],
        "location_id" : format_id("METRO_"+grp["computed_metro_cbsa"].iloc[0]),
        "_id": format_id("METRO_"+grp["computed_metro_cbsa"].iloc[0] + "_" + ind[1].strftime("%Y-%m-%d")),
        "admin_level": 1.5,
        "country_name": grp["computed_country_name"].iloc[0],
        "sub_parts": get_metro_counties(grp["CBSA_Code"].iloc[0]),
        "wb_region": grp["computed_region_wb"].iloc[0]
    }
    compute_stats(item, grp, grouped_sum, ind[0], ind[1])
    return item

print("Generating metro items ... ")
grouped_sum = daily_df.groupby(["computed_metro_cbsa", "date"]).sum()
with multiprocessing.Pool(processes = nprocess) as pool:
    metro_items = pool.starmap(generate_metro_item, zip(daily_df.groupby(["computed_metro_cbsa", "date"]), repeat(grouped_sum), repeat(metro)))
    pool.close()
    pool.join()
    items.extend(metro_items)
    print("Completed generation of {} metro items.".format(len(metro_items)))

for item in items:
    for k,v in item.items():
        if type(v) == np.int64:
            item[k] = int(v)
        if type(v) == np.float64 or type(v) == np.float:
            item[k] = float(v)

with open(out_json_path, "w") as fout:
    json.dump(items, fout)
    fout.close()

print("Wrote {} items to {}".format(len(items), out_json_path))
