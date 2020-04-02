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

nprocess = 8

# Read shapefiles
admn0_path = os.path.join("./data","ne_10m_admin_0_countries.shp")
admn0_shp = fiona.open(admn0_path)
admn1_path = os.path.join("./data","ne_10m_admin_1_states_provinces.shp")
admn1_shp = fiona.open(admn1_path)
admn2_path = os.path.join("./data","tl_2019_us_county.shp")
usa_admn2_shp = fiona.open(admn2_path)
usa_metro_path = os.path.join("./data", "cb_2018_us_cbsa_500k.shp")
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

# Replace US counts with data from NYT
state_feats = {}
usa_admn2_feats = {}
nyt_county = pd.read_csv("../outbreak_db/nyt-covid-19-data/us-counties.csv", dtype = {
    "fips": str
})
nyt_state = pd.read_csv("../outbreak_db/nyt-covid-19-data/us-states.csv", dtype = {
    "fips": str
})

# Map territory fips to adm0_a3 code
fips_iso3 = {
    "VIR": "52",
    "GUM": "66",
    "MNP": "69",
    "VIR": "78",
    "PRI": "72"
}

# Extract matching features from NE shapefiles
usa_admn1_shp = [i for i in admn1_shp if i["properties"]["adm0_a3"] in fips_iso3.keys() or i["properties"]["adm0_a3"] == "USA"]

us_state_feats = []
for fips in nyt_state["fips"].unique():
    feats = [i for i in usa_admn1_shp if (i["properties"]["fips"] != None and i["properties"]["fips"][2:] == fips) or (i["properties"]["adm0_a3"] != "USA" and fips_iso3[i["properties"]["adm0_a3"]] == fips)]
    if len(feats) == 0:
        print("NYT Data doesn't have matching for state with fips {}".format(fips))
        continue
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
metro = pd.read_csv("./data/census_metropolitan_areas.csv", skiprows = 2, dtype = {
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
            d = {}
            current_date = None
            for state_test in state_tests:
                for k,v in state_test.items():
                    if v == None or k == "state":
                        continue
                    if k in ["lastUpdateEt", "checkTimeEt"]:
                        v = dt.strptime(v, "%m/%d %H:%M").strftime("2020-%m-%d %H:%M")
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

for ind, row in daily_df.iterrows():
    if ind % 5000 == 0:
        print("Completed {:.2f}% ...".format((ind/daily_df.shape[0]) * 100))
    if row["Lat"] == 91 and row["Long"] == 181:  # Cruises: wb_region: Cruises, admin0: Cruises, admin1: Diamond/Grand/princess
        region_name = "Cruises"
        state_name = get_cruise_ship_name(str(row["Country_Region"]) + " " + str(row["Province_State"]))
        daily_df.loc[ind, "name"] = state_name
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
    country_feat = None
    if row["Country_Region"] == "USA_NYT":
        country_feat = usa_country_feat
    else:
        country_feat = country_feats[(row["Lat"], row["Long"])]
    daily_df.loc[ind, "computed_country_name"] = country_feat["properties"]["NAME"]
    daily_df.loc[ind, "computed_country_iso3"] = country_feat["properties"]["ADM0_A3"]
    daily_df.loc[ind, "computed_country_pop"] = country_feat["properties"]["POP_EST"]
    daily_df.loc[ind, "computed_region_wb"] = country_feat["properties"]["REGION_WB"] if country_feat["properties"]["ADM0_A3"] != "CHN" else country_feat["properties"]["REGION_WB"] + ": China"
    centroid = get_centroid(country_feat["geometry"])
    daily_df.loc[ind, "computed_country_long"] = centroid[0]
    daily_df.loc[ind, "computed_country_lat"] = centroid[1]
    if not pd.isna(row["Province_State"]):
        if row["Country_Region"] == "USA_NYT" and row["Admin2"] not in ["New York City", "Kansas City"]:
            state_feat = us_state_feats[row["fips"][:2]]
            daily_df.loc[ind, "computed_state_long"] = row["Lat"]
            daily_df.loc[ind, "computed_state_lat"] = row["Long"]
        elif row["Admin2"] not in ["New York City", "Kansas City"]:
            state_feat = state_feats[(row["Lat"], row["Long"])]
            centroid = get_centroid(state_feat["geometry"])
            daily_df.loc[ind, "computed_state_long"] = centroid[0]
            daily_df.loc[ind, "computed_state_lat"] = centroid[1]
        daily_df.loc[ind, "computed_state_name"] = state_feat["properties"]["name"]
        daily_df.loc[ind, "computed_state_iso3"] = state_feat["properties"]["iso_3166_2"]
        # Add testing data to states in US
        if row["Country_Region"] == "USA_NYT":
            testing_key = row["date"].strftime("%Y-m%-%d") + "_" + state_feat["properties"]["iso_3166_2"]
            if testing_key in us_testing:
                for k,v in us_testing[key].items():
                    if k not in ["date", "hash"]:
                        daily_df.loc[ind, "testing_" + k] = v
        # Add county and cities
        if row["Country_Region"] == "USA_NYT" and not row["Admin2"].lower() == "unassigned" and not pd.isna(row["Admin2"]):
            if (row["Admin2"] == "New York City" and row["Province_State"] == "New York") and pd.isna(row["fips"]):
                daily_df.loc[ind, "computed_city_name"] = "New York City"
                daily_df.loc[ind, "computed_city_iso3"] = "NY_NYC"
                daily_df.loc[ind, "computed_city_long"] = row["Lat"]
                daily_df.loc[ind, "computed_city_lat"] = row["Long"]
                metro_feat = metro_feats["28140"]
                daily_df.loc[ind, "computed_metro_cbsa"] = metro_feat["properties"]["CBSAFP"]
                daily_df.loc[ind, "computed_metro_name"] = metro_feat["properties"]["NAME"]
                daily_df.loc[ind, "computed_metro_lat"], daily_df.loc[ind, "computed_metro_long"] = get_centroid(metro_feat["geometry"])
            elif (row["Admin2"] == "Kansas City" and row["Province_State"] == "Missouri") and pd.isna(row["fips"]):
                daily_df.loc[ind, "computed_city_name"] = "Kansas City"
                daily_df.loc[ind, "computed_city_iso3"] = "MO_KC"
                daily_df.loc[ind, "computed_city_long"] = row["Lat"]
                daily_df.loc[ind, "computed_city_lat"] = row["Long"]
                metro_feat = metro_feats["35620"]
                daily_df.loc[ind, "computed_metro_cbsa"] = metro_feat["properties"]["CBSAFP"]
                daily_df.loc[ind, "computed_metro_name"] = metro_feat["properties"]["NAME"]
                daily_df.loc[ind, "computed_metro_lat"], daily_df.loc[ind, "computed_metro_long"] = get_centroid(metro_feat["geometry"])
            else:
                county_feat = usa_admn2_feats[row["fips"]]
                daily_df.loc[ind, "computed_county_name"] = county_feat["properties"]["NAMELSAD"]
                daily_df.loc[ind, "computed_county_iso3"] = county_feat["properties"]["STATEFP"] + county_feat["properties"]["COUNTYFP"]
                centroid = get_centroid(county_feat["geometry"])
                daily_df.loc[ind, "computed_county_long"] = centroid[0]
                daily_df.loc[ind, "computed_county_lat"] = centroid[1]
                if not pd.isna(row["CBSA Code"]):  
                    metro_feat = metro_feats[row["CBSA Code"]]
                    if metro_feat != None:# Eliminate micropolitan areas
                        daily_df.loc[ind, "computed_metro_cbsa"] = metro_feat["properties"]["CBSAFP"]
                        daily_df.loc[ind, "computed_metro_name"] = metro_feat["properties"]["NAME"]
                        daily_df.loc[ind, "computed_metro_lat"], daily_df.loc[ind, "computed_metro_long"] = get_centroid(metro_feat["geometry"])
    daily_df.loc[ind, "JHU_Lat"] = row["Lat"]
    daily_df.loc[ind, "JHU_Long"] = row["Long"]

# Add GDP data
gdp_data_df = pd.read_csv(os.path.join("./data/API_NY.GDP.PCAP.CD_DS2_en_csv_v2_887243.csv"),header = 2)

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
        "num_subnational": int(country_sub_national[ind[0]]),
        "gdp_last_updated":grp["gdp_update_year"].iloc[0],
        "gdp_per_capita":grp["country_gdp"].iloc[0]  # For every date number of admin1 regions in country with reported cases.
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
        "long": grp["computed_state_long"].iloc[0],
        "gdp_last_updated":grp["gdp_update_year"].iloc[0],
        "country_gdp_per_capita":grp["country_gdp"].iloc[0]
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
        "long": grp["computed_county_long"].iloc[0],
        "gdp_last_updated":grp["gdp_update_year"].iloc[0],
        "country_gdp_per_capita":grp["country_gdp"].iloc[0]
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

# Aggregate cities: KC and NYC
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
        "admin_level": 1.7
    }
    compute_stats(item, grp, grouped_sum, ind[0], ind[1])
    items.append(item)

# metropolitan areas
grouped_sum = daily_df.groupby(["computed_metro_cbsa", "date"]).sum()
for ind, grp in daily_df.groupby(["computed_metro_cbsa", "date"]):
    item = {
        "date": ind[1].strftime("%Y-%m-%d"),
        "name": grp["computed_metro_name"].iloc[0],
        "cbsa": grp["computed_metro_cbsa"].iloc[0],
        "lat": grp["computed_metro_lat"].iloc[0],
        "long": grp["computed_metro_long"].iloc[0],
        "location_id" : format_id("METRO_"+grp["computed_metro_cbsa"].iloc[0]),
        "_id": format_id("METRO_"+grp["computed_metro_cbsa"].iloc[0] + "_" + ind[1].strftime("%Y-%m-%d")),
        "admin_level": 1.5
    }
    compute_stats(item, grp, grouped_sum, ind[0], ind[1])
    items.append(item)

print("Wrote {} items to file".format(len(items)))
with open("./data/biothings_items.json", "w") as fout:
    json.dump(items, fout)
    fout.close()
