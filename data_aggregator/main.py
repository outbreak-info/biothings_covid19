#!/usr/bin/python3
import pandas as pd
import os
import sys
from datetime import datetime as dt
import fiona
import multiprocessing
from itertools import repeat
import numpy as np
import json
import re
import configparser
from data_aggregator.geometry     import *
from data_aggregator.daily_report import *
from data_aggregator.stats        import *
import time
import pickle
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(message)s',
                    filename='logs/data_aggregator.log')

for key in logging.Logger.manager.loggerDict:
    # there's a better way to set up logging
    # but this ignores anything less than a warning in imported libraries
    logging.getLogger(key).setLevel(logging.WARNING)

def pick(df, index):
    df.to_pickle("./pickles/{index}.p".format(index=index))
    return

def picklist(l, name):
    with open('./pickles/list{name}.p'.format(name=name), 'wb') as p:
        pickle.dump(l, p)

def unpick(filename):
    return pd.read_pickle("./pickles/{filename}.p".format(filename=filename))

if __name__ == "__main__":
    if len(sys.argv) > 1:
        conf_path = sys.argv[1]
    else:
        conf_path = "config.ini"
    # Get paths
    config = configparser.ConfigParser()
    config.read(conf_path)
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

    #######################
    # Parse daily reports #
    #######################
    logging.info("🕜 GET REPORTS")

    daily_reports = [read_daily_report(os.path.join(daily_reports_path, i)) for i in os.listdir(daily_reports_path) if i[-4:] == ".csv"]

    daily_df = pd.concat(daily_reports, ignore_index = True)

    # Correct wrong lat_lng longs. Check if name matches shapefile nad populate list below.
    wrong_lat_long = ["Belize", "Malaysia"]
    for cntry in wrong_lat_long:
        # Check if only country_feat has admin1 or admin2.
        n_admin_lower = daily_df[daily_df["Country_Region"] == cntry][["Province_State"]].dropna().shape[0] + daily_df[daily_df["Country_Region"] == cntry][["Admin2"]].dropna().shape[0]
        if n_admin_lower > 0:
            logging.warning("{} has admin 1 or 2. Verify if centroid of country is correct lat long!")
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

    # First get lat_long long for state and then for country
    add_lat_long(daily_df, "Province_State", "Admin2")
    add_lat_long(daily_df, "Admin2")
    add_lat_long(daily_df, "Country_Region", ["Province_State", "Admin2"])

    # Remove nan lat long : cruises mostly
    unknown = daily_df[daily_df["Lat"].isna()]
    logging.warning("Unknown lat longs for {} rows".format(unknown.shape[0]))
    unknown_confirmed = unknown.sort_values("date", ascending = False).groupby("Province_State").head(1)["Confirmed"].sum()
    logging.warning("Unaccounted cases due to missing lat long: {}".format(unknown_confirmed))
    logging.info("\n".join(daily_df[daily_df["Lat"].isna()]["Country_Region"].unique()))
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
            logging.warning("NYT Data doesn't have matching for state with fips {}".format(fips))
            assert False, "FIPS for NYT data missing. Please add iso3 code to fips_iso3 dict on line 205"
        us_state_feats.append([fips, feats[0]])

    us_state_feats = dict(us_state_feats)

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
    logging.info("🕡 NYT COUNTY OVER")

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
    logging.info("🕝 NYT OVER")

    # Add metro politan CBSA codes
    metro = pd.read_csv(census_regions_path, skiprows = 2, dtype = {
        "FIPS State Code": str,
        "FIPS County Code": str,
        "CBSA Code": str
    })
    metro = metro[~metro["FIPS County Code"].isna()]  # Gets rid of bottom 3 rows in file
    metro["fips"] = metro["FIPS State Code"] + metro["FIPS County Code"].apply(lambda x: x.zfill(3))
    daily_df = pd.merge(daily_df, metro, on = "fips", how="left")

    metro_feats = []
    metro_list = daily_df["CBSA Code"].dropna().unique()
    with multiprocessing.Pool(processes = nprocess) as pool:
        metro_feats = pool.starmap(get_metro_feat, zip(metro_list, repeat(list(usa_metro_shp))))
        pool.close()
        pool.join()

    metro_feats = dict(zip(metro_list, metro_feats))

    us_testing = get_us_testing_data(admn1_shp)

    #################################################
    # Compute geo joins for countries other than US #
    #################################################

    state_feats = {}
    country_feats = {}

    logging.info("Computing geo joins ... ")

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

    logging.info("Completed geo joins.")

    logging.info("Populating dataframe ... ")

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
    logging.info("Populating cruises ... ")
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
    logging.info("Populating countries ... ")
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
    logging.info("Populating US States ... ")
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
    logging.info("Populating Admin1 regions outside US ... ")
    non_us_states = daily_df.loc[~daily_df["Province_State"].isna() & (daily_df["Country_Region"] != "USA_NYT") & (~daily_df["Admin2"].isin(["New York City", "Kansas City"]))]
    centroids = non_us_states.apply(lambda x: get_centroid(state_feats[(x["Lat"], x["Long"])]["geometry"]), axis = 1)
    daily_df.loc[non_us_states.index, "computed_state_long"] = [i[0] for i in centroids]
    daily_df.loc[non_us_states.index, "computed_state_lat"] = [i[1] for i in centroids]
    daily_df.loc[non_us_states.index, "computed_state_name"] = non_us_states.apply(lambda x: state_feats[(x["Lat"], x["Long"])]["properties"]["name"], axis = 1)
    daily_df.loc[non_us_states.index, "computed_state_iso3"] = non_us_states.apply(lambda x: state_feats[(x["Lat"], x["Long"])]["properties"]["iso_3166_2"], axis = 1)

    # Admin2
    logging.info("Populating US counties ... ")
    us_county_df = daily_df[~daily_df["Province_State"].isna() & (daily_df["Country_Region"] == "USA_NYT") & ~(daily_df["Admin2"] == "Unassigned") & ~(pd.isna(daily_df["Admin2"])) & ~(daily_df["Admin2"].isin(["New York City", "Kansas City"]))]
    daily_df.loc[us_county_df.index, "computed_county_name"] = us_county_df["fips"].apply(lambda x: usa_admn2_feats[x]["properties"]["NAMELSAD"])
    daily_df.loc[us_county_df.index, "computed_county_iso3"] = us_county_df["fips"].apply(lambda x: usa_admn2_feats[x]["properties"]["STATEFP"] + usa_admn2_feats[x]["properties"]["COUNTYFP"])
    centroids = us_county_df["fips"].apply(lambda x: get_centroid(usa_admn2_feats[x]["geometry"]))
    daily_df.loc[us_county_df.index, "computed_county_long"] = [i[0] for i in centroids]
    daily_df.loc[us_county_df.index, "computed_county_lat"] = [i[1] for i in centroids]

    # Add metropolitan areas
    logging.info("Populating metropolitan areas ...")
    us_metro_df = us_county_df[~us_county_df["CBSA_Code"].isna()]
    daily_df.loc[us_metro_df.index, "computed_metro_cbsa"] = us_metro_df["CBSA_Code"].apply(lambda x: metro_feats[x]["properties"]["CBSAFP"] if metro_feats[x] != None else None)
    daily_df.loc[us_metro_df.index, "computed_metro_name"] = us_metro_df["CBSA_Code"].apply(lambda x: metro_feats[x]["properties"]["NAME"] if metro_feats[x] != None else None)
    centroids = us_metro_df["CBSA_Code"].apply(lambda x: get_centroid(metro_feats[x]["geometry"]) if metro_feats[x] != None else [None, None])
    daily_df.loc[us_metro_df.index, "computed_metro_long"] = [i[0] for i in centroids]
    daily_df.loc[us_metro_df.index, "computed_metro_lat"] = [i[1] for i in centroids]

    # Add admin2 codes for cities: NYC and KC
    logging.info("Populating cities (NYC + KC)")
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
    logging.info("Adding GDP per capita for countries")
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


    logging.info("Dataframe ready")

    # Export dataframe
    daily_df.to_csv(export_df_path)

    ###########################
    #Generate items and stats #
    ###########################

    format_id = lambda x: x.replace(" ", "_").replace("&", "_")

    items = []

    # Compute sub_national from latest dates for all countries
    logging.info("Generating admin0 items ... ")

    country_sub_national = daily_df.sort_values("date").groupby(["computed_country_iso3"]).apply(lambda x: len(x[x["date"] == x["date"].max()]["computed_state_iso3"].unique())).sort_values()
    grouped_sum = daily_df.groupby(["computed_country_iso3", "date"]).sum()

    with multiprocessing.Pool(processes = nprocess) as pool:
        country_items = pool.starmap(generate_country_item, zip(daily_df.sort_values("date").groupby(["computed_country_iso3", "date"]), repeat(grouped_sum), repeat(country_sub_national)))
        pool.close()
        pool.join()
        items.extend(country_items)
        logging.info("Completed generation of {} admin0 items.".format(len(country_items)))

    logging.info("Generating admin1 items ... ")

    grouped_sum = daily_df.groupby(["computed_state_iso3", "date"]).sum()

    with multiprocessing.Pool(processes = nprocess) as pool:
        testing_columns = [i for i in daily_df.columns if "testing_" in i]
        state_items = pool.starmap(generate_state_item, zip(daily_df.groupby(["computed_state_iso3", "date"]), repeat(grouped_sum), repeat(testing_columns)))
        pool.close()
        pool.join()
        items.extend(state_items)
        logging.info("Completed generation of {} admin1 items".format(len(state_items)))

    logging.info("Generating admin2 items ... ")
    grouped_sum = daily_df.groupby(["computed_county_iso3", "date"]).sum()

    with multiprocessing.Pool(processes = nprocess) as pool:
        county_items = pool.starmap(generate_county_item, zip(daily_df.groupby(["computed_county_iso3", "date"]), repeat(grouped_sum)))
        pool.close()
        pool.join()
        items.extend(county_items)
        logging.info("Completed generation of {} admin2 items.".format(len(county_items)))

    logging.info("Generating region_wb items ... ")
    grouped_sum = daily_df.groupby(["computed_region_wb", "date"]).sum()
    with multiprocessing.Pool(processes = nprocess) as pool:
        region_items = pool.starmap(generate_region_item, zip(daily_df.groupby(["computed_region_wb", "date"]), repeat(grouped_sum)))
        pool.close()
        pool.join()
        items.extend(region_items)
        logging.info("Completed generation of {} region_wb items".format(len(region_items)))

    # Aggregate cities: KC and NYC
    # Ignore multiprocessing because only 2 cities
    logging.info("Generating city items ... ")
    picklist(items, 'region_wb')
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
    logging.info("Completed generation of {} city items ... ".format(len(city_items)))
    picklist(city_items, 'city_items')

    logging.info("Generating metro items ... ")
    grouped_sum = daily_df.groupby(["computed_metro_cbsa", "date"]).sum()
    with multiprocessing.Pool(processes = nprocess) as pool:
        metro_items = pool.starmap(generate_metro_item, zip(daily_df.groupby(["computed_metro_cbsa", "date"]), repeat(grouped_sum), repeat(metro)))
        pool.close()
        pool.join()
        items.extend(metro_items)
        logging.info("Completed generation of {} metro items.".format(len(metro_items)))
    picklist(items, 'metro')

    for item in items:
        for k,v in item.items():
            if type(v) == np.int64:
                item[k] = int(v)
            if type(v) == np.float64 or type(v) == np.float:
                item[k] = float(v)

    with open(out_json_path, "w") as fout:
        json.dump(items, fout)
        fout.close()

    logging.info("Wrote {} items to {}".format(len(items), out_json_path))
