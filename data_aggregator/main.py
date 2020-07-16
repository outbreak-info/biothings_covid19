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
from data_aggregator.geometry     import get_centroid, get_closest_polygon, check_point_in_polygon, get_distance_from_polygon
from data_aggregator.daily_report import read_daily_report, add_lat_long, get_us_admn2_feat, fix_daily_reports
from data_aggregator.stats        import (
                                          compute_days_since,
                                          compute_doubling_rate,
                                          get_us_testing_data,
                                          generate_metro_item,
                                          generate_region_item,
                                          generate_county_item,
                                          generate_state_item,
                                          generate_country_item,
                                          get_metro_feat,
                                          compute_stats,
                                          populate_country,
                                          populate_state,
                                          populate_non_us_state,
                                          populate_us_county,
                                          populate_us_metro,
                                         )
import time
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(message)s',
                    filename='logs/data_aggregator.log')

for key in logging.Logger.manager.loggerDict:
    # ignore lower-level libraries
    logging.getLogger(key).setLevel(logging.WARNING)

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

    daily_reports = [read_daily_report(os.path.join(daily_reports_path, i)) for i in os.listdir(daily_reports_path) if i[-4:] == ".csv"]

    daily_df = pd.concat(daily_reports, ignore_index = True)
    fix_daily_reports(daily_df, admn0_shp)


    # Replace US counts with data from NYT
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
    def extract_us_state_feats(state_fips):
        us_state_feats = {}
        for fips in state_fips:
            feats = [i for i in usa_admn1_shp if (i["properties"]["fips"] != None and i["properties"]["fips"][2:] == fips) or (i["properties"]["adm0_a3"] != "USA" and fips_iso3[i["properties"]["adm0_a3"]] == fips)]
            if len(feats) == 0:
                logging.warning("NYT Data doesn't have matching for state with fips {}".format(fips))
                assert False, "FIPS for NYT data missing. Please add iso3 code to fips_iso3 dict on line 205"
            us_state_feats[fips] = feats[0]

        return us_state_feats

    us_state_feats = extract_us_state_feats(nyt_state["fips"].unique())

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
    tmp = daily_df.apply(populate_country, axis =  1)
    for i in tmp.columns:
        daily_df.loc[tmp.index, i] = tmp[i]

    # US States set lat. For New York City and Kansas City, lat_lng already set
    logging.info("Populating US States ... ")
    us_states = daily_df.loc[~daily_df["Province_State"].isna() & (daily_df["Country_Region"] == "USA_NYT") & (~daily_df["Admin2"].isin(["New York City", "Kansas City"]))]
    tmp = us_states.apply(populate_state, axis= 1)
    for i in tmp.columns:
        daily_df.loc[tmp.index, i] = tmp[i]

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

    tmp = non_us_states.apply(populate_non_us_state, axis= 1)
    for i in tmp.columns:
        daily_df.loc[tmp.index, i] = tmp[i]

    # Admin2
    logging.info("Populating US counties ... ")
    us_county_df = daily_df[~daily_df["Province_State"].isna() & (daily_df["Country_Region"] == "USA_NYT") & ~(daily_df["Admin2"] == "Unassigned") & ~(pd.isna(daily_df["Admin2"])) & ~(daily_df["Admin2"].isin(["New York City", "Kansas City"]))]
    tmp = us_county_df.apply(populate_us_county, axis= 1)
    for i in tmp.columns:
        daily_df.loc[tmp.index, i] = tmp[i]

    # Add metropolitan areas
    logging.info("Populating metropolitan areas ...")
    us_metro_df = us_county_df[~us_county_df["CBSA_Code"].isna()]
    tmp = us_metro_df.apply(populate_us_metro, axis= 1)
    for i in tmp.columns:
        daily_df.loc[tmp.index, i] = tmp[i]

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
    daily_df.loc[nyc_df.index, "computed_metro_pop"] = metro_feat["properties"]["POPESTI"]
    centroid = get_centroid(metro_feat["geometry"])
    daily_df.loc[nyc_df.index, "computed_metro_long"] = centroid[0]
    daily_df.loc[nyc_df.index, "computed_metro_lat"] = centroid[1]
    # Add state for city_df records
    ny_state_feature = next(i for i in admn1_shp if i["properties"]["i_3166_"] == "US-NY")
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
    daily_df.loc[kc_df.index, "computed_metro_pop"] = metro_feat["properties"]["POPESTI"]
    centroid = get_centroid(metro_feat["geometry"])
    daily_df.loc[kc_df.index, "computed_metro_long"] = centroid[0]
    daily_df.loc[kc_df.index, "computed_metro_lat"] = centroid[1]
    # Add state for city_df records
    mo_state_feature = next(i for i in admn1_shp if i["properties"]["i_3166_"] == "US-MO")
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
            "country_name": grp["computed_country_name"].iloc[0],
            "population": grp["computed_metro_pop"].iloc[0]
            }
        compute_stats(item, grp, grouped_sum, ind[0], ind[1])
        city_items.append(item)

    items.extend(city_items)
    logging.info("Completed generation of {} city items ... ".format(len(city_items)))

    logging.info("Generating metro items ... ")
    grouped_sum = daily_df.groupby(["computed_metro_cbsa", "date"]).sum()
    with multiprocessing.Pool(processes = nprocess) as pool:
        metro_items = pool.starmap(generate_metro_item, zip(daily_df.groupby(["computed_metro_cbsa", "date"]), repeat(grouped_sum), repeat(metro)))
        pool.close()
        pool.join()
        items.extend(metro_items)
        logging.info("Completed generation of {} metro items.".format(len(metro_items)))

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
