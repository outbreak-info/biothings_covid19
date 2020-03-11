import os
import pandas as pd
import fiona
from shapely.geometry import shape, Point
from collections import Counter
from datetime import datetime as dt
import hashlib

def keep_polygons_with_highest_area(admn_shp, unique_key):
    admn_shp = list(admn_shp)
    # Get unique ISO by getting polygon with highest area
    multiple_features = [k for k,v in Counter([i["properties"][unique_key] for i in admn_shp if i["properties"][unique_key] != "" and i["properties"][unique_key] != None]).items() if v > 1]
    for iso in multiple_features:
        polygons = [i for i in admn_shp if i["properties"][unique_key] == iso]
        polygons.sort(key = lambda x: -1 * shape(x["geometry"]).area)
        for i in polygons[1:]:      # Delete other polygons while ertaining one with highest area
            del admn_shp[admn_shp.index(i)]
    return admn_shp

def check_point_in_polygon(geom, lat ,lng):
    p = Point(lng, lat)
    if p.within(shape(geom)):
        return True
    return False

def load_annotations(data_folder):
    data_file_path = os.path.join(data_folder,"time_series_19-covid-Confirmed.csv")
    df = pd.read_csv(data_file_path)
    # Remove cruise ships
    df = df[~df["Province/State"] != "Grand Princess Cruise Ship"]
    admn0_path = os.path.join(data_folder,"ne_10m_admin_0_countries.shp")
    admn0_shp = fiona.open(admn0_path)
    admn0_shp = keep_polygons_with_highest_area(admn0_shp, "ADM0_A3")
    admn1_path = os.path.join(data_folder,"ne_10m_admin_1_states_provinces.shp")
    admn1_shp = fiona.open(admn1_path)
    admn1_shp = keep_polygons_with_highest_area(admn1_shp, "iso_3166_2")
    admn2_path = os.path.join(data_folder,"tl_2019_us_county.shp")
    usa_admn2_shp = fiona.open(admn2_path)
    for ind, row in df.iterrows():
        lat = 0
        lng = 0
        attr = {
            "admin_level": None,
            "admin0": None,
            "admin1": None,
            "admin2": None,
            "admin0_iso3": None,
            "admin1_iso3": None,
            "admin2_fips": None,
            "admin0_pop": None,
            "lat": None,
            "lng": None,
            "date": None
        }
        m = []
        geom = None
        admin_level = None
        admin0_features = None
        admin1_features = None
        admin2_features = None
        # Get country either way
        admin0_features = [feat for feat in admn0_shp if check_point_in_polygon(feat["geometry"], row["Lat"], row["Long"])]
        if pd.isna(row["Province/State"]):  # Case count set at country level only
            admin_level = 0
        else:
            admin1_features = [feat for feat in admn1_shp if check_point_in_polygon(feat["geometry"], row["Lat"], row["Long"])]
            admin_level = 1
            if "County" in row["Province/State"]:
                admin2_features = [feat for feat in usa_admn2_shp if check_point_in_polygon(feat["geometry"], row["Lat"], row["Long"])]
                admin_level = 2
        if admin0_features == None or len(admin0_features) < 1:
            print("Country not found for indice {} with name {}".format(ind, row["Country/Region"]))
            continue
        attr["admin_level"] = admin_level
        if len(admin0_features) > 1:
            print("Multiple country features found for indice {} with name {}".format(ind, row["Country/Region"]))
        attr["admin0"] = admin0_features[0]["properties"]["NAME"]
        attr["admin0_iso3"] = admin0_features[0]["properties"]["ADM0_A3"]
        attr["admin0_pop"] = admin0_features[0]["properties"]["POP_EST"]
        geom = shape(admin0_features[0]["geometry"])
        if admin_level > 0:         # For admin_level 1 and 2
            if len(admin1_features) < 1:
                print("State not found for indice {} with name {}".format(ind, row["State/Province"]))
                continue
            if len(admin1_features) > 1:
                print("Multiple country features found for indice {} with name {}".format(ind, row["Country/Region"]))
            attr["admin1"] = admin1_features[0]["properties"]["name"]
            attr["admin1_iso3"] = admin1_features[0]["properties"]["iso_3166_2"]
            geom = shape(admin1_features[0]["geometry"])
            if admin_level == 2:
                if len(admin2_features) < 1:
                    print("County not found for indice {} with name {}".format(ind, row["State/Province"]))
                    continue
                if len(admin2_features) > 1:
                    print("Multiple couty features found for indice {} with name {}".format(ind, row["State/Province"]))
                attr["admin2"] = admin2_features[0]["properties"]["NAME"]
                attr["admin2_fips"] = admin2_features[0]["properties"]["STATEFP"] + admin2_features[0]["properties"]["COUNTYFP"]
                geom = shape(admin2_features[0]["geometry"])
        attr["lng"] = geom.centroid.xy[0][0]
        attr["lat"] = geom.centroid.xy[1][0]
        if ind % 20 == 0:
            print("Completed {} records".format(ind + 1))
        get_id = lambda x: x if x != None else ""
        for d in [dt.strptime(i, "%m/%d/%y").strftime("%Y-%m-%d") for i in row.index[4:]]:
            # ID includes admin0_iso3 + admin1_iso3 + admin2_fips + date
            id_text = get_id(attr["admin0_iso3"]) + get_id(attr["admin1_iso3"]) + get_id(attr["admin2_fips"]) + d
            hash_id = hashlib.md5(id_text.encode())
            _id = hash_id.hexdigest()
            attr["date"] = d
            yield {"_id": _id, "annotations": attr}
