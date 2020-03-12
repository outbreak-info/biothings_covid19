import os
import pandas as pd
import fiona
from shapely.geometry import shape, Point, LinearRing
from collections import Counter
from datetime import datetime as dt
import hashlib
import copy

from biothings import config
logging = config.logger

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
def get_closest_polygon(shp, lat, lng):
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

def load_annotations(data_folder):
    annotations = {}
    for ind, row in confirmed.iterrows():
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
        admin0_feature = None
        admin1_feature = None
        admin2_feature = None
        # Get country either way
        admin0_feature = get_closest_polygon(admn0_shp, row["Lat"], row["Long"])
        if pd.isna(row["Province/State"]):  # Case count set at country level only
            admin_level = 0
        else:
            admin1_feature = get_closest_polygon(admn1_shp, row["Lat"], row["Long"])
            admin_level = 1
            if "County" in row["Province/State"] or ", " in row["Province/State"]:
                admin2_feature = get_closest_polygon(usa_admn2_shp, row["Lat"], row["Long"])
                admin_level = 2
        if admin0_feature == None:
            print("Country not found for indice {} with name {}".format(ind, row["Country/Region"]))
            continue
        attr["admin_level"] = admin_level
        attr["admin0"] = admin0_feature["properties"]["NAME"]
        attr["admin0_iso3"] = admin0_feature["properties"]["ADM0_A3"]
        attr["admin0_pop"] = admin0_feature["properties"]["POP_EST"]
        geom = admin0_feature["geometry"]
        if admin_level > 0:         # For admin_level 1 and 2
            if admin1_feature == None:
                print("State not found for indice {} with name {}".format(ind, row["State/Province"]))
                continue
            attr["admin1"] = admin1_feature["properties"]["name"]
            attr["admin1_iso3"] = admin1_feature["properties"]["iso_3166_2"]
            geom = admin1_feature["geometry"]
            if admin_level == 2:
                if admin2_feature == None:
                    print("County not found for indice {} with name {}".format(ind, row["State/Province"]))
                    continue
                attr["admin2"] = admin2_feature["properties"]["NAME"]
                attr["admin2_fips"] = admin2_feature["properties"]["STATEFP"] + admin2_feature["properties"]["COUNTYFP"]
                geom = admin2_feature["geometry"]
        attr["lng"], attr["lat"]  = get_centroid(geom)
        attr["old_lat"] = row["Lat"]
        attr["old_lng"] = row["Long"]
        attr["old_name"] = "{}, {}".format(row["Province/State"], row["Country/Region"])
        if ind % 20 == 0:
            print("Completed {} records".format(ind + 1))
        get_text = lambda x: x if x != None else ""
        for date_ind, d in enumerate([dt.strptime(i, "%m/%d/%y").strftime("%Y-%m-%d") for i in row.index[4:]]):
            # ID includes admin0_iso3 + admin1_iso3 + admin2_fips + date
            id_text = get_text(attr["admin0_iso3"]) + get_text(attr["admin1_iso3"]) + get_text(attr["admin2_fips"]) + get_text(attr["admin2"]) + d
            hash_id = hashlib.md5(id_text.encode())
            _id = hash_id.hexdigest()
            if _id in annotations:
                annotations[_id]["confirmed"] += row[row.index[4 + date_ind]]
                annotations[_id]["recovered"] += recovered.ix[ind, row.index[4 + date_ind]]
                annotations[_id]["deaths"] += deaths.ix[ind, row.index[4 + date_ind]]
            else:
                item = copy.deepcopy(attr)
                item["date"] = d
                item["id_text"] = id_text
                item["confirmed"] = row[row.index[4 + date_ind]]
                item["recovered"] = recovered.ix[ind, row.index[4 + date_ind]]
                item["deaths"] = deaths.ix[ind, row.index[4 + date_ind]]
                annotations[_id] = item
    for _id, annt in annotations.items():
        yield {"_id": _id, "annotations": annt}
