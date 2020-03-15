import os
import pandas as pd
import fiona
from shapely.geometry import shape, Point, LinearRing
from collections import Counter
from datetime import datetime as dt
import hashlib
import copy
import numpy as np

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

def aggregate_countries(orig_df, shp, feats = []):
    df = orig_df.copy()
    countries = []
    logging.info("Computing country spatial joins")
    df["feats"] = df[["Lat", "Long"]].apply(lambda x: get_closest_polygon(shp, x["Lat"], x["Long"]), axis = 1) if len(feats) == 0 else feats
    logging.info("Completed  spatial joins")
    df["computed_country"] = df["feats"].apply(lambda i: i["properties"]["ADM0_A3"])
    for n, grp in df.groupby("computed_country"):
        states = grp[~grp["Province/State"].isna()]
        country = grp[grp["Province/State"].isna()]
        admin0_feature = grp.iloc[0]["feats"]
        # Lat Long
        grp_lat_lng = grp.iloc[0][grp.columns[2:4]]
        if states.shape[0] > 0 and country.shape[0] > 0:
            logging.warning("{}: State and country level available. Manual check required to sum.".format(n))
        if states.shape[0] > 0 and country.shape[0] == 0:
            logging.info("{}: Only states. Summing all".format(n))
        if states.shape[0] == 0 and country.shape[0] > 0:
            logging.info("{}: Only country".format(n))
        grp_sum = grp.sum()[grp.columns[4:-2]]
        grp_lat_lng.append(grp_sum)
        row = grp_lat_lng.append(grp_sum)
        row["JHU_Lat"] = row["Lat"]
        row["JHU_Long"] = row["Long"]
        row["admin_level"] = 0
        row["name"] = admin0_feature["properties"]["NAME"]
        row["iso3"] = admin0_feature["properties"]["ADM0_A3"]
        row["location_id"] = n
        row["population"] = admin0_feature["properties"]["POP_EST"]
        geom = admin0_feature["geometry"]
        centroid = get_centroid(geom)
        row["lat"] = centroid[1]
        row["long"] = centroid[0]
        countries.append(row)
    countries_df = pd.concat(countries, axis = 1).transpose()
    return countries_df, df["feats"].tolist()

def aggregate_states(orig_df, admn0_shp, admn1_shp, feats = []):
    df = orig_df.copy()
    df_states = df[~df["Province/State"].isna()]
    df_states["feats"] = df_states[["Lat", "Long"]].apply(lambda x: get_closest_polygon(admn1_shp, x["Lat"], x["Long"]), axis = 1) if len(feats) == 0 else feats
    df_states["computed_state"] = df_states["feats"].apply(lambda i: i["properties"]["iso_3166_2"]+ "_" + i["properties"]["adm0_a3"])
    states = []
    for n, grp in df_states.groupby("computed_state"):
        grp_lat_lng = grp.iloc[0][grp.columns[2:4]]
        grp_sum = grp.sum()[grp.columns[4:-2]]
        admin1_feature = grp.iloc[0]["feats"]
        row = grp_lat_lng.append(grp_sum)
        row["JHU_Lat"] = row["Lat"]
        row["JHU_Long"] = row["Long"]
        row["admin_level"] = 1
        row["name"] = admin1_feature["properties"]["name"]
        row["iso3"] = admin1_feature["properties"]["iso_3166_2"]
        row["location_id"] = n
        row["country_iso3"] = admin1_feature["properties"]["adm0_a3"]
        row["country_name"] = admin1_feature["properties"]["admin"]
        country_pop = [i["properties"]["POP_EST"] for i in admn0_shp if i["properties"]["ADM0_A3"] == row["country_iso3"]][0]
        row["country_population"] = country_pop
        geom = admin1_feature["geometry"]
        centroid = get_centroid(geom)
        row["lat"] = centroid[1]
        row["long"] = centroid[0]
        logging.info("Finished computing for {}".format(n))
        states.append(row)
    states_df = pd.concat(states, axis = 1).transpose()
    return states_df, df_states["feats"].tolist()

def get_stats(confirmed_row, dead_row, recovered_row, date_cols):
    confirmed_row = confirmed_row[date_cols]
    recovered_row = recovered_row[date_cols]
    dead_row = dead_row[date_cols]
    attr = {}
    first = {}
    to_date = lambda x: dt.strptime(x, "%m/%d/%y").strftime("%Y-%m-%d")
    for row, n in zip([confirmed_row, dead_row, recovered_row], ["confirmed", "dead", "recovered"]):
        attr[n+"_currentToday"] = to_date(row.index[-1])
        attr[n+"_numIncrease"] = row[-1] - row[-2]
        attr[n+"_pctIncrease"] = (row[-1] - row[-2])/row[-2] if row[-2] > 0 else 0
        diff = row[row.diff() > 0]
        first_date = to_date(diff.index[0]) if diff.shape[0] > 0 else ""
        first[n] = first_date
        attr[n+"_firstDate"] = first_date
        attr[n+"_newToday"] = True if row.index[-1] == first_date else False
        attr[n+"_currentCases"] = row[-1]
    if first["confirmed"] != "" and first["dead"] != "":
        attr["first_dead-first_confirmed"] = (dt.strptime(first["confirmed"], "%Y-%m-%d") - dt.strptime(first["dead"], "%Y-%m-%d")).days
    else:
        attr["first_dead-first_confirmed"] = ""
    return attr

def load_annotations(data_folder):
    annotations = {}
    # Read shapefiles
    admn0_path = os.path.join(data_folder,"ne_10m_admin_0_countries.shp")
    admn0_shp = fiona.open(admn0_path)
    admn1_path = os.path.join(data_folder,"ne_10m_admin_1_states_provinces.shp")
    admn1_shp = fiona.open(admn1_path)
    admn2_path = os.path.join(data_folder,"tl_2019_us_county.shp")
    usa_admn2_shp = fiona.open(admn2_path)
    # Read csv
    confirmed_file_path = os.path.join(data_folder,"time_series_19-covid-Confirmed.csv")
    confirmed = pd.read_csv(confirmed_file_path)
    recovered_file_path = os.path.join(data_folder,"time_series_19-covid-Recovered.csv")
    recovered = pd.read_csv(recovered_file_path)
    deaths_file_path = os.path.join(data_folder,"time_series_19-covid-Deaths.csv")
    deaths = pd.read_csv(deaths_file_path)
    # Remove cruises for now
    confirmed = confirmed[confirmed["Province/State"].apply(lambda x: "princess" not in x.lower() if not pd.isna(x) else True)]
    deaths = deaths[deaths["Province/State"].apply(lambda x: "princess" not in x.lower() if not pd.isna(x) else True)]
    recovered = recovered[recovered["Province/State"].apply(lambda x: "princess" not in x.lower() if not pd.isna(x) else True)]

    countries_confirmed, feats = aggregate_countries(confirmed, admn0_shp)
    countries_recovered, feats = aggregate_countries(recovered, admn0_shp, feats)
    countries_dead, feats = aggregate_countries(deaths, admn0_shp, feats)

    states_confirmed, feats = aggregate_states(confirmed, admn0_shp, admn1_shp)
    states_recovered, feats = aggregate_states(recovered, admn0_shp, admn1_shp, feats)
    states_dead, feats = aggregate_states(deaths, admn0_shp, admn1_shp, feats)

    items = []
    for (cid, conf), (rid, recov), (did, dead) in zip(countries_confirmed.iterrows(), countries_recovered.iterrows(), countries_dead.iterrows()):
        date_cols = countries_confirmed.columns[2:-9]
        item = {}
        for i in countries_confirmed.columns[-9:]:
            item[i] = conf[i]
        for d in date_cols:
            ditem = copy.deepcopy(item)
            ditem["confirmed"] = conf[d]
            ditem["recovered"] = recov[d]
            ditem["dead"] = dead[d]
            ditem["date"] = dt.strptime(d, "%m/%d/%y").strftime("%Y-%m-%d")
            attr = get_stats(conf, recov, dead, date_cols)
            for k,v in attr.items():
                ditem[k] = v
            items.append(ditem)

    for (cid, conf), (rid, recov), (did, dead) in zip(states_confirmed.iterrows(), states_recovered.iterrows(), states_dead.iterrows()):
        date_cols = states_confirmed.columns[2:-11]
        item = {}
        for i in states_confirmed.columns[-11:]:
            item[i] = conf[i]
        for d in date_cols:
            ditem = copy.deepcopy(item)
            ditem["confirmed"] = conf[d]
            ditem["recovered"] = recov[d]
            ditem["dead"] = dead[d]
            ditem["date"] = dt.strptime(d, "%m/%d/%y").strftime("%Y-%m-%d")
            attr = get_stats(conf, recov, dead, date_cols)
            for k,v in attr.items():
                ditem[k] = v
            items.append(ditem)

    for item in items:
        for k,v in item.items():
            if type(v) == np.int64:
                item[k] = int(item[v])
            if type(v) == np.float64 or type(v) == np.float:
                item[k] = float(v)
        item["_id"] = item["location_id"]+"_"+item["date"]
        yield item
