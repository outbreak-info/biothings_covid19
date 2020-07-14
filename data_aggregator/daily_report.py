import pandas as pd
import numpy as np
from datetime import datetime as dt
import os
import logging

def read_daily_report(path):
    _dtypes = {
        "Provine/State": str,
        "Country/Region": str,
        "Laste Update": str,
        "Confirmed": float,
        "Deaths": float,
        "Recovered": float,
        "Lat": float,
        "Long_":float,
        "FIPS": str,
        "Admin2": str,
        "Province_State": str,
        "Country_Region": str,
        "Last_Update": str,
        "Active": float,
        "Combined_Key": str,
        "Incidence_Rate": float,
        "Case-Fatality_Ratio": float
    }
    df = pd.read_csv(path, dtype = _dtypes)
    df.columns = [i.replace("/", "_").replace(" ", "_").strip() for i in df.columns]
    df.columns = [i if i not in ["Lat", "Latitude"] else "Lat" for i in df.columns]
    df.columns = [i if i not in ["Long_", "Longitude"] else "Long"for i in df.columns]
    df["date"] = dt.strptime(os.path.basename(path)[:-4], "%m-%d-%Y")
    if "Lat" in df.columns:
        df["Lat"] = df["Lat"].apply(lambda x: np.nan if x == 0 else x)
        df["Long"] = df["Long"].apply(lambda x: np.nan if x == 0 else x)
    df = df.apply(lambda x: x.str.strip() if x.dtype == 'O' else x)
    return df

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
            logging.info("Lat, Long not found for {}: {}".format(key, row[key]))
            continue
        daily_df.loc[ind, "Lat"] = mean_lat_long.loc[row[key]]["Lat"]
        daily_df.loc[ind, "Long"] = mean_lat_long.loc[row[key]]["Long"]


def get_us_admn2_feat(fips, shp):
    feats = [i for i in shp if str(i["properties"]["STATEFP"]) + str(i["properties"]["COUNTYFP"]) == fips]
    if len(feats) == 0:
        logging.info("NYT Data doesn't have matching for county with fips {}".format(fips))
        return None
    return feats[0]
