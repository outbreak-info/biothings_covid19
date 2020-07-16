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
    feats = [i for i in shp if str(i["properties"]["STATEFP"]) + str(i["properties"]["COUNTYF"]) == fips]
    if len(feats) == 0:
        logging.info("NYT Data doesn't have matching for county with fips {}".format(fips))
        return None
    return feats[0]

def fix_daily_reports(daily_df, admn0_shp)
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

