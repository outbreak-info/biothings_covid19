import json
import configparser
import os
from datetime import datetime as dt

# Get paths from config
config = configparser.ConfigParser()
config.read("config.ini")

data_json_path = config["output"]["out_json_path"]
breaks_json_path = os.path.join(config["GIF output"]["gif_output"], "breaks-{}.json".format(dt.now().strftime("%Y-%m-%d")))

data_json = {}
with open(data_json_path) as json_file:
    data_json = json.load(json_file)

print("Read biothings items from {}.".format(data_json_path))

breaks_json = {}
with open(breaks_json_path) as json_file:
    breaks_json = json.load(json_file)

print("Read breaks json from {}.".format(breaks_json_path))

print("Adding breaks .. ")
# Add breaks_json based n admin_level
admin_level_id_map = {
    0: "admin0",
    1: "US_states",
    2: "US_counties",
    1.5: "US_metros"
}
for rec in data_json:
    if rec["admin_level"] not in admin_level_id_map.keys():
        continue
    _breaks = next(i for i in breaks_json if i["id"] == admin_level_id_map[rec["admin_level"]])
    if _breaks == None:
        continue
    for k,v in _breaks.items():
        if "breaks" not in k:
            continue
        rec[k] = v

# Write to file
with open(data_json_path, "w") as outfile:
    json.dump(data_json, outfile)

print("JSON with breaks written to {}.".format(data_json_path))
