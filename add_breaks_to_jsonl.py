import json
import configparser
import os
from datetime import datetime as dt

# Get paths from config
config = configparser.ConfigParser()
config.read("config.ini")

data_json_path = config["output"]["out_json_path"] + "l"
json_out_path = data_json_path.replace(".json", "_breaks.json")
breaks_json_path = os.path.join(config["GIF output"]["gif_output"], "breaks-{}.json".format(dt.now().strftime("%Y-%m-%d")))

print("Read biothings items from {}.".format(data_json_path))
print("Read breaks json from {}.".format(breaks_json_path))

def load_json_lines(path):
    with open(path) as infile:
        for line in infile:
            yield json.loads(line)
        infile.close()

breaks_json = {}
with open(breaks_json_path) as json_file:
    breaks_json = json.load(json_file)


print("Adding breaks .. ")
# Add breaks_json based n admin_level
admin_level_id_map = {
    0: "admin0",
    1: "US_states",
    2: "US_counties",
    1.5: "US_metros"
}
with open(json_out_path, "w") as outfile:
    for rec in load_json_lines(data_json_path):
        if rec["admin_level"] not in admin_level_id_map.keys():
            json.dump(rec, outfile)
            outfile.write("\n")
            continue
        _breaks = next(i for i in breaks_json if i["id"] == admin_level_id_map[rec["admin_level"]])
        if _breaks == None:
            json.dump(rec, outfile)
            outfile.write("\n")
            continue
        for k,v in _breaks.items():
            if "breaks" not in k:
                continue
            rec[k] = v
        json.dump(rec, outfile)
        outfile.write("\n")
    outfile.close()

print("JSON with breaks written to {}.".format(json_out_path))
