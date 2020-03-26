import os
import json

from biothings import config
logging = config.logger

def load_annotations(data_folder):
    json_path = os.path.join(data_folder,"biothings_items.json")
    items = []
    with open(json_path) as f:
        items = json.load(f)
        f.close()
    for item in items:
        for k,v in item.items():
            if type(v) == np.int64:
                item[k] = int(v)
            if type(v) == np.float64 or type(v) == np.float:
                item[k] = float(v)
        yield item
