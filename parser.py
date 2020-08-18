import os
import json
import gzip
import numpy as np

from biothings import config
logging = config.logger

def load_annotations(data_folder):
    json_path = os.path.join(data_folder,"biothings_items.json.gz")
    items = []
    with gzip.open(json_path) as f:
        json_bytes = f.read()
        json_str = json_bytes.decode("utf-8")
        items = json.loads(json_str)
        f.close()
    for item in items:
        for k,v in item.items():
            if type(v) == np.int64:
                item[k] = int(v)
            if type(v) == np.float64 or type(v) == np.float:
                item[k] = float(v)
        yield item
