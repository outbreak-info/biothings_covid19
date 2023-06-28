import os
import json
import gzip

from biothings.utils.dataload import to_number

def load_annotations(data_folder):
    json_path = os.path.join(data_folder,"epi_data.jsonl.gz")
    with gzip.open(json_path) as f:
        for line in f:
            # TODO: use biothings.utils.serializer import load_json when upgrade biothings.api
            item = json.loads(line)
            for k,v in item.items():
                item[k] = to_number(v)
            yield item
