def get_customized_mapping(cls):
    mapping = {
        "date": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "lat": {
            "type": "float"
        },
        "long": {
            "type": "float"
        },
        "location_id": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "admin1": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "admin2": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "admin_level": {
            "type": "float"
        },
        "geometry": {
            "properties": {
                "type": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                },
                "coordinates": {
                    "type": "float"
                },
            }
        },
        "confirmed_per_100k_breaks": {
            "type":"float"
        },
        "confirmed_rolling_14days_ago_diff_breaks": {
            "type":"float"
        },
        "confirmed_rolling_14days_ago_diff_per_100k_breaks": {
            "type":"float"
        },
        "confirmed_rolling_breaks": {
            "type":"float"
        },
        "confirmed_rolling_per_100k_breaks": {
            "type":"float"
        },
        "dead_per_100k_breaks": {
            "type":"float"
        },
        "dead_rolling_14days_ago_diff_breaks": {
            "type":"float"
        },
        "dead_rolling_14days_ago_diff_per_100k_breaks": {
            "type":"float"
        },
        "dead_rolling_breaks": {
            "type":"float"
        },
        "dead_rolling_per_100k_breaks": {
            "type":"float"
        },
        "mostRecent": {
            "type": "boolean"
        },
        "confirmed": {
            "type": "float"
        },
        "confirmed_rolling": {
            "type": "float"
        },
        "confirmed_rolling_14days_ago": {
            "type": "float"
        },
        "confirmed_rolling_14days_ago_diff": {
            "type": "float"
        },
        "confirmed_doublingRate": {
            "type": "float"
        },
        "confirmed_numIncrease": {
            "type": "float"
        },
        "confirmed_pctIncrease": {
            "type": "float"
        },
        "dead": {
            "type": "float"
        },
        "dead_rolling": {
            "type": "float"
        },
        "dead_rolling_14days_ago": {
            "type": "float"
        },
        "dead_rolling_14days_ago_diff": {
            "type": "float"
        },
        "dead_numIncrease": {
            "type": "float"
        },
        "dead_pctIncrease": {
            "type": "float"
        },
        "dead_doublingRate": {
            "type": "float"
        },
        "population": {
            "type": "integer"
        },
        "num_subnational": {
            "type": "integer"
        },
        "country_population": {
            "type": "integer"
        },
        "iso3": {
            "type": "keyword"
        },
        "name": {
            "type": "keyword",
            "fields": {
                "lower": {
                    "type": "keyword",
                    "normalizer": "keyword_lowercase_normalizer"
                }
            }
        },
        "country_name": {
            "type": "keyword"
        },
        "wb_region": {
            "type": "keyword"
        },
        "confirmed_rolling_per_100k": {
            "type": "float"
        },
        "dead_rolling_per_100k": {
            "type": "float"
        },
        "confirmed_per_100k": {
            "type": "float"
        },
        "dead_per_100k": {
            "type": "float"
        },
    }
    return mapping
