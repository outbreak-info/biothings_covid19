def get_customized_mapping(cls):
    mapping = {
        "confirmed_numIncrease": {
            "type": "float"
        },
        "dead_currentCases": {
            "type": "float"
        },
        "confirmed_currentPctIncrease": {
            "type": "float"
        },
        "admin_level": {
            "type": "integer"
        },
        "confirmed_newToday": {
            "type": "boolean"
        },
        "location_id": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "recovered_newToday": {
            "type": "boolean"
        },
        "first_dead-first_confirmed": {
            "type": "integer"
        },
        "recovered_currentPctIncrease": {
            "type": "float"
        },
        "confirmed_firstDate": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "confirmed": {
            "type": "float"
        },
        "confirmed_currentIncrease": {
            "type": "float"
        },
        "confirmed_currentToday": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "dead_currentIncrease": {
            "type": "float"
        },
        "dead_numIncrease": {
            "type": "float"
        },
        "recovered_currentIncrease": {
            "type": "float"
        },
        "dead_firstDate": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "iso3": {
            "type": "keyword"
        },
        "recovered_currentCases": {
            "type": "float"
        },
        "recovered": {
            "type": "float"
        },
        "recovered_firstDate": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "recovered_currentToday": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "dead": {
            "type": "float"
        },
        "dead_currentPctIncrease": {
            "type": "float"
        },
        "dead_newToday": {
            "type": "boolean"
        },
        "date": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "recovered_numIncrease": {
            "type": "float"
        },
        "dead_currentToday": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "confirmed_currentCases": {
            "type": "float"
        },
        "country_iso3": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "state_iso3": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "long": {
            "type": "float"
        },
        "lat": {
            "type": "float"
        },
        "country_population": {
            "type": "float"
        },
        "name": {
            "type": "keyword"
        },
        "wb_region": {
            "type": "keyword"
        },
        "population": {
            "type": "float"
        },
        "country_name": {
            "type": "keyword"
        },
        "num_subnational": {
            "type": "integer"
        },
        "state_name": {
            "type": "keyword"
        }
    }
    return mapping
