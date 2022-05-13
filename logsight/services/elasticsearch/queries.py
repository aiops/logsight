GET_ALL_AD = {

    "index": "$index",
    "body": {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "timestamp": {
                                "format": "strict_date_optional_time",
                                "gt": "$start_time",
                                "lte": "$end_time"
                            }
                        }
                    }
                ],
                "filter": [
                    {
                        "match_all": {}
                    }
                ]
            }
        }
    }}
