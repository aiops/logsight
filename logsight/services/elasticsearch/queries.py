GET_ALL_AD = {

    "index"   : "$index",
    "doc_type": '_doc',
    "body"    : {
        "query": {
            "bool": {
                "must"  : [
                    {
                        "range": {
                            "@timestamp": {
                                "format": "strict_date_optional_time",
                                "gte"   : "$start_time",
                                "lte"   : "$end_time"
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
