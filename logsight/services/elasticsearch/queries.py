GET_ALL_AD = {

    "index": "$index", "body": {"query": {"bool": {"must": [
        {"range": {"timestamp": {"format": "strict_date_optional_time", "gt": "$start_time", "lte": "$end_time"}}}],
        "filter": [{"match_all": {}}]}}}}

GET_ALL_TEMPLATES = {

    "index": "$index", "body": {
        "aggs": {"aggregations": {"terms": {"field": "template.keyword", "order": {"_count": "desc"}, "size": 1000}}},
        "fields": [{"field": "timestamp", "format": "date_time"}], "script_fields": {}, "stored_fields": ["*"],
        "runtime_mappings": {}, "_source": {"excludes": []}, "query": {"bool": {"filter": [
            {"range": {"timestamp": {"format": "strict_date_optional_time", "gt": "now-1y", "lte": "$end_time"}}}]}}}}
