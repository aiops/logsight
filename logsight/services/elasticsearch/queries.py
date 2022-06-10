GET_ALL_AD = {

    "index": "$index", "body": {"query": {"bool": {"must": [
        {"range": {"timestamp": {"format": "strict_date_optional_time", "gt": "$start_time", "lte": "$end_time"}}}],
        "filter": [{"match_all": {}}]}}}}

GET_ALL_TEMPLATES = {

    "index": "$index", "body": {
        "aggs": {"aggregations": {"terms": {"field": "template.keyword", "order": {"_count": "desc"}, "size": 10000}}},
        "fields": [{"field": "ingest_timestamp", "format": "date_time"}], "script_fields": {}, "stored_fields": ["*"],
        "runtime_mappings": {}, "_source": {"excludes": []}, "query": {"bool": {"filter": [
            {"range": {
                "ingest_timestamp": {"format": "strict_date_optional_time", "gt": "now-15y", "lte": "now"}}}]}}}}

GET_ALL_LOGS_INGEST = {
    "index": "$index", "body": {"query": {"bool": {"must": [
        {"range": {"ingest_timestamp": {"format": "strict_date_optional_time", "gt": "$start_time"}}}],
        "filter": [{"match_all": {}}]}}}}
