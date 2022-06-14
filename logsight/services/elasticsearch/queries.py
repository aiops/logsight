_index_placeholder = "$index"
_start_time_placeholder = "$start_time"
_end_time_placeholder = "$end_time"

GET_ALL_AD = {

    "index": _index_placeholder, "body": {
        "sort": [
            {"timestamp": {"order": "asc"}}
        ], "query": {"bool": {"must": [{"range": {
            "timestamp": {"format": "strict_date_optional_time", "gt": _start_time_placeholder,
                          "lte": _end_time_placeholder}}}], "filter": [{"match_all": {}}]}}}}

GET_ALL_TEMPLATES = {

    "index": _index_placeholder, "body": {
        "aggs": {"aggregations": {"terms": {"field": "template.keyword", "order": {"_count": "desc"}, "size": 10000}}},
        "fields": [{"field": "ingest_timestamp", "format": "date_time"}], "script_fields": {}, "stored_fields": ["*"],
        "runtime_mappings": {}, "_source": {"excludes": []},
        "sort": [
            {"timestamp": {"order": "asc"}}
        ], "query": {"bool": {"filter": [
            {"range": {"ingest_timestamp": {"format": "strict_date_optional_time", "gt": "now-15y", "lte": "now"}}}]}}}}

GET_ALL_LOGS_INGEST = {"index": _index_placeholder, "body": {
    "sort": [
        {"timestamp": {"order": "asc"}}
    ], "query": {"bool": {
        "must": [
            {"range": {"ingest_timestamp": {"format": "strict_date_optional_time", "gt": _start_time_placeholder}}}],
        "filter": [{"match_all": {}}]}}}}

DELETE_BY_QUERY = {"index": '$index', "body": {
     "query": {"bool": {"must": [{"range": {
        "timestamp": {"format": "strict_date_optional_time", "gte": _start_time_placeholder,
                      "lte": _end_time_placeholder}}}], "filter": [{"match_all": {}}]}}}}
