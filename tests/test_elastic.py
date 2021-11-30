from connectors.sink import ElasticsearchSink

conn_params = {"host": "localhost",
               "port": 9200,
               "username": "elastic",
               "password": "elasticsearchpassword"
               }
sink = ElasticsearchSink(**conn_params,index="test")
sink.es.ping()
sink.send("data")
print(sink)