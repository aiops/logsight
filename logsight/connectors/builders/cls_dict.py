from logsight.connectors import sinks, sources

cls_conn = {"source": {"kafka": sources.KafkaSource,
                       "socket": sources.SocketSource,
                       "file": sources.FileSource,
                       "stdin": sources.StdinSource,
                       "zeromq": sources.ZeroMQSubSource
                       },
            "sink": {"kafka": sinks.KafkaSink,
                     "socket": sinks.SocketSink,
                     "file": sinks.FileSink,
                     "stdout": sinks.PrintSink,
                     "elasticsearch": sinks.ElasticsearchSink,
                     "zeromq": sinks.ZeroMQPubSink
                     }}
