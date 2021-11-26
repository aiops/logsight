import multiprocessing
import threading
from time import sleep, time

from connectors.source import PrintSource, FileSource
from connectors.sink import PrintSink

from modules.log_parsing import ParserModule
from modules.anomaly_detection import AnomalyDetectionModule

#src = PrintSource()
#src_data = PrintSource()
#sink = PrintSink()
#sink_data = PrintSink()


### PARSE TEST
# class StateConfigs:
#     buffer_size = 3
#     retrain_after = 2
#
#
# class Configs:
#     timeout_period = 20000
#     state_configs = StateConfigs()
#
#
# configs = Configs()
# module = ParserModule(src, sink, src_data, sink_data, configs)
#
# module.run()


### AD TEST
#class Configs:
#    buffer_size = 3
#    timeout_period = 2


#src_data = FileSource('logfile.txt')

#module = AnomalyDetectionModule(src_data, sink_data, src, sink, Configs())
from modules.anomaly_detection.log_anomaly_detection import LogAnomalyDetector
# ad = LogAnomalyDetector()
# ad.load_model(None,'test')
#module.run()

q = multiprocessing.Manager().Queue(10000)

def read():
    i = 0
    t = time()
    while True:
        if q.empty():
            print("Empty")
            sleep(1)
        q.get()
        i+=1
        if i%1000 == 0:
            print(f"Read {i} messages in {time() - t} seconds")

def write():
    i = 0
    t = time()
    while True:
        if q.full():
            print("Empty")
            sleep(1)
        q.put("Test")
        i += 1
        if i % 1000 == 0:
            print(f"Written {i} messages in {time() - t} seconds")

t1 = threading.Thread(target=read)
t1.start()

t2 = threading.Thread(target=write())
t2.start()

t1.join()
t2.join()
