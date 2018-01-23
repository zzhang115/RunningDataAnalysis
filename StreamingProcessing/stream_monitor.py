from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

import time
import socket

class mini_monitor(object):
    def __init__(self, logfile):
        self.logfile = logfile
        self.error_cnt = 0

    def analyze(self):
        with open(self.logfile, 'r') as f:
            while True:
                # Remove the trailing \n
                line = f.readline().rstrip()
                if line == '' or line == '\n':
                    time.sleep(0.1)
                else:
                    self.on_data(line)

    def on_data(self, line):
        # Filter out the HEARTBEAT lines
        if 'HEARBEAT' in line:
            return

        # print(line.split('"'))
        # Access log / Error log?
        if line[0] == '[':
            self.on_error_log(line)
        else:
            self.on_access_log(line)
    def on_access_log(self, line):
        return

    def on_error_log(self, line):
        fields = line.split(']')
        fields = [x.strip(' [') for x in fields]
        lv = fields[1]

        if lv == 'ERROR':
            self.error_report(fields)
        return

    def error_report(self, fields):
        self.error_cnt += 1
        print('Total num of errors: ' + str(self.error_cnt))
        print('Error detail: ' + fields[-1])

def monitor(sc):
    # Initialize spark streaming context with a batch interval of 10 sec,
    # The messages would accumulate for 10 seconds and then get processed.
    ssc = StreamingContext(sc, batch_interval)

    # Receive the logs
    host = socket.gethostbyname(socket.gethostname())
    # Create a DStream that represents streaming data from TCP source
    socket_stream = ssc.socketTextStream(host, 5555)
    lines = socket_stream.window(window_time)
    # lines.pprint()
    lines.foreachRDD(lambda x: print(x.collect()))
    # Start the streaming process
    ssc.start()
    # time.sleep(window_time*5)
    process_cnt = 0
    while process_cnt < process_times:
        time.sleep(window_time)

        process_cnt += 1
    ssc.stop()

if __name__ == "__main__":
    # Define Spark configuration
    conf = SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("Stock Analysis")
    # Initialize a SparkContext
    sc = SparkContext(conf=conf)

    batch_interval = 10
    window_time = 10
    process_times = 1

    # Compute the whole time for displaying
    total_time = batch_interval * process_times

    monitor(sc)