import sys
from pyspark import SparkContext

if __name__ == '__main__':
    file = sys.argv[1]
    sc = SparkContext(appName="demo")
    data = sc.textFile(file).map(lambda line: line.upper())
    data.saveAsTextFile("demo1output")
    sc.stop()

