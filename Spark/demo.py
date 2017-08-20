import sys
from pyspark import SparkContext

if __name__ == '__main__':
    file = sys.argv[1]
    sc = SparkContext(appName="demo")
    data = sc.textFile(file).map(lambda line: line.upper())
    # data.saveAsTextFile("demo1output")
    dataFilter = data.filter(lambda line : line.startswith("HI"))
    dataFilter.saveAsTextFile("demo2output")
    sc.stop()

