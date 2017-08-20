import sys
from pyspark import SparkContext

if __name__ == '__main__':
    file = sys.argv[1]
    sc = SparkContext(appName="demo")
    data = sc.textFile(file).map(lambda line: line.upper())
    # data.saveAsTextFile("demo1output")
    # dataFilter = data.filter(lambda line : line.startswith("HI"))
    # dataFilter.saveAsTextFile("demo2output")

    # dataFilter2 = data.flatMap(lambda line: line.split(" "))
    # dataFilter2.saveAsTextFile("demo3output")

    dataFilter3 = data.flatMap(lambda line: line.split(" ")).distinct()
    dataFilter3.saveAsTextFile("demo4output")
    sc.stop()

