import sys
import os
import shutil
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

    # print(data) # PythonRDD[2] at RDD at PythonRDD.scala:48
    # dataFilter3 = data.flatMap(lambda line: line.split(" ")).distinct() # flatMap: Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results.
    # dataFilter3.saveAsTextFile("demo4output")

    file2 = "demo.txt"
    if not os.path.isfile(file2):
        print("demo file is not exits!")

    # os.path.exists('demo.txt') or os.path.exits('demo5output') check if path or file exits
    # os.remove(file) # remove file
    # shutil.rmtree(outputDir) # remove directory
    output = "demo5output"
    if os.path.isdir(output):
        print("output directory has already exits!")
        shutil.rmtree(output)

    data2 = sc.textFile(file2).flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda v1, v2: v1 + v2)
    data2.saveAsTextFile(output)
    sc.stop()

