from pyspark import SparkContext as sc, SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("app-name-of-your-choice").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    data = [1, 2, 3, 4, 5]
    distData = sc.parallelize(data, 5)
    print(distData)