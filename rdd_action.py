from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
    data = [1, 2, 3, 4, 5]
    distData = sc.parallelize(data, 5)
    print(distData.first())
    print(distData.count())
    print(distData.take(3))