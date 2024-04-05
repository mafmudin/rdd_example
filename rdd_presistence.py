from pyspark import SparkContext, SparkConf, StorageLevel

if __name__ == '__main__':
    sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
    data = [1, 2, 3, 4, 5]
    distData = sc.parallelize(data, 5)
    distData.persist(StorageLevel.MEMORY_ONLY)
    print(distData.count())