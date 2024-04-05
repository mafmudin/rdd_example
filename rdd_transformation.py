from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

    # map data with variable
    data = [1, 2, 3, 4, 5]
    distData = sc.parallelize(data, 5)
    mappedDistData = distData.map(lambda x: (x, 1)).collect()
    print(mappedDistData)

    # map data with external variable
    externalDataset = sc.textFile("data.txt")
    mappedExternalDataset = externalDataset.map(lambda x: (x, 1)).collect()
    print(mappedExternalDataset)

    # intersection
    otherData = sc.parallelize([5,6,7,8], 4)
    intersectionDistData = distData.intersection(otherData)
    print(intersectionDistData.collect())
