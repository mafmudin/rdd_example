from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
    externalDataset = sc.textFile("data.txt")
    print(externalDataset)