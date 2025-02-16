from pyspark import SparkContext, SparkConf


# Initialize or get existing SparkContext
conf = SparkConf().setAppName("RDD Pipeline").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf=conf)

# Create an RDD
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rdd = sc.parallelize(data)

# Apply Transformations
rdd1 = rdd.map(lambda x: x * 2)            # Transformation 1: Map
rdd2 = rdd1.filter(lambda x: x % 2 == 0)   # Transformation 2: Filter
rdd3 = rdd2.flatMap(lambda x: [(x, x**2)]) # Transformation 3: FlatMap
rdd4 = rdd3.groupByKey()                   # Transformation 4: GroupByKey
rdd5 = rdd4.mapValues(list).mapValues(lambda x: sum(x)) # Transformation 5: ReduceByKey

# Execute an Action
result = rdd5.collect()
print(result)

# Stop the Spark Context
sc.stop()

