from pyspark import SparkConf, SparkContext

# Initialize Spark Context
conf = SparkConf().setAppName("Text Data Analysis").setMaster("local[*]")  # Replace local[*] with the master URL in cluster mode
sc = SparkContext(conf=conf)

# Load and preprocess data
text_file = sc.textFile("hdfs://path_to_your_data/*.txt")  # Use a local or HDFS path
words = text_file.flatMap(lambda line: line.lower().split()) \
                 .map(lambda word: ''.join(filter(str.isalnum, word)))  # Remove punctuation

# MapReduce steps
word_counts = words.map(lambda word: (word, 1)) \
                   .reduceByKey(lambda a, b: a + b)

# Sort and save results
sorted_word_counts = word_counts.sortBy(lambda x: x[1], ascending=False)
sorted_word_counts.saveAsTextFile("hdfs://path_to_output/word_counts")

print("Word count completed!")
sc.stop()
