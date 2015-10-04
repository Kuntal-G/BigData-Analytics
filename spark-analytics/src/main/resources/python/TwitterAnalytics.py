from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, Row
from pyspark.sql.types import IntegerType
import json
import sys

if __name__ == "__main__":
    inputFile = sys.argv[1]
    conf = SparkConf().setAppName("TwitterAnalytics")
    sc = SparkContext()
    hiveCtx = HiveContext(sc)
    print "Loading tweets from " + inputFile
    input = hiveCtx.jsonFile(inputFile)
    input.registerTempTable("tweets")
    topTweets = hiveCtx.sql("SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10")
    print topTweets.collect()
    topTweetText = topTweets.map(lambda row : row.text)
    print topTweetText.collect()
    # Make a happy person row
    happyPeopleRDD = sc.parallelize([Row(name="ganguly", favouriteBeverage="coffee")])
    happyPeopleSchemaRDD = hiveCtx.inferSchema(happyPeopleRDD)
    happyPeopleSchemaRDD.registerTempTable("strong_people")
    # Make a UDF to tell us how long some text is
    hiveCtx.registerFunction("strLenPython", lambda x: len(x), IntegerType())
    lengthSchemaRDD = hiveCtx.sql("SELECT strLenPython('text') FROM tweets LIMIT 10")
    print lengthSchemaRDD.collect()
    sc.stop()