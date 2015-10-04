
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.feature import HashingTF


if __name__ == "__main__":
    sc = SparkContext(appName="SpamAnalytcs")

    # Load 2 types of emails from text files.
    spam = sc.textFile("data/spam.txt")
    ham = sc.textFile("data/ham.txt")

    # Create a HashingTF 
    tf = HashingTF(numFeatures = 100)
    # Each email is split into words, and each word is mapped to one feature.
    spamFeatures = spam.map(lambda email: tf.transform(email.split(" ")))
    hamFeatures = ham.map(lambda email: tf.transform(email.split(" ")))

    # Create LabeledPoint 
    positiveExamples = spamFeatures.map(lambda features: LabeledPoint(1, features))
    negativeExamples = hamFeatures.map(lambda features: LabeledPoint(0, features))
    training_data = positiveExamples.union(negativeExamples)
    training_data.cache() # Cache data .

    # Run Logistic Regression using the SGD optimizer.
     model = LogisticRegressionWithSGD.train(training_data)

    # Test on a positive example (spam) and a negative one (ham).
    # First apply the same HashingTF feature transformation used on the training data.
    posTestExample = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
    negTestExample = tf.transform("Hi Papa, I started studying Big data, Spark the computing ...".split(" "))

    # Now use the learned model to predict spam/ham for new emails.
    print "Prediction for positive test example: %g" % model.predict(posTestExample)
    print "Prediction for negative test example: %g" % model.predict(negTestExample)

    sc.stop()
