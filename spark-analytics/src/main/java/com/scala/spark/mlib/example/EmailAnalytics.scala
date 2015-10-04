package com.scala.spark.mllib.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint

object EmailAnalytics {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("EmailAnalytics")
    val sc = new SparkContext(conf)

    
    val spam = sc.textFile("data/spam.txt")
    val ham = sc.textFile("daa/ham.txt")

    // Create a HashingTF 
    val tf = new HashingTF(numFeatures = 100)
    
    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val hamFeatures = ham.map(email => tf.transform(email.split(" ")))

    // Create LabeledPoint
    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    val negativeExamples = hamFeatures.map(features => LabeledPoint(0, features))
    val trainingData = positiveExamples ++ negativeExamples
    trainingData.cache() 

    // Create a Logistic Regression learner
    val lrLearner = new LogisticRegressionWithSGD()
    val model = lrLearner.run(trainingData)

   
    val posTestExample = tf.transform("Special Deal for you to get more money ...".split(" "))
    val negTestExample = tf.transform("Hello Mom, i will be coming home this durga puja ...".split(" "))
    // Now use the learned model to predict spam/ham for new emails.
    println(s"Prediction for positive test example: ${model.predict(posTestExample)}")
    println(s"Prediction for negative test example: ${model.predict(negTestExample)}")

    sc.stop()
  }
}
