package com.deepera.ctg
/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable._

object SimpleApp {
  	def main(args: Array[String]) {
    	val logFile = "file:///home/liuhao/spark/README.md" // Should be some file on your system
    	val conf = new SparkConf().setAppName("Simple Application")
    	val sc = new SparkContext(conf)
    	val logData = sc.textFile(args(1), 2).cache()
    	val numAs = logData.filter(line => line.contains("a")).count()
    	val numBs = logData.filter(line => line.contains("b")).count()

    	println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))


  	}	
}