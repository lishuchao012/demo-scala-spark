package com.spark
import org.apache.spark.SparkContext  
import org.apache.spark.SparkContext._  
import org.apache.spark.SparkConf  

object WordCount {
  
  def main(args: Array[String]) {
    println("scala start");
    //countLetter();
    //callHdfs();
    estimatePi();
  }
  
  def countLetter (){
    val logFile = "README" 
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]") 
    val sc = new SparkContext(conf)  
    val logData = sc.textFile(logFile, 2).cache()  
    val numAs = logData.filter(line => line.contains("a")).count()  
    val numBs = logData.filter(line => line.contains("b")).count()  
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))  
  }
  
  def callHdfs (){
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]") 
    val sc = new SparkContext(conf) 
    val textFile = sc.textFile("hdfs://localhost:9000//user/craw.sql")
    val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.saveAsTextFile("hdfs://localhost:9000//user/craw2")
  }
  
  def estimatePi (){
    val NUM_SAMPLES = 1000;
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]") 
    val sc = new SparkContext(conf) 
    val count = sc.parallelize(1 to NUM_SAMPLES).filter { _ =>
      val x = math.random
      val y = math.random
      x*x + y*y < 1
    }.count()
    println(s"Pi is roughly ${4.0 * count / NUM_SAMPLES}")
  }
}