package com.spark
import org.apache.spark.SparkContext  
import org.apache.spark.SparkContext._  
import org.apache.spark.SparkConf  
import org.apache.spark.sql.SQLContext

object WordCount {
  
  def main(args: Array[String]) {
    println("scala start");
    //countLetter();
    //visitHdfs();
    //estimatePi();
    visitDB();
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
  
  def visitHdfs (){
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
  
  def visitDB (){
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")  
    val sc = new SparkContext(conf);  
    val sqlContext = new SQLContext(sc);
    val url = "jdbc:postgresql://localhost:5432/meta?user=postgres&password=123456"
    val df = sqlContext.read.format("jdbc").option("url", url).option("dbtable", "sys_user").load()
    
    // Looks the schema of this DataFrame.
    df.printSchema()
    
    // Counts people by age
    val countsByAge = df.groupBy("password").count()
    countsByAge.show()
  }
}