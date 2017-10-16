package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem2{
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("Problem2").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    
    val rdd = textFile.flatMap(_.split("[\n]+")) //Split by lines
    val result = rdd.map(x => x.split("\t")).map(x => (x(1),x(0).toInt)).sortBy(_._2).map(x=>(x._1,(x._2).toString)).reduceByKey((pre,after)=>(pre +","+ after)).sortBy(_._1).map(x=>(x._1+"\t"+x._2))
    //Swap the source and destination nodes ,reduce the key value pairs,sort them in order, then change the output format
    

    //result.foreach(println)
    result.saveAsTextFile(outputFolder)
  }
}