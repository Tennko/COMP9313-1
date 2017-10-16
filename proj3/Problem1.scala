package comp9313.ass3


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.Map
import scala.collection.mutable.Set
import scala.collection.mutable.ArrayBuffer

object Problem1 {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFolder = args(1)
    val k = args(2).toInt
    
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    
    var words = textFile.zipWithIndex.map(_.swap).flatMapValues(_.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+"))
    
    words = words.map(x=>(x._1,x._2.toLowerCase()))
    words = words.flatMapValues("[a-zA-Z]+".r findAllIn _) //Filtering required words
    val words2 = words.groupBy(_._1).mapValues(_.map(_._2))
    val words3 = words2.map(x=>(x._2).toArray)
    //words3.foreach(println)
    val buffer = new ArrayBuffer[String]()
    
    words3.collect().foreach(x =>
    { var set = scala.collection.mutable.Set.empty[String]
      for (i<-0 to x.length-1){
    
      set.add(x(i))  //Use a set to record words occured in a line 
      
    } 
      for (x<-set){buffer +=(x)} //Put all the words in a buffer
    }
    )
    
    val rdd = sc.parallelize(buffer)
    val wordcount = rdd.map(x=>(x,1)).reduceByKey(_+_).sortBy(-_._2) // map and reduce
    
    val topk = wordcount.take(k)   // Take first k words
    val result = topk.map(x=>(x._1+"\t"+x._2)) //Change output format
    //result.foreach(println)
    
    sc.makeRDD(result).saveAsTextFile(outputFolder)
    
    
    
  }
}