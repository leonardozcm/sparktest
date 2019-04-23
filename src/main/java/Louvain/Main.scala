package Louvain

import org.apache.spark.graphx.Edge
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created with IntelliJ IDEA.
  * Created by @author ChangMin Zhao 
  * Description: 
  * User: 59771
  * Date: 2018-08-13
  * Time: 19:17
  * Usage: sparktest
  */
object Main {
  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("My App"))
  val sparkexam: SparkSession = SparkSession
    .builder()
    .config("spark.debug.maxToStringFields", "10000000")
    .appName("My App")
    .getOrCreate()
  val filedir = "src//main//resources//"

  def main(args: Array[String]): Unit = {
    var edgeRDD = sc.textFile(filedir + "test.tsv").map(row => {
  val token = row.split(" ").map(_.trim)
      token.length match {
        case 2=>{new Edge()}
      }
    })

  }
}
