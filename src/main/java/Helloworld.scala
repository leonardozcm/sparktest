import java.io.{StringReader, StringWriter}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.opencsv.{CSVReader, CSVWriter}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created with IntelliJ IDEA.
  * Created by @author ChangMin Zhao 
  * Description: 
  * User: 59771
  * Date: 2018-07-04
  * Time: 9:39
  * Usage: sparktest
  */
object Helloworld {
  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("My App"))
  val sparkexam: SparkSession = SparkSession
    .builder()
    .config("spark.debug.maxToStringFields", "10000000")
    .appName("My App")
    .getOrCreate()
  val filedir = "src//main//resources//"

  def main(args: Array[String]) {
    accumulatortest()
  }

  def countsAvg(array: Array[Int]): Unit = {
    val input = sc.parallelize(array)
    val result = input.aggregate((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
    println(result._1 / result._2)
  }

  def countWords(): Unit = {
    val input = sc.textFile(filedir + "inputfile")
    val words = input.flatMap(line => line.split("."))
    val counts = words.map(word => (word, 1)).reduceByKey((x, y) => x + y)
    counts.saveAsTextFile(filedir + "outfile")
  }

  def countsquare(x: List[Int]): Unit = {
    val input = sc.parallelize(x)
    val squared = input.map(x => x * x)
    println(squared.collect().mkString(","))
  }

  def pageRank(): Unit = {
    val input = sc.textFile(filedir + "pagelinks")
    val links = input.map(line => (line.split(":")(0), line.split(":")(1).split(","))).persist()
    var ranks = links.mapValues(v => 1.0)
    for (i <- 0 until 10) {
      val contributions = links.join(ranks).flatMap {
        case (pageID, (link, rank))
        => link.map(dest => (dest, rank / link.size))
      }
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(x => 0.15 + 0.85 * x)
    }
    ranks.saveAsTextFile(filedir + "pagerankresult")
  }

  /**
    * json的mapper要调用mapper.registerModule(DefaultScalaModule)，指明是scala类不是java类
    *
    */
  def jsonTest(): Unit = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    println(mapper.writeValueAsString(new Person("zcm", 58)))
  }

  /** 写匿名函数注意，如果那一行不是定义变量则必然有返回值，会导致返回后的比如println运行不到
    * 对Array[]可以用map，map与foreach不同在于map有返回值，foreach没有（Unit）
    * mapPartitions对迭代器函数作用，map对元素函数作用
    * 这个函数用于读取csv，将里面的值全部加一，再输出
    */
  def csvTest(): Unit = {
    val input = sc.textFile(filedir + "csvtest.csv")

    val res: Unit = input.map { line
    =>
      val reader = new CSVReader(new StringReader(line))
      reader.readNext().map {
        lines =>
          (lines.toInt + 1) + ""
      }
    }.mapPartitions {
      str =>
        val stringwriter = new StringWriter()
        val cSVWriter = new CSVWriter(stringwriter)
        cSVWriter.writeAll(str.toList)
        Iterator(stringwriter.toString)

    }.saveAsTextFile(filedir + "csvres")
  }

  /**
    * Text\Intwritable在hadoop.io包里
    */
  def seqTest(): Unit = {
    val data = sc.parallelize(List(("zcm", 21), ("sdas", 8), ("csniado", 67)))
    data.saveAsSequenceFile(filedir + "seqres")
    sc.sequenceFile(filedir + "seqres", classOf[Text], classOf[IntWritable])
      .map { case (x, y) => (x.toString, y.get() + "zcmsac") }.saveAsTextFile(filedir + "seqres2")
  }

  /**
    * 原书中用的accumulator已将被弃用，改用longaccumulator
    */
  def accumulatortest(): Unit = {
    val input = sc.textFile(filedir + "inputfile")
    val blanklines = sc.longAccumulator
    var counter = 1
    input.foreach {
      line =>
        if (line == "") {
          blanklines.add(1)
          counter=counter+1
        }
    }
    println("blanlines" + blanklines.value+",counter ="+counter)
  }

  /**
    * spark2.1引入的sparksession，从功能上可以取代sc
    * SparkConf、SparkContext和SQLContext都已经被封装在SparkSession当中
    */
  def accumlator_and_sparksessiontest(): Unit = {
    val testweets = sparkexam.read.textFile(filedir + "inputfile")
    val blanklines = sparkexam.sparkContext.longAccumulator
    testweets.foreach {
      line =>
        if (line == "") {
          blanklines.add(1)
        }
    }
    println("blanlines" + blanklines.value)
  }

  /**
    * 用registerTempTable是可以，而用creatcreateOrReplaceTempView是官网推荐的
    * 而createOrReplaceGlobalTempView()是得不到的，查看源码可以知道
    * 他们调用了createTempViewCommand同一个api，只是在global上一个是true一个是false
    */
  def sqljsontest() = {
    val zipsDF = sparkexam.read.json(filedir + "testweet.jsonl")
    zipsDF.createOrReplaceTempView("tweet")
    // zipsDF.createOrReplaceGlobalTempView()
    val idDF = zipsDF.sqlContext.sql("SELECT id FROM tweet")
    idDF.show()
  }

  /**
    * 这没有使用书上的隐式转换，而是调用sqlcontext的createdataframe来创建
    * schemardd，然后调用sql的api
    * 测试udf时发现书上也出现了令人失望的明显错误，google后找到正常的格式
    */
  def sqlRDDtest(): Unit = {
    val personrdd = sparkexam.sparkContext.
      parallelize(List(Person("zcm", 87), Person("sd", 7), Person("weido", 23)))
    val personschemadd = sparkexam.sqlContext.createDataFrame(personrdd)
    personschemadd.createOrReplaceTempView("person")
    personschemadd.sqlContext.sql("select name from person").show(2)

    //UDF测试
    sparkexam.udf.register("twoyearspasses", (y: Int) => y + 2
    )
    sparkexam.sql("select twoyearspasses(age) from person").show()
  }


}