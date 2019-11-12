package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date", required = true)
  val text = opt[Boolean](descr = "plain-text data", required = false, default=Some(false))
  val parquet = opt[Boolean](descr = "parquet data", required = false, default=Some(false))
  requireOne(text, parquet)
  verify()
}

object Q3 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf3(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    if (args.text()){
      log.info("Running on Plain Text: " + args.text())
    } else if (args.parquet()){
      log.info("Running on Parquet: " + args.parquet())
    }


    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)
    val date = args.date()

    if (args.text()){
      // pain text data
      // where l_shipdata
      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
      val partRDD = sc.textFile(args.input() + "/part.tbl")
      val supplierRDD = sc.textFile(args.input() + "/supplier.tbl")

      val partkey = partRDD
        .map(line => {
          val tokens = line.split('|')
          // (partkey, partname)
          (tokens(0),tokens(1))
        })
        .collectAsMap()
      val partkeyHMap = sc.broadcast(partkey)


      val supplekey = supplierRDD
        .map(line => {
          val tokens = line.split('|')
          // (supplekey, supplename)
          (tokens(0),tokens(1))
        })
        .collectAsMap()
      val supplekeyHMap = sc.broadcast(supplekey)

      val result = lineitemRDD
          .filter(line => {
            val tokens = line.split('|')
            // get sepcific date
            tokens(10).contains(date)
          })
        .map(line => {
          val tokens = line.split('|')
          // (orderkey, (partkey, suppkey))
          (tokens(0).toLong, (tokens(1), tokens(2)))
        })
        .sortByKey()
        .take(20)
        .map(p => (p._1, partkeyHMap.value(p._2._1), supplekeyHMap.value(p._2._2)))
        .foreach(println)

    } else {
      // parquet data
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val partDF = sparkSession.read.parquet(args.input() + "/part")
      val partRDD = partDF.rdd
      val supplierDF = sparkSession.read.parquet(args.input() + "/supplier")
      val supplierRDD = supplierDF.rdd

      val partkey = partRDD
        .map(line => {
          // (partkey, partname)
          (line(0),line(1))
        })
        .collectAsMap()
      val partkeyHMap = sc.broadcast(partkey)


      val supplekey = supplierRDD
        .map(line => {
          // (supplekey, supplename)
          (line(0),line(1))
        })
        .collectAsMap()
      val supplekeyHMap = sc.broadcast(supplekey)

      val result = lineitemRDD
        .filter(line => {
          // get sepcific date
          line(10).toString.contains(date)
        })
        .map(line => {
          // (orderkey, (partkey, suppkey))
          (line(0).toString.toLong, (line(1), line(2)))
        })
        .sortByKey()
        .take(20)
        .map(p => (p._1, partkeyHMap.value(p._2._1), supplekeyHMap.value(p._2._2)))
        .foreach(println)
    }
  }
}
