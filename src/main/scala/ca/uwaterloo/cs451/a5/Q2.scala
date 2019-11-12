package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date", required = true)
  val text = opt[Boolean](descr = "plain-text data", required = false, default=Some(false))
  val parquet = opt[Boolean](descr = "parquet data", required = false, default=Some(false))
  requireOne(text, parquet)
  verify()
}

object Q2 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    if (args.text()){
      log.info("Running on Plain Text: " + args.text())
    } else if (args.parquet()){
      log.info("Running on Parquet: " + args.parquet())
    }


    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)
    val date = args.date()

    if (args.text()){
      // pain text data
      // where l_shipdata
      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
      val ordersRDD = sc.textFile(args.input() + "/orders.tbl")

      val orderkey = lineitemRDD
        .filter(line => {
          val tokens = line.split('|')
          tokens(10).toString.contains(date)
        })
          .map(line => {
            val tokens = line.split('|')
            // order number
            (tokens(0),1)
          })
      val clerks = ordersRDD
          .map(line => {
            val tokens = line.split('|')
            // order number and clerks name
            (tokens(0), tokens(6))
          })
      var result = orderkey.cogroup(clerks)
          .filter(p => p._2._1.iterator.hasNext)
          .map(p => (p._2._2.iterator.next(), p._1.toString.toLong))
          .sortBy(_._2)
      result.take(20)
          .foreach(println)

    } else {
      // parquet data
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val orderDF = sparkSession.read.parquet(args.input() + "/orders")
      val orderRDD = orderDF.rdd

      val orderkey = lineitemRDD
        .filter(line => {
          line(10).toString.contains(date)
        })
        .map(line => (line(0), 1))

      val clerks = orderRDD.map(line => (line(0), line(6)))
      var result = orderkey.cogroup(clerks)
        .filter(p => p._2._1.iterator.hasNext)
        .map(p => (p._2._2.iterator.next(), p._1.toString.toLong))
        .sortBy(_._2)
      result.take(20)
        .foreach(println)
    }
  }
}
