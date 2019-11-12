package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf4(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date", required = true)
  val text = opt[Boolean](descr = "plain-text data", required = false, default=Some(false))
  val parquet = opt[Boolean](descr = "parquet data", required = false, default=Some(false))
  requireOne(text, parquet)
  verify()
}

object Q4 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf4(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    if (args.text()){
      log.info("Running on Plain Text: " + args.text())
    } else if (args.parquet()){
      log.info("Running on Parquet: " + args.parquet())
    }


    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)
    val date = args.date()

    if (args.text()){
      // pain text data
      // where l_shipdata
      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
      val ordersRDD = sc.textFile(args.input() + "/orders.tbl")
      val customerRDD = sc.textFile(args.input() + "/customer.tbl")
      val nationRDD = sc.textFile(args.input() + "/nation.tbl")

      val custkey = customerRDD
        .map(line => {
          val tokens = line.split('|')
          // (cuskey, nationkey)
          (tokens(0),tokens(3))
        })
        .collectAsMap()
      val cuskeyHMap = sc.broadcast(custkey)


      val nationkey = nationRDD
        .map(line => {
          val tokens = line.split('|')
          // (nationkey, nationname)
          (tokens(0),tokens(1))
        })
        .collectAsMap()
      val nationkeyHMap = sc.broadcast(nationkey)

      val l_orderkey = lineitemRDD
          .filter(line => {
            val tokens = line.split('|')
            // get sepcific date
            tokens(10).contains(date)
          })
        .map(line => {
          val tokens = line.split('|')
          // (orderkey, (partkey, suppkey))
          (tokens(0), 1)
        })
        // add same oderkey together
        .reduceByKey(_ + _)

      val o_orderkey = ordersRDD
        .map(line => {
          val tokens = line.split('|')
          // (o_orderkey, custkey)
          (tokens(0),tokens(1))
        })

      l_orderkey.cogroup(o_orderkey)
        .filter(p => p._2._1.iterator.hasNext)
        // (nationkey, 1)
        .map (p => (cuskeyHMap.value(p._2._2.iterator.next()), p._2._1.iterator.next()))
        .reduceByKey(_ + _)
        // (nationkey, nationname, nationkeycount)
        .map(p => (p._1.toString.toInt, nationkeyHMap.value(p._1), p._2))
        .sortBy(_._1)
        .collect()
        .foreach(println)


    } else {
      // parquet data
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
      val customerDF = sparkSession.read.parquet(args.input() + "/customer")
      val customerRDD = customerDF.rdd
      val nationDF = sparkSession.read.parquet(args.input() + "/nation")
      val nationRDD = nationDF.rdd

      val custkey = customerRDD
        .map(line => {
          // (cuskey, nationkey)
          (line(0),line(3))
        })
        .collectAsMap()
      val cuskeyHMap = sc.broadcast(custkey)


      val nationkey = nationRDD
        .map(line => {
          // (nationkey, nationname)
          (line(0),line(1))
        })
        .collectAsMap()
      val nationkeyHMap = sc.broadcast(nationkey)

      val l_orderkey = lineitemRDD
        .filter(line => {
          // get sepcific date
          line(10).toString.contains(date)
        })
        .map(line => {
          // (orderkey, (partkey, suppkey))
          (line(0), 1)
        })
        // add same oderkey together
        .reduceByKey(_ + _)

      val o_orderkey = ordersRDD
        .map(line => {
          // (o_orderkey, custkey)
          (line(0),line(1))
        })

      l_orderkey.cogroup(o_orderkey)
        .filter(p => p._2._1.iterator.hasNext)
        // (nationkey, 1)
        .map (p => (cuskeyHMap.value(p._2._2.iterator.next()), p._2._1.iterator.next()))
        .reduceByKey(_ + _)
        // (nationkey, nationname, l_count)
        .map(p => (p._1.toString.toInt, nationkeyHMap.value(p._1), p._2))
        .sortBy(_._1)
        .collect()
        .foreach(println)
    }
  }
}
