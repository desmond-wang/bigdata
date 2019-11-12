package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf5(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val text = opt[Boolean](descr = "plain-text data", required = false, default=Some(false))
  val parquet = opt[Boolean](descr = "parquet data", required = false, default=Some(false))
  requireOne(text, parquet)
  verify()
}

object Q5 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf5(argv)

    log.info("Input: " + args.input())
    if (args.text()){
      log.info("Running on Plain Text: " + args.text())
    } else if (args.parquet()){
      log.info("Running on Parquet: " + args.parquet())
    }


    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)

    if (args.text()){
      // pain text data
      // where l_shipdata
      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
      val ordersRDD = sc.textFile(args.input() + "/orders.tbl")
      val customerRDD = sc.textFile(args.input() + "/customer.tbl")
      val nationRDD = sc.textFile(args.input() + "/nation.tbl")

      // nation key as primary key for each nation
      val ca = 3
      val us = 24

      val custkey = customerRDD
        .map(line => {
          val tokens = line.split('|')
          // (cuskey, nationkey)
          (tokens(0),tokens(3).toInt)
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
        .map(line => {
          val tokens = line.split('|')
          // (orderkey, shipdate)
          (tokens(0), tokens(10))
        })

      // order from us and ca
      val o_orderkey = ordersRDD
        .map(line => {
          val tokens = line.split('|')
          // (o_orderkey, custkey)
          (tokens(0),tokens(1))
        })
          .filter(p => {
            val nationkey = cuskeyHMap.value(p._2)
            nationkey == ca || nationkey == us
          })

      //(orderkey, (date, custkey))
      l_orderkey.cogroup(o_orderkey)
        .filter(p => p._2._2.iterator.hasNext)
        .flatMap(p => {
          val custkey = p._2._2.iterator.next()
          // sperate each month
          // ((name, month), 1)
          p._2._1.map(date => ((cuskeyHMap.value(custkey), date.slice(0,7)), 1))
        })
        .reduceByKey(_ + _)
        .sortByKey()
        .collect()
        .foreach(p => println("(" + p._1._1 + "," + p._1._2 + "," + p._2 + ")"))

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

      // nation key as primary key for each nation
      val ca = 3
      val us = 24

      val custkey = customerRDD
        .map(line => {
          // (cuskey, nationkey)
          (line(0),line(3).toString.toInt)
        })
        .collectAsMap()
      val cuskeyHMap = sc.broadcast(custkey)


      val nationkey = nationRDD
        .map(line => {
          // (nationkey, nationname)
          (line(0).toString.toInt,line(1))
        })
        .collectAsMap()
      val nationkeyHMap = sc.broadcast(nationkey)

      val l_orderkey = lineitemRDD
        .map(line => {
          // (orderkey, shipdate)
          (line(0), line(10))
        })

      // order from us and ca
      val o_orderkey = ordersRDD
        .map(line => {
          // (o_orderkey, custkey)
          (line(0),line(1))
        })
        .filter(p => {
          val nationkey = cuskeyHMap.value(p._2)
          nationkey == ca || nationkey == us
        })

      //(orderkey, (date, custkey))
      l_orderkey.cogroup(o_orderkey)
        .filter(p => p._2._2.iterator.hasNext)
        .flatMap(p => {
          val custkey = p._2._2.iterator.next()
          // sperate each month
          // ((name, month), 1)
          p._2._1.map(date => ((cuskeyHMap.value(custkey), date.toString.substring(0,7)), 1))
        })
        .reduceByKey(_ + _)
        .sortByKey()
        .collect()
        .foreach(p => println("(" + p._1._1 + "," + p._1._2 + "," + p._2 + ")"))
    }
  }
}
