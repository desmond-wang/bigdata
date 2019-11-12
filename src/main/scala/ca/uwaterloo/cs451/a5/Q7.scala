package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf7(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date", required = true)
  val text = opt[Boolean](descr = "plain-text data", required = false, default=Some(false))
  val parquet = opt[Boolean](descr = "parquet data", required = false, default=Some(false))
  requireOne(text, parquet)
  verify()
}

object Q7 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf7(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    if (args.text()){
      log.info("Running on Plain Text: " + args.text())
    } else if (args.parquet()){
      log.info("Running on Parquet: " + args.parquet())
    }


    val conf = new SparkConf().setAppName("Q7")
    val sc = new SparkContext(conf)
    val date = args.date()

    if (args.text()){
      // pain text data
      // where l_shipdata
      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
      val ordersRDD = sc.textFile(args.input() + "/orders.tbl")
      val customerRDD = sc.textFile(args.input() + "/customer.tbl")

      val custkey = customerRDD
        .map(line => {
          val tokens = line.split('|')
          // (cuskey, custname)
          (tokens(0),tokens(1))
        })
        .collectAsMap()
      val cuskeyHMap = sc.broadcast(custkey)


      val l_orderkey = lineitemRDD
          .filter(line => {
            val tokens = line.split('|')
            // l_shipdate > "YYYY-MM-DD"
            tokens(10) > date
          })
        .map(line => {
          val tokens = line.split('|')
          // (orderkey, l_extendedprice*(1-l_discount)) as revenue)
          (tokens(0), tokens(5).toDouble * (1.0 - tokens(6).toDouble))
        })
        // add same oderkey together
        .reduceByKey(_ + _)

      val o_orderkey = ordersRDD
        .filter(line => {
          val tokens = line.split('|')
          // o_orderdate < "YYYY-MM-DD"
          tokens(4) < date
        })
          .map(line => {
            val tokens = line.split('|')
            val name = cuskeyHMap.value(tokens(1))
            // (ordernumber, cutomername, orderdate, ship_priority)
            (tokens(0), (name, tokens(4), tokens(7)))
          })

      // (orderkey, (revenue, (custname, orderdate, ship_priority)))
      l_orderkey.cogroup(o_orderkey)
        .filter(p => p._2._1.iterator.hasNext && p._2._2.iterator.hasNext)

        // ((custname, orderkey, orderdate, ship_priority) , revenue)
        .map (p => {
          val orderkey = p._1
          val revenue = p._2._1.iterator.next()
          val ordersIterator = p._2._2.iterator.next()
          val name = ordersIterator._1
          val orderdate = ordersIterator._2
          val shipproprity = ordersIterator._3
          (revenue, (orderkey, name, orderdate, shipproprity))
        })
        .sortByKey(false)
        .take(10)
        // (name, oderkey, revenue, orderdate, shipprority
        .map(p => (p._2._2, p._2._1, p._1, p._2._3, p._2._4))
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

      val custkey = customerRDD
        .map(line => {
          // (cuskey, custname)
          (line(0),line(1))
        })
        .collectAsMap()
      val cuskeyHMap = sc.broadcast(custkey)


      val l_orderkey = lineitemRDD
        .filter(line => {
          // l_shipdate > "YYYY-MM-DD"
          line(10).toString > date
        })
        .map(line => {
          // (orderkey, l_extendedprice*(1-l_discount)) as revenue)
          (line(0), line(5).toString.toDouble * (1.0 - line(6).toString.toDouble))
        })
        // add same oderkey together
        .reduceByKey(_ + _)

      val o_orderkey = ordersRDD
        .filter(line => {
          // o_orderdate < "YYYY-MM-DD"
          line(4).toString < date
        })
        .map(line => {
          val name = cuskeyHMap.value(line(1))
          // (ordernumber, cutomername, orderdate, ship_priority)
          (line(0), (name, line(4), line(7)))
        })

      // (orderkey, (revenue, (custname, orderdate, ship_priority)))
      l_orderkey.cogroup(o_orderkey)
        .filter(p => p._2._1.iterator.hasNext && p._2._2.iterator.hasNext)

        // ((custname, orderkey, orderdate, ship_priority) , revenue)
        .map (p => {
          val orderkey = p._1
          val revenue = p._2._1.iterator.next()
          val ordersIterator = p._2._2.iterator.next()
          val name = ordersIterator._1
          val orderdate = ordersIterator._2
          val shipproprity = ordersIterator._3
          (revenue, (orderkey, name, orderdate, shipproprity))
        })
        .sortByKey(false)
        .take(10)
        // (name, oderkey, revenue, orderdate, shipprority
        .map(p => (p._2._2, p._2._1, p._1, p._2._3, p._2._4))
        .foreach(println)
    }
  }
}
