package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf6(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date", required = true)
  val text = opt[Boolean](descr = "plain-text data", required = false, default=Some(false))
  val parquet = opt[Boolean](descr = "parquet data", required = false, default=Some(false))
  requireOne(text, parquet)
  verify()
}

object Q6 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf6(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    if (args.text()){
      log.info("Running on Plain Text: " + args.text())
    } else if (args.parquet()){
      log.info("Running on Parquet: " + args.parquet())
    }


    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)

    val date = args.date()

    if (args.text()){
      // pain text data
      // where l_shipdata
      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")

      val lineitem = lineitemRDD
        // filter the target date
        .filter((line => {
          val tokens = line.split('|')
          tokens(10).toString.contains(date)
        }))
        .map(line => {
          val tokens = line.split('|')
          val returflag = tokens(8).toString()
          val linestatus = tokens(9).toString()
          val quantity = tokens(4).toString.toDouble
          val extenedparice = tokens(5).toString.toDouble
          val discount = tokens(6).toString.toDouble
          val tax = tokens(7).toString.toDouble
          val discount_price = extenedparice * (1-discount)
          val final_price = extenedparice * (1-discount) * (1 + tax)

          // used to calculate total number
          val count = 1
          ((returflag, linestatus), (quantity,extenedparice,discount_price, final_price,discount,count))
        })
        // add same together
        .reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6))
        .sortByKey()
        .map(p => {
          val returflag = p._1._1
          val linestatus = p._1._2
          val sum_qty = p._2._1
          val sum_base_price = p._2._2
          val sum_disc_price = p._2._3
          val sum_final_price = p._2._4
          val avg_qty = p._2._1 / p._2._6
          val avg_extened_price = p._2._2 / p._2._6
          val avg_disc = p._2._5 / p._2._6
          val count_order = p._2._6
          (returflag, linestatus, sum_qty,sum_base_price, sum_disc_price, sum_final_price,
            avg_qty, avg_extened_price, avg_disc, count_order)
        })
        .collect()
        .foreach(println)


    } else {
      // parquet data
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd


      val lineitem = lineitemRDD
        // filter the target date
        .filter((line => {
          line(10).toString.contains(date)
        }))
        .map(line => {
          val returflag = line(8).toString()
          val linestatus = line(9).toString()
          val quantity = line(4).toString.toDouble
          val extenedparice = line(5).toString.toDouble
          val discount = line(6).toString.toDouble
          val tax = line(7).toString.toDouble
          val discount_price = extenedparice * (1-discount)
          val final_price = extenedparice * (1-discount) * (1 + tax)

          // used to calculate total number
          val count = 1
          ((returflag, linestatus), (quantity,extenedparice,discount_price, final_price,discount,count))
        })
        // add same together
        .reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6))
        .sortByKey()
        .map(p => {
          val returflag = p._1._1
          val linestatus = p._1._2
          val sum_qty = p._2._1
          val sum_base_price = p._2._2
          val sum_disc_price = p._2._3
          val sum_final_price = p._2._4
          val avg_qty = p._2._1 / p._2._6
          val avg_extened_price = p._2._2 / p._2._6
          val avg_disc = p._2._5 / p._2._6
          val count_order = p._2._6
          (returflag, linestatus, sum_qty,sum_base_price, sum_disc_price, sum_final_price,
            avg_qty, avg_extened_price, avg_disc, count_order)
        })
        .collect()
        .foreach(println)

    }
  }
}
