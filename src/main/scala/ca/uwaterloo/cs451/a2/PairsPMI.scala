

package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner
import scala.collection.mutable.ListBuffer

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "number of threshold", required = false, default = Some(10))
  // val numExecutors = opt[String](descr = "number of executors", required = false)
  // val executorCores = opt[Int](descr = "number of executor cores", required = false)
  // val executorMemory = opt[String](descr = "number of executor memory", required = false)
  verify()
}

class pairsPmiPartitioner(partitionsNum: Int) extends Partitioner {
  override def numPartitions: Int = partitionsNum
  override def getPartition(key: Any): Int = key match {
    case (x: String, y: String) =>
      ((x.hashCode()  & Integer.MAX_VALUE ) % numPartitions).toInt

  }
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Number of threshold: " + args.threshold())

    val conf = new SparkConf()
      .setAppName("Pairs PMI")
      // x workers
      // .set("spark.executor.instances", "2")
      // x cores on each workers
      // .set("spark.executor.cores", "4")
      // x g for executor memory
      // .set("spark.executor.memory", "24G")

    val sc = new SparkContext(conf)

    val threshold = args.threshold().toInt
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    val singleWordCounts = textFile
      .flatMap(line =>{
        val tokens = tokenize(line)
          var set_tokens = tokens.take(40).toSet
          if (tokens.length > 0) {
            set_tokens.map(p => p).toList
        // if (tokens.length > 1) tokens.map(p => p + ", *").toList.dropRight(1) else List()
          } else List()
      })
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)
      .sortByKey()


    val totalWordCounts = textFile
      .flatMap(line => {
        val tokens = tokenize(line).take(40)
        // List of (x, y) and (x, *)
         tokens.map(p => "*, *").toList
      })

      .map(bigram => ("*", 1))
      .reduceByKey(_ + _)

      val total_word_cout = sc.broadcast(totalWordCounts.lookup("*")(0))
      val single_word_count = sc.broadcast(singleWordCounts.collectAsMap())

      val pmiCalculation = textFile
          .flatMap(line => {
            val tokens = tokenize(line).take(40)
            val setTokens = tokens.toSet
            val pairs : ListBuffer[(String,String)] = ListBuffer()
            for (x <- setTokens) {
              for (y <- setTokens) {
                if (x != y) {
                  pairs.append((x, y))
                }
              }
            }
            pairs.map(p=> p._1.toString + " " + p._2  ).toList
          })
        .map(bigram => ((tokenize(bigram)(0),tokenize(bigram)(1)), 1))
        .reduceByKey(_ + _)
        .sortByKey()

        .partitionBy(new pairsPmiPartitioner(args.reducers()))
        .map(bigram => {
            val co_count = bigram._2
            val prob_x = single_word_count.value.get(bigram._1._1).head
            val prob_y = single_word_count.value.get(bigram._1._2).head
            if (co_count >= threshold) {
              val pmi = Math.log10(co_count.toFloat * total_word_cout.value/(prob_x.toFloat * prob_y.toFloat))
              (bigram._1, (pmi, co_count))
            }
          })

    pmiCalculation.saveAsTextFile(args.output())
  }
}
