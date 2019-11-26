
package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable._

class ApplySpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "model path", required = true)
  verify()
}

object ApplySpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new ApplySpamClassifierConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val model = args.model() + "/part-00000"
    val textFile = sc.textFile(args.input())

    // get and broadcast weight
    val temp = sc.textFile(model)
      .map(line =>{
        val tokens = line.split("\\(|,|\\)")
        val feature = tokens(1).toInt
        val weight = tokens(2).toDouble
        (feature, weight)
      })
      .collectAsMap()
    val weight = sc.broadcast(temp)

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      features.foreach(f => {
        val w = weight.value
        if (w.contains(f)) score += w(f)
      })
      score
    }

    val result = textFile.map(line => {
      val tokens = line.split(" ")
      val docID = tokens(0)
      val isSpam = tokens(1)
      // feature vector of the training instance
      val features = tokens.slice(2, tokens.length).map(numStr => numStr.toInt)

      val score = spamminess(features)
      val perdictionLabel = if (score > 0) "spam" else "ham"
      (docID, isSpam, score, perdictionLabel)
    })

    result.saveAsTextFile(args.output())
  }
}