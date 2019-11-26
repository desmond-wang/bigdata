package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class ApplyEnsembleSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model, method)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "trained model path", required = true)
  val method = opt[String](descr = "ensemble method", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new ApplyEnsembleSpamClassifierConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())
    log.info("Method: " + args.method())

    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val method = args.method()
    val textFile = sc.textFile(args.input())
    val modelGroup_x = args.model() + "/part-00000"
    val modelGroup_y = args.model() + "/part-00001"
    val modelBritney = args.model() + "/part-00002"

    // get and broadcast weight
    def weights(modelPath: String) : Map[Int, Double] = {
      val model = sc.textFile(modelPath)

      model.map(line =>{
        val tokens = line.split("\\(|,|\\)")
        val feature = tokens(1).toInt
        val weight = tokens(2).toDouble

        (feature, weight)
      }).collect().toMap
    }

    val weightsX = sc.broadcast(weights(modelGroup_x))
    val weightsY = sc.broadcast(weights(modelGroup_y))
    val weightsBritney = sc.broadcast(weights(modelBritney))

    def spamminess(features: Array[Int], w: Map[Int, Double]) : Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }

    def isSpam(score: Double) = if (score > 0) true else false

    val results = textFile.map(line => {

      val tokens = line.split(" ")
      val docID = tokens(0)
      val isSpamVal = tokens(1)
      val features = tokens.slice(2, tokens.length).map(numStr => numStr.toInt)

      // set up
      var spamScore = 0.0
      var predictionLabel = "null"

      val spamScoreX = spamminess(features, weightsX.value)
      val spamScoreY = spamminess(features, weightsY.value)
      val spamScoreBritney = spamminess(features, weightsBritney.value)

      if (method.equals("vote")) {
        var spamVotes = 0
        var hamVotes = 0
        if (isSpam(spamScoreX)) spamVotes += 1 else hamVotes += 1
        if(isSpam(spamScoreY)) spamVotes += 1 else hamVotes += 1
        if(isSpam(spamScoreBritney)) spamVotes += 1 else hamVotes += 1
        spamScore = spamVotes - hamVotes

      } else if (method.equals("average")) {
        spamScore = (spamScoreX + spamScoreY + spamScoreBritney) / 3.0
      }
      predictionLabel = if (spamScore > 0) "spam" else "ham"

      (docID, isSpamVal, spamScore, predictionLabel)
    })
    results.saveAsTextFile(args.output())
  }
}