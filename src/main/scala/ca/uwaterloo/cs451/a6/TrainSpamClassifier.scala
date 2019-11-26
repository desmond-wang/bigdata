
package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable._


class TrainSpamClassifierConf(args: Seq[String]) extends ScallopConf(args){
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "output trained model path", required = true)
  val shuffle = opt[Boolean](descr = "shuffle option", required = false, default=Some(false))
  verify()
}

object TrainSpamClassifier extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new TrainSpamClassifierConf(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Shuffle: " + args.shuffle())

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val shuffle = args.shuffle()
    val textFile = sc.textFile(args.input())

    // w is the weight vector (make sure the variable is within scope)
    val w = Map[Int, Double]()

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }

    // This is the main learner:
    val delta = 0.002

    var trainDataSet = textFile.map(line => {
      val tokens = line.split(" ")
      val docid = tokens(0)
      // change spam to bool
      val isSpam = if (tokens(1).trim().equals("spam")) 1 else 0
      // feature vector of the training instance
      val features = tokens.slice(2, tokens.length).map(numStr => numStr.toInt)
      // generate random
      val rand = scala.util.Random.nextInt

      (0, (docid,isSpam,features,rand))
    })

    if (shuffle) {
      // sort by randoan number
      trainDataSet = trainDataSet.sortBy(p => p._2._4)
    }

    val training = trainDataSet.groupByKey(1)
      .flatMap(p => {
        val instance = p._2.iterator
        while(instance.hasNext) {
          // ingore the 0
          val record = instance.next()
          val docid = record._1
          val isSpam = record._2
          val features = record._3

          // update weight
          val score = spamminess(features)
          val prob = 1.0 / (1 + Math.exp(-score))
          features.foreach(f => {
            if (w.contains(f)) {
              w(f) += (isSpam - prob) * delta
            } else {
              w(f) = (isSpam - prob) * delta
            }
          })
        }
        w
      })
    training.saveAsTextFile(args.model())
  }
}