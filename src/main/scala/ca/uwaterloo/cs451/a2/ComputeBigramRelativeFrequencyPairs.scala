/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner

class ConfP(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(
    // numExecutors, executorCores, executorMemory, 
    input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  // val numExecutors = opt[String](descr = "number of executors", required = false)
  // val executorCores = opt[Int](descr = "number of executor cores", required = false)
  // val executorMemory = opt[String](descr = "number of executor memory", required = false)
  verify()
}

class myPartitioner(partitionsNum: Int) extends Partitioner {
  override def numPartitions: Int = partitionsNum
  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[String]

    return ( (k.split(" ").head.hashCode() & Integer.MAX_VALUE ) % numPartitions).toInt

  }
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf()
      .setAppName("Compute Bigram Relative Frequency Pairs")
      // x workers
      // .set("spark.executor.instances", "2")
      // x cores on each workers
      // .set("spark.executor.cores", "4")
      // x g for executor memory
      // .set("spark.executor.memory", "24G")

    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val counts = textFile

      .flatMap(line => {
        val tokens1 = tokenize(line)
        val tokens2 = tokenize(line)
        // List of (x, y) and (x, *)
        List(
        (if (tokens1.length > 1) tokens1.sliding(2).map(p => p.mkString(", ")).toList else List()),
        (if (tokens2.length > 1) tokens2.map(p => p + ", *").toList.dropRight(1) else List())
        ).flatten
      })

      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)
      .sortByKey()

      .partitionBy(new myPartitioner(args.reducers()))

        .mapPartitions(x => {
          var marginal = 0.0f
          x.map(p => {

            if (p._1.split(", ")(1) == "*"){
              marginal = p._2.toFloat
              ("(" + p._1 + ")", p._2)
            } else {
              ("(" + p._1 + ")", p._2 /  marginal)
            }
          })})

    counts.saveAsTextFile(args.output())
  }
}
