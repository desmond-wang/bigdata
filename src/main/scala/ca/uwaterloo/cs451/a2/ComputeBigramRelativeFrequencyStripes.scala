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

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val numExecutors = opt[Int](descr = "number of executors", required = false)
  val executorCores = opt[Int](descr = "number of executor cores", required = false)
  val executorMemory = opt[Int](descr = "number of executor memory", required = false)
  verify()
}

class myPartitioner(partitionNum: Int) extends Partitioner {
  override def numPartitions: Int = partitionsNum
  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[String]

    return ( (k.split(" ").head.hashCode() & Integer.MAX_VALUE ) % numPartitions).toInt

  }
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf()
      .setAppName("Compute Bigram Relative Frequency Stripes")
      // x workers
      .set("spark.executor.instances", args.numExecutors)
      // x cores on each workers
      .set("spark.executor.cores", args.executorCores)
      // x g for executor memory
      .set("spark.executor.memory", args.executorMemory)

    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val counts = textFile

      .flatMap(line => {
        val tokens1 = tokenize(line)
        val tokens2 = tokenize(line)
        // List of (x, y) and (x, *)
        if (tokens1.length > 1) tokens1.sliding(2).toList.map(p=> {
          var immutableMap = scala.collection.immutable.Map[Stirng,Int]()
          var subList = immutableMap + (p(1) -> 1)
          (p(0),subList)
        }) else List()
      })

      .reduceByKey((x,y) => x ++ y.map {case (k,v) => k -> (v + x.getOrElse(k,0))})

      .sortByKey()
      .partitionBy(new myPartitioner(args.reducers()))

      .mapPartitions(x => {
        x.map(p => {
          var sum = 0.0f
          var mutableMap = scala.collection.mutable.Map(p._2.toSeq: _*)
          mutableMap.foreach {case(k, v) => {sum = sum + v}}
          var returnVal = mutableMap.map {case (k,v) => (k, v/sum)}
          (p._1, returnVal)
        })})

    counts.saveAsTextFile(args.output())
  }
}
