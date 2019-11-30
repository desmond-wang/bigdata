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

package ca.uwaterloo.cs451.a7

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._

import scala.collection.mutable

class TrendingArrivalsConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object TrendingArrivals {
  val log = Logger.getLogger(getClass().getName())


  def trackStateFunc(batchTime: Time, key: String, value: Option[Tuple3[Int, Long, Int]],
                     state: State[Tuple3[Int, Long, Int]]): Option[(String, Tuple3[Int, Long, Int])] = {
    val previousTraffic = state.getOption.getOrElse((0,0L,0))._1
    val currentTraffic = value.getOrElse((0,0L,0))._1
    val output = (key, (currentTraffic, batchTime.milliseconds, previousTraffic))

    if (currentTraffic >= 10 && currentTraffic >= previousTraffic * 2) {
      // doubled, need print
      var printline = "Number of arrivals to "
      if (key == "goldman") {
        printline += "Goldman Sachs has doubled from "
        printline += previousTraffic.toString + " to " + currentTraffic.toString
        printline += " at " + batchTime.milliseconds.toString + "!"
      } else {
        printline += "Citigroup has doubled from "
        printline += previousTraffic.toString + " to " + currentTraffic.toString
        printline += " at " + batchTime.milliseconds.toString + "!"
      }
      println(printline)
    }
    state.update((currentTraffic, batchTime.milliseconds, previousTraffic))
    Some(output)
  }

  def main(argv: Array[String]): Unit = {
    val args = new TrendingArrivalsConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("EventCount")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)

    val batchListener = new StreamingContextBatchCompletionListener(ssc, 144)

    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)


    val stateSpec = StateSpec.function(trackStateFunc _)

    // goldman = [[-74.0141012, 40.7152191], [-74.013777, 40.7152275], [-74.0141027, 40.7138745], [-74.0144185, 40.7140753]]
    //citigroup = [[-74.011869, 40.7217236], [-74.009867, 40.721493], [-74.010140,40.720053], [-74.012083, 40.720267]]

    val goldman_lat_min = -74.0144185
    val gold_lat_max = -74.013777

    val gold_lon_min = 40.7138745
    val gold_lon_max = 40.7152275

    val citi_lat_min = -74.012083
    val citi_lat_max = -74.009867

    val citi_lon_min = 40.720053
    val citi_lon_max = 40.7217236

    val wc = stream.map(_.split(","))
      // for two different type of taxi company format
      .map(tuple => {
        if (tuple(0) == "green") {
          if (tuple(8).toDouble > goldman_lat_min && tuple(8).toDouble < gold_lat_max &&
          tuple(9).toDouble > gold_lon_min && tuple(9).toDouble < gold_lon_max) {
            ("goldman", 1)
          } else if (tuple(8).toDouble > citi_lat_min && tuple(8).toDouble < citi_lat_max &&
            tuple(9).toDouble > citi_lon_min && tuple(9).toDouble < citi_lon_max){
            ("citigroup", 1)
          } else {
            ("garbage", 1)
          }
        } else {
          if (tuple(10).toDouble > citi_lat_min && tuple(10).toDouble < citi_lat_max &&
            tuple(11).toDouble > citi_lon_min && tuple(11).toDouble < citi_lon_max) {
            ("citigroup", 1)
          } else if (tuple(10).toDouble > goldman_lat_min && tuple(10).toDouble < gold_lat_max &&
            tuple(11).toDouble > gold_lon_min && tuple(11).toDouble < gold_lon_max) {
            ("goldman", 1)
          } else {
            ("garbage", 1)
          }
        }
      })
      // remove garbage
      .filter(r => (r._1 != "garbage"))
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(10), Minutes(10))
      // change the format to match statespec
      .map(p => (p._1, (p._2, 0L, 0)))
      .mapWithState(stateSpec)
      .persist()

    wc.saveAsTextFiles(args.output() + "/part")

    wc.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}
