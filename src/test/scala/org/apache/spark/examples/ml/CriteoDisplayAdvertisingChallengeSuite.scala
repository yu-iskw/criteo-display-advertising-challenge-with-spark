package org.apache.spark.examples.ml

import scala.io.Source

import org.apache.log4j.{Level, Logger}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.util.MLlibTestSparkContext

class CriteoDisplayAdvertisingChallengeSuite extends SparkFunSuite with MLlibTestSparkContext {

  val path = this.getClass.getResource("/day_0.part.txt").getPath

  test("parse") {
    Source.fromFile(path).getLines().foreach { line =>
      val row = CriteoDisplayAdvertisingChallenge.parse(line)
      assert(row.isInstanceOf[Row])
      assert(row.length === 40)
    }
  }

  test("prepare data") {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val df = CriteoDisplayAdvertisingChallenge.createDataFrame(sc, sqlContext, path).cache()
    assert(df.columns.length === 40)
    assert(df.count() === 10000)

    val encodedDF = CriteoDisplayAdvertisingChallenge.encode(df)
    assert(encodedDF.columns.length === 40)
    println(1)
  }

}
