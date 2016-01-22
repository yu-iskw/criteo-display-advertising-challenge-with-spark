package org.apache.spark.examples.ml

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.MLlibTestSparkContext

class CriteoDisplayAdvertisingChallengeSuite extends SparkFunSuite with MLlibTestSparkContext {

  val path = this.getClass.getResource("/day_0.part.txt").getPath

  test("createDataFrame") {
    val df = CriteoDisplayAdvertisingChallenge.createDataFrame(sc, sqlContext, path)
    assert(df.columns.length === 40)
  }

}
