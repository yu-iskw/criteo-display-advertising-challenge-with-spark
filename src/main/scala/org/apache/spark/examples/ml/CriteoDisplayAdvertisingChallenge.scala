package org.apache.spark.examples.ml

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}

class CriteoDisplayAdvertisingChallenge {

}

object CriteoDisplayAdvertisingChallenge extends Logging {

  private[examples] val labelColumn = "label"

  private[examples] val numericalColumns = Array(
    "i1", "i2", "i3", "i4", "i5",
    "i6", "i7", "i8", "i9", "i10",
    "i11", "i12", "i13"
  )

  private[examples] val categoricalColumns = Array(
    "c1", "c2", "c3", "c4", "c5",
    "c6", "c7", "c8", "c9", "c10",
    "c11", "c12", "c13", "c14", "c15",
    "c16", "c17", "c18", "c19", "c20",
    "c21", "c22", "c23", "c24", "c25",
    "c26"
  )

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
  }

  private[examples]
  def run(sc: SparkContext, sqlContext: SQLContext, path: String): Unit = {
    val df = createDataFrame(sc, sqlContext, path)
  }

  private[examples]
  def createDataFrame(sc: SparkContext, sqlContext: SQLContext, path: String): DataFrame = {

    val rdd = sc.textFile(path).map(line => parse(line))
    val schema = getSchema
    sqlContext.createDataFrame(rdd, schema)
  }

  private[examples]
  def encode(df: DataFrame): DataFrame = {
    val pipeline = new Pipeline()
    val stages: Array[PipelineStage] = categoricalColumns.flatMap { c =>
      val indexedCol = s"${c}_index"
      val vecCol = s"${c}_vec"
      val strIdx = new StringIndexer()
        .setInputCol(c)
        .setOutputCol(indexedCol)
        .setHandleInvalid("skip")
      val ohe = new OneHotEncoder()
        .setInputCol(indexedCol)
        .setOutputCol(vecCol)
      Array(strIdx, ohe)
    }
    pipeline.setStages(stages)
    pipeline.fit(df).transform(df)
  }

  private[examples]
  def getSchema: StructType = {
    val fields = Array(StructField("label", DoubleType, false)) ++
      numericalColumns.map(name => StructField(name, IntegerType, true)) ++
      categoricalColumns.map(name => StructField(name, StringType, true))
    StructType(fields)
  }

  private[examples]
  def parse(line: String): Row = {
    val elms = line.split( """\t""")
    val converted = Seq(elms(0).toDouble) ++
      (1 to 13).map(i => if (elms(i) == "") null else elms(i).toInt) ++
      (14 to 39).map(i => elms(i))
    Row(converted: _*)
  }
}

