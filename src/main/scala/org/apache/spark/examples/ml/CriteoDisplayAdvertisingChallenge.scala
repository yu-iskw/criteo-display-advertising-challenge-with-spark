package org.apache.spark.examples.ml

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}

class CriteoDisplayAdvertisingChallenge {

}

object CriteoDisplayAdvertisingChallenge extends Logging {

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
    import sqlContext.implicits._

    val rdd = sc.textFile(path).map {line =>
      try {
        Some(parse(line))
      } catch {
        case _ => None
      }
    }
    rdd.filter(_.isDefined).map(_.get).toDF()
  }

  private[examples]
  def parse(line: String): ClickData = {
    val elms = line.split("""( |\t)""")
    ClickData(
      elms(0).toDouble,
      elms(1).toInt, elms(2).toInt, elms(3).toInt, elms(4).toInt, elms(5).toInt,
      elms(6).toInt, elms(7).toInt, elms(8).toInt, elms(9).toInt, elms(10).toInt,
      elms(11).toInt, elms(12).toInt, elms(13).toInt,
      elms(14), elms(15), elms(16), elms(17), elms(18), 
      elms(19), elms(20), elms(21), elms(22), elms(23),
      elms(24), elms(25), elms(26), elms(27), elms(28),
      elms(29), elms(30), elms(31), elms(32), elms(33),
      elms(34), elms(35), elms(36), elms(37), elms(38),
      elms(39)
    )
  }
}

case class ClickData(
  label: Double,
  i1: Int, i2: Int, i3: Int, i4: Int, i5: Int,
  i6: Int, i7: Int, i8: Int, i9: Int, i10: Int,
  i11: Int, i12: Int, i13: Int,
  c1: String, c2: String, c3: String, c4: String, c5: String,
  c6: String, c7: String, c8: String, c9: String, c10: String,
  c11: String, c12: String, c13: String, c14: String, c15: String,
  c16: String, c17: String, c18: String, c19: String, c110: String,
  c21: String, c22: String, c23: String, c24: String, c25: String,
  c26: String)
