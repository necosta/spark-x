package pt.necosta.sparkx

import org.apache.spark.sql.SparkSession
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class DataflowSpec extends FlatSpec with Matchers with SharedSparkContext {

  "Dataflow" should "correctly add uid column" in {
    implicit val spark : SparkSession = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = (1 to 100).toDF("col1")
    val dfOutput = df.transform(Dataflow.addIdColumn())
    dfOutput.columns.length should be(2)

    assertThrows[IllegalArgumentException] {
      val df = (1 to 100).toDF(Dataflow.idColumnName)
      df.transform(Dataflow.addIdColumn())
    }
  }
}
