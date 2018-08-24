package pt.necosta.sparkx

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Dataflow extends WithSpark {

  val idColumnName = "id"

  def addIdColumn(): DataFrame => DataFrame = {
    df => {
      require(!df.columns.contains(idColumnName))
      df.withColumn(idColumnName, monotonically_increasing_id)
    }
  }
}
