package pt.necosta.sparkx

import org.apache.spark.sql.Dataset
import java.sql.Timestamp

case class TransportRecord(FL_DATE: Timestamp,
                           AIRLINE_ID: Int,
                           CARRIER: String,
                           FL_NUM: Int,
                           ORIGIN_AIRPORT_ID: Int,
                           ORIGIN_AIRPORT_SEQ_ID: Int,
                           ORIGIN_CITY_MARKET_ID: Int,
                           DEST_AIRPORT_ID: Int,
                           DEST_AIRPORT_SEQ_ID: Int,
                           DEST_CITY_MARKET_ID: Int)

object DataObject extends WithSpark {

  def csvToDataset(sourceFilePath: String): Dataset[TransportRecord] = {
    import spark.implicits._

    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(sourceFilePath)
      .as[TransportRecord]
  }
}
