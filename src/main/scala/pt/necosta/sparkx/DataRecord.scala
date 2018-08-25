package pt.necosta.sparkx

import java.sql.Timestamp

case class InputRecord(FL_DATE: Timestamp,
                       AIRLINE_ID: Int,
                       CARRIER: String,
                       FL_NUM: Int,
                       ORIGIN_AIRPORT_ID: Int,
                       ORIGIN_AIRPORT_SEQ_ID: Int,
                       ORIGIN_CITY_MARKET_ID: Int,
                       DEST_AIRPORT_ID: Int,
                       DEST_AIRPORT_SEQ_ID: Int,
                       DEST_CITY_MARKET_ID: Int)

case class LookupRecord(Code: Int, Description: String)

case class OutputRecord(FL_DATE: Timestamp,
                        AIRLINE_ID: Int,
                        AirlineDescription: String)
