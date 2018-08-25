package pt.necosta.sparkx

import java.sql.Timestamp

case class InputRecord(FL_DATE: Timestamp,
                       AIRLINE_ID: Int,
                       FL_NUM: Int,
                       ORIGIN_AIRPORT_ID: Int,
                       DEST_AIRPORT_ID: Int,
                       DEST_CITY_NAME: String,
                       DEP_DELAY: Option[Double],
                       ARR_DELAY: Option[Double],
                       CANCELLED: Double)

case class LookupRecord(Code: Int, Description: String)

case class OutputRecord(FL_DATE: Timestamp,
                        AirlineDesc: String,
                        OriginAirportDesc: String,
                        DestAirportDesc: String,
                        DEST_CITY_NAME: String,
                        DepartureDelay: Option[Int],
                        ArrivalDelay: Option[Int],
                        IsCancelled: Option[Boolean])
