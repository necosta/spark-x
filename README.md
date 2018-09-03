# spark-x
Simple Spark project built in Scala

### To-Do
With source (data for January, which will be around 100+MB deflated):
https://transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time

1. Create the dataframe with joins with master (optimization using broadcasts)
1. Based on the dataset find the Airports with the least delay (sorting and selecting the top)
1. Multiple groupings like which Airline has most flights to New York (uses reduce and combine operators)
1. Secondary sorts like which airlines arrive the worst on which airport and by what delay
1. Custom partitions using airline Id (in combination with - d)
1. Any other interesting insights

### Pre-requisites
* Install [SBT](https://www.scala-sbt.org/download.html)

### Source data
Available here: https://transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time

Select fields:
* Time Period > Flight Date
* Airline > AirlineID (L_AIRLINE_ID lookup)
* Airline > FlightNum
* Origin > OriginAirportID (L_AIRPORT_ID lookup)
* Destination > DestAirportID (L_AIRPORT_ID lookup)
* Destination > DestCityName
* Departure Perf > DepDelay
* Arrival Perf > ArrDelay
* Cancellations and Diversions > Cancelled

**ToDo:** Automate import of base table from webpage

### How to build Spark job
* Build: `sbt compile`
* Test: `sbt test`
* Package: `sbt package`

### How to run Spark job
* Build image:
```
docker build --build-arg VERSION=x.y.z -t sparkx .
```
**Note**: Get version from version.sbt file

* Run image:
```
docker run sparkx
```
