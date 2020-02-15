package com.mrjob.demo;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class TemperatureMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	/*
	 * Link = https://lstsal.wordpress.com/2015/08/23/mapreduce-programme-find-highest-temperature-for-each-year-in-ncdc-data-set/
## Objective is to calculate max temp of weather station for that year
Step 1:- Extract  location, date, type, data, from the input data 1900

So we have data in format of 
------------------------------
Lets say For year..1900
For a  particular station, min & max temperatures for all  days across an year 

stationID1 day1 min  -valX   >>> on Jan 1
stationID1 day1 max  valX
stationID1 day2 min -valy
stationID1 day2 max  valy
stationID1 day3 min -valy
stationID1 day3 max  valy
...............................
................................
stationID1 day365  max -val
stationID1 day365  max val   >>>on Dec 31
...
...
...
...For N no of station
stationID[n] day1 min  -valX
stationID[n] day1 max   valX
stationID[n] day2 min  -valy
stationID[n] day2 max   valy
...............................
................................
stationID[n] day365  max -val
stationID[n] day365  max val


--------------------------------

Step 2- Filter out on TMAX 
stationID1 day1 max  -valX

Mapper output => (stationId , maxTemperature)

(stationID1,valX)   -- max temperature of day1
(stationID1,valX)   -- max temperature of day2
(stationID1,valX)   -- max temperature of day3
..............................................
(stationID1,valX)   -- max temperature of day365


(stationID2,valX)   -- max temperature of day1
(stationID2,valX)   -- max temperature of day2
(stationID2,valX)   -- max temperature of day3
..............................................
(stationID2,valX)   -- max temperature of day365



Step 3 : Output of sort/merge  group by stationID

(stationID1,[(max temperature day1),(max temperature day2 )(max temperature day3) .... (max temperature 365) ]
(stationID2,[(max temperature day1),(max temperature day2 )(max temperature day3) .... (max temperature 365) ]
(stationID3,[(max temperature day1),(max temperature day2 )(max temperature day3) .... (max temperature 365) ]

 (stationID1,[88, 67,  ............... 44 ] )
 (stationID2,[44,88,  ...............  88 ] )

Step 5 
Calculate the max temperature ..  
For stationID1
calculate the max value from the array => [88, 67,  ............... 44] )


[cloudera@quickstart ~]$ 
sudo -u hdfs  hadoop jar TemperatureDriverMain.jar /user/cloudera/weatherstation.txt  /output_weathermintemperature

[cloudera@quickstart ~]$ hdfs dfs -cat /output_weathermintemperature/part-r-00000
EZE00100082	90.13999862670899
ITE00100554	90.13999862670899

 */
	
	    protected void map(LongWritable key, Text value, Context context)	throws IOException, java.lang.InterruptedException
	    {
		//ITE00100554	18000101	TMAX	-75			E  - Day1 
		//ITE00100554	18000101	TMIN	-148		E  - Day1 
		 String record = value.toString(); 
		 String[] recordtoken = record.split("\t");
		 //location, date, type, data
		  String locationstationID = recordtoken[0];
		  String date = recordtoken[1];
		  String type = recordtoken[2];
		  String temperature = recordtoken[3];
		  if (type.equalsIgnoreCase("TMAX"))
		  {
		  System.out.println("RECORD"+"---"+ locationstationID +"\t"+ type +"\t" +temperature);
		  Float tempFarenhiet = Float.parseFloat(temperature);
		  double celsius = (float)((tempFarenhiet)/10.0);
		  double fahrenheit = celsius * 1.8 + 32.0;
		  context.write(new Text(locationstationID), new DoubleWritable(fahrenheit) );
		  }
          }
}