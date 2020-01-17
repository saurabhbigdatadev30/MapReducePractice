package com.mrjob.demo;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class TemperatureReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	 
 @Override
protected void reduce(Text locationstationID, Iterable<DoubleWritable> temperatureValues, Context context)	throws IOException, java.lang.InterruptedException
{
Double maxVal=Double.MIN_VALUE;	 
 Double temp;
 System.out.println("KEY IS >>>>>>>"+ locationstationID);
 for (DoubleWritable val : temperatureValues) {
	 System.out.println("Value  IS >>>>>>>"+ val); 
	 maxVal = Math.max(maxVal, val.get());
 }
 context.write(locationstationID, new DoubleWritable(maxVal)); 	 
	 
}
}
