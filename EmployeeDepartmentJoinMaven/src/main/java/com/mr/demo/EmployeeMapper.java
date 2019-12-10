package com.mr.demo;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class EmployeeMapper extends Mapper<LongWritable, Text, Text, Text>
{		
	/* Employee file contains 
	 1281,Shawn,Architect,7890,1481,10
     1381,Jacob,Admin,4560,1481,20
     1481,flink,Mgr,9580,1681,10
     1581,Richard,Developer,1000,1681,40
     1681,Mira,Mgr,5098,1481,10
     1781,John,Developer,6500,1681,10
	 */
	
	public void map(LongWritable key, Text value, Context con) throws IOException,InterruptedException 
	{
		
		String line = value.toString().trim();        // convert incoming record to string
		String[] EmployeeData = line.split(",");   // [{ 1281} {Shawn} {Architect} {7890} {1481} {10} ]
		System.out.println("Employee Mapper" + "\t" + EmployeeData[0] + ""+ EmployeeData[1] + ""+ EmployeeData[2] + ""+ EmployeeData[3]+  "" + EmployeeData[4]);
	   con.write(new Text(EmployeeData[5]), new Text("Employee,"+EmployeeData[0]+" "+EmployeeData[1]+" "+EmployeeData[2]+" "+EmployeeData[3]+" "+EmployeeData[4]));
		
	}
}
