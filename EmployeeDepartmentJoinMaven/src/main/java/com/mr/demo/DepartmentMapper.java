package com.mr.demo;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class DepartmentMapper extends Mapper<LongWritable, Text, Text, Text>
{
	//  10,Inventory,HYDERABAD 
	/*
	10,INVENTORY,HYDERABAD
	20,ACCOUNTS,PUNE
	30,DEVELOPMENT,CHENNAI
	 */	
	public void map(LongWritable key, Text V1, Context context) throws IOException,InterruptedException 
	{
		String line = V1.toString().trim();      //  line =   10,Inventory,HYDERABAD 
		String[] depttokens = line.split(",");       // depttokens = [{10} {Inventory} {HYDERABAD}]
		System.out.println("Department Mapper" + "\t" + depttokens[0] + ""+ depttokens[1]);
		context.write(new Text(depttokens[0]), new Text("department,"+depttokens[1]+" "+depttokens[2]));
	}

}
