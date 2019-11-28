package com.mrjob.demo;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
public class XMLMapper extends Mapper<LongWritable, Text, LongWritable, Text>
{
    @Override
    protected void map(LongWritable key, Text value, Context context)throws IOException, java.lang.InterruptedException
    {
    	System.out.println("executing mapper ##############");
	  //identity mapper-  reading and outputting as it is
	    context.write(key, value);
	  	    
	}
}
