package com.mrjob.demo;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
public class SecondMapper extends Mapper<Text, IntWritable, Text, IntWritable>
{

    @Override
    protected void map(Text key, IntWritable value, Context c)	throws IOException, java.lang.InterruptedException
    {

	/* 
	 * Second Mapper reads in each word and convert all characters in it to lower case 
	 * It emits value as whatever the count of word read in (essentially ONE)
	 */
	String lowerCaseWord = key.toString().toLowerCase();
	c.write(new Text(lowerCaseWord), value);
    }
}
