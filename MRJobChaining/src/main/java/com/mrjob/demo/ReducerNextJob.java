package com.mrjob.demo;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
public class ReducerNextJob extends Reducer<Text, IntWritable, Text, IntWritable>
{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context c)throws IOException, java.lang.InterruptedException
    {
    System.out.println("key for Job2 in ReducerNextJob " +"-" + key);
	int characterCount = 0;
	for (IntWritable count : values)
	{ 
		System.out.println("values  for ReducerNextJob is " +"-" +count.get());
	    characterCount += count.get();
	}
	/* emit total count for words starting with character */
	System.out.println("values  for ReducerNextJob is >>>> " +"-" +"\t" +characterCount);
	c.write(key, new IntWritable(characterCount));
    }
}
