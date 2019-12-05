package com.mrjob.demo;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
public class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{

    private static final IntWritable ONE = new IntWritable(1);

    @Override
/*
 * * Input file to mapper 
	 *  john,lupa,john,frank,frank,john,kuoa,john,frank,frank,john,frank,frank,lupa,steve
		john,frank,john,kuoa,steve,steve
        lupa,john,steve,kuoa,lexa
        kuoa,steve,smith
 */
    protected void map(LongWritable key, Text value, Context c)	throws IOException, java.lang.InterruptedException
    {

	/* First Mapper reads in each line, split it into words and emit every word */
	String[] words = value.toString().split(",");

	for (String word : words)
	{
	    c.write(new Text(word), ONE);
	}
    }
}
