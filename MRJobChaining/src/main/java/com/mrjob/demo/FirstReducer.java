package com.mrjob.demo;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{

	/*Input file to mapper 
	 *  john,lupa,john,frank,frank,john,kuoa,john,frank,frank,john,frank,frank,lupa,steve
		john,frank,john,kuoa,steve,steve
        lupa,john,steve,kuoa,lexa
        kuoa,steve,smith
	 * 
	 * 
	 * 
	 *    
	 * Input file to reducer  =>key - lowercase words , value = array =>
	 * (lowercaseword1 = john , [1,1,1,1....])
	 * (lowercaseword2 = lupa , [1,1,1,1....])
	 * (lowercaseword3 , [1,1,1,1....])
	 * (lowercaseword4 , [1,1,1,1....])
	 */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context c)throws IOException, java.lang.InterruptedException
    {
    /* 
     * reduce n =1  (lowercaseword1 = john , [1,1,1,1....])
     * reduce n= 2  (lupa , [1,1,1,1....])
     */
	int totalWordFrequency = 0;
	for (IntWritable count : values)
	{
	    totalWordFrequency += count.get();
	}
	/* emit total frequency for each word */
	System.out.println("*****");
	System.out.println(key + "----" + totalWordFrequency  );	
	c.write(key, new IntWritable(totalWordFrequency));
	/*
	 * output of reducer 1
	 * (lowercaseword1 , 32)
	 * (lowercaseword2 , 12)
	 * (lowercaseword3 , 11)
	 * (lowercaseword4 , 7)
	 * 
	 * 
	 * *****************   SUMMARY ***********************
	 * 
	 

Output of reducer 1  , this will be written in temporary file , from which job2 will read ... 
	frank	7
	john	8
	kuoa	4
	lexa	1
	lupa	3
	smith	1
	steve	5
	
	 */
	
	
	
	
	
	
	
	
	
    }
}
