package com.mrjob.demo;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
public class MapperNextJob extends Mapper<LongWritable, Text, Text, IntWritable>
{
/*
	Output of reducer 1, 
	frank	7
	john	8
	kuoa	4
	lexa	1
	lupa	3
	smith	1
	steve	5

This will be written in temporary file , from which job2 Mapper needs to be  read ... 	
	
When Job 2 mapper will read,it will read from the file, where the temporary output of job1 is written
Since the mapper of Job 2 reads the the output of previous job so key = byte offset & value = line i.e => frank	7


key =0  Value  =frank	7
key =8  Value  =john	8
key =15 Value  =kuoa	4
key =22 Value  =lexa	1
key =29 Value  =lupa	3 
key =36 Value  =smith	1
key =44 Value  =steve	5 
	

For mappper 1 - input
key =0 (byte offset)
Value  = frank	7

For mappper 2 - input 
key =8 (byte offset)
Value =john	8

mappper 3 - input
key =15 (byte offset)
Value  =kuoa	4

mappper 4 - input
key =22
Value  =lexa	1

mappper 5 - input
key =29
Value  =lupa	3

mappper 6 - input
key =36
Value  =smith	1

mappper 7 - 
key =44
Value =steve	5
	
	
	
*/	
	
    @Override
    protected void map(LongWritable key, Text value, Context c)throws IOException, java.lang.InterruptedException
    {
    	// 0 | frank	7  m1()
    	// 8 | john	    8  m2()
    	// 15| kuoa	    4  m3 ()
     System.out.println("key for Job2" +"-" + key);
     System.out.println("Value for job 2"+ " "+  value);
	String[] words = value.toString().split("\\s+"); // splitting on basis of space i.e input=> frank	7
	// for map1() - words[0]= frank & words[1]= 7
	String firstCharacter = words[0].substring(0, 1); //f
	c.write(new Text(firstCharacter), new IntWritable(Integer.parseInt(words[1])));
	           //    f  7
	           //    j  8
	           //    k  4
	
    }
}
