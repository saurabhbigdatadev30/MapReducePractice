package com.mrjob.demo.LoyalBankCustomers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
 // OMOI808692OZ,Allison,Abbott,21,female,Chicago 
public class PersonMapper extends Mapper<LongWritable, Text, Text, Text>
{
    @Override
    protected void map(LongWritable key, Text value, Context c)	throws IOException, java.lang.InterruptedException
    {

    String line = value.toString();
	String[] words = line.split(",");

	Text id = new Text(words[0]);           //  OMOI808692OZ
	
	Text person = new Text("P,"+ words[1]+","+ words[2]);
	c.write(id, person);                 // OMOI808692OZ   P,Allison,Abbott
    }
}

