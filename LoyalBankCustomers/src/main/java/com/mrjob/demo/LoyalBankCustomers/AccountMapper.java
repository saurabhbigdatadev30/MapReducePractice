package com.mrjob.demo.LoyalBankCustomers;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

//  OMOI808692OZ,1245015582,savings,3667,822,no
public class AccountMapper extends Mapper<LongWritable, Text, Text, Text>
{
    
    @Override
    protected void map(LongWritable key, Text value, Context c)	throws IOException, java.lang.InterruptedException
    {

	String line = value.toString();
	String[] words = line.split(",");

	Text id = new Text(words[0]);       // OMOI808692OZ
	
	Text account = new Text("A," + words[1] + "," + words[3] + "," +words[4] + "," +words[5]);
	
	c.write(id, account);                 // OMOI808692OZ   A,1245015582,3667,822,no
    }
}
