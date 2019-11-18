package com.mrjob.demo;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

// To calculate avg success rate of particular advertisement category , location wise
/* input file format
FKLY490998LB,2010-01-29 06:12:17,Mumbai,Ecommerce,39,13,25-35
NIXK800977XM,2010-01-03 00:01:20,Mumbai,Ecommerce,281,5,18-25
CSUR861665UU,2010-01-02 01:41:16,Hyderabad,Health,234,12,45-50
YYZI277313ZG,2010-01-03 17:21:23,Hyderabad,Health,465,9,18-25
XCUI929329UU,2010-01-30 09:22:51,Delhi,Ecommerce,341,9,35-40


//The input file contains following header info
	//Advertisement ID | Date of display of Advertisement | Place of display | Category of Advertisement | No of clicks | No of success | Age Group 
	//FKLY490998LB,      2009-12-29 06:12:17,                     Mumbai,             Ecommerce,                 39,              13,            25-35
	 
	 Calculate average success rate of particular Category of Advertisement as Place of display
 */

public class FaceBookMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	
	  @Override
	    protected void map(LongWritable key, Text value, Context c)	throws IOException, java.lang.InterruptedException
	    {
       
		String line = value.toString();          // FKLY490998LB,2010-01-29 06:12:17,Mumbai,Ecommerce,39,13,25-35
		/* Split csv string */
		String[] words = line.split(",");        // [ {FKLY490998LB} {2009-12-29 06:12:17} {Mumbai} {Ecommerce} {39} {13} {25-35} ]

		// Category of Advertisement is made the key , while city of diplay, no of clicks, no 
		System.out.println( "mapper output" + "\t" + ""+ words[3] + "\t"+"" +(words[2] + "," + words[4] + "," +  words[5]));
		c.write(new Text(words[3]), new Text (words[2] + "," + words[4] + "," +  words[5]));
	    }     // Ecommerce                   Mumbai        ,   39          ,      13  
}


/* in shuffle and sort group by category
Ecommerce    [ { Mumbai,39,13 }  Mumbai,281,5 } {Delhi,341,9} {Delhi,398,10}................]
Health       [Hyderabad ,234,12 , ................. ]


*/