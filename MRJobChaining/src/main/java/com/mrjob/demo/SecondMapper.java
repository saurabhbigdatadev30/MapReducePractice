package com.mrjob.demo;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
public class SecondMapper extends Mapper<Text, IntWritable, Text, IntWritable>
{

    @Override
    
    /*Input to Mapper2  ------------->
    * JOHN,1
      LUPA,1
      JOHN,1
      FRANK,1
      FRANK,1
      JOHN,1
      .....
      ..........
      
    */
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
/*Output of Mapper 2 ------------->
 *
* john,1
  lupa,1
  john,1
  frank,1
  frank,1
  john,1
  lupa,1
  .....
  ..........
  
*/