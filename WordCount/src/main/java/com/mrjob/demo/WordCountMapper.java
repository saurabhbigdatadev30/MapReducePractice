package com.mrjob.demo;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

	  /* No of mapper is equal to  no of splits i.e on 128 MB data block 
	 
	  map(LongWritable key, Text value, Context context) is invoked on every line of the split i.e 
	  
	  		Line 1 => invoke  map(LongWritable key, Text value, Context context). 	 key = byte offset , value = line 1
	  		Line 2 => invoke  map(LongWritable key, Text value, Context context). 	 key = byte offset , value = line 2
			Line 3 => invoke  map(LongWritable key, Text value, Context context).	 key = byte offset , value = line 3
			.......................................................................................................
			.......................................................................................................
			.......................................................................................................
			.......................................................................................................
	  		Line n => invoke  map(LongWritable key, Text value, Context context) key = byte offset , value = line n
	 
	 */
	
	@Override
     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	  {
			String line = value.toString();
			String[] words = line.split(" ");
				for (String word : words){
					if(word.length()>0){
						context.write(new Text(word),new IntWritable(1));
        }
    }
}
}
/* Output of Mapper ------------------------             

 
Example 
invoke map(LongWritable key, Text value, Context context) on line 1 
Line 1 => [1, (word1 word2 word3)]

String[] words = word1 word2 word3
context.write(word1,1)
context.write(word2,1)
context.write(word3,1)

invoke    map(LongWritable key, Text value, Context context)on line 2 
Line 2 => [13, (word4 word5 )]
String[] words = word4 word5
context.write(word4,1)
context.write(word5,1)




 * (Word1,1)
   (Word2,1)
 * (Word3,1)
 * (Word1,1)
 * (Word4,1)
 * (Word1,1)
 * (Word1,1)
 * (Word2,1)
 * (Word4,1)
 * (Word4,1)
 * 
 * 
 * /* Output of merge/sort()
     * Word1 , {1,1,1,1,}
     * Word2 , {1,1,1,..........}
     * Word3 , {1,1,1,1,1........}
     * Word4 , {1,1,1,1,1,1,1.....}
 * 
 */
