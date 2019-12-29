package com.mrjob.demo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
      
    /* input to the reducer
     * Word1 , {1,1,1,1,}
     * Word2 , {1,1,1,..........}
     * Word3 , {1,1,1,1,1........}
     * Word4 , {1,1,1,1,1,1,1.....}
    
  		No of reduce() invoked = no of unique key 
  		i.e on every key reducer is invoked once  
  		i.e Word1 = reduce [n=1]   
      	Word1 , {1,1,1,1,..} key = word
      	Iterable<IntWritable> values = {1,1,1,1,1......}
     */
    	public void reduce(Text key, Iterable<IntWritable> values, Context context) {
        try {
        	System.out.println("input to reducer key is " + "\t" + key); //Word1
        	 int count = 0;
             for (IntWritable value:values) //{1,1,1,1,}
             {
            	 System.out.println("input to reducer value is>>> " + "\t" + value);
            	 count += value.get();
             }
			context.write(key, new IntWritable(count));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

    }
}
