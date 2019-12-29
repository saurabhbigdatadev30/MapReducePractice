package com.mrjob.demo;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

	@Override
     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		String[] words =line.split(" ");
			for (String word : words){
				if(word.length()>0){
					context.write(new Text(word),new IntWritable(1));
        }
    }
}
}
