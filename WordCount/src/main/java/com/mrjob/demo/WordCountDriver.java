package com.mrjob.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class WordCountDriver 
{
    public static void main( String[] args )
    {
		try {
			Path inputPath = new Path(args[0]);
			Path outputDir = new Path(args[1]);
			 System.out.println("************ INSIDE MAIN METHOD OF WordCountDriver **********************");
			 System.out.println("input path" +"\t"+ inputPath +"\t" + "output path" +"\t"+ outputDir);
			  if(args.length !=2){
		          System.err.println("Invalid Command");
		          System.err.println("Usage: WordCount <input path> <output path>");
		          System.exit(0);
			  }
			Configuration conf = new Configuration();
			  System.out.println("************ INSIDE conf**********************");
		      System.out.println("conf>>>>>>>"+ conf);
		      Job job;
			job = new Job(conf);
			job.setJobName("Word Count");
			job.setJarByClass(WordCountDriver.class);
			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
			 FileInputFormat.addInputPath(job, inputPath);
		     FileOutputFormat.setOutputPath(job, new Path(args[1]));
		     FileSystem fs = FileSystem.get(conf);
		     fs.delete(outputDir);
			 job.waitForCompletion(true);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }
}
