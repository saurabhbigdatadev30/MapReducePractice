package com.mrjob.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FaceBookAnalysisMainMavenV1 {
 public static void main(String[] args) {
	System.out.println("************ INSIDE MAIN METHOD OF FaceBookAnalysisMain **********************");
	  if(args.length !=2){
          System.err.println("Invalid Command");
          System.err.println("Usage: WordCount <input path> <output path>");
           System.exit(0);
	  }
	  Configuration conf = new Configuration();
	  System.out.println("************ INSIDE conf**********************");
      System.out.println("conf>>>>>>>"+ conf);
      Job job;
	try {
		
    job = new Job(conf);
    job.setJarByClass(FaceBookAnalysisMainMavenV1.class); 
    job.setJobName("Facebook Analysis");
	// name of mapper class
	job.setMapperClass(FaceBookMapper.class);
	// name of reducer class
	job.setReducerClass(FaceBookReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	 FileInputFormat.addInputPath(job, new Path(args[0]));
     FileOutputFormat.setOutputPath(job, new Path(args[1]));
     FileSystem fs = FileSystem.get(conf);
     fs.delete(new Path(args[1]));
	 job.waitForCompletion(true);
	
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
}
}

