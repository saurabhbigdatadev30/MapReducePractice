package com.mrjob.demo;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// Jar location is 
//G:\BIG-DATA\ClouderaExport\Hadoop\ExportedEclipseJAR

public class XMLDriverCustomInputFormat {
	 public static void main(String[] args) 
	    {
		    Path inputPath = new Path(args[0]);
			Path outputDir = new Path(args[1]);
	     
	     System.out.println("************ INSIDE MAIN METHOD OF XMLDriver## **********************");
	     System.out.println("args is" + "---"+ inputPath );
		  Configuration conf = new Configuration();
		  System.out.println("************ INSIDE conf**********************");
	      System.out.println("conf>>>>>>>"+ conf);
		Job job;
		try {
			job = new Job(conf);
			job.setJobName("XML Reader");
			job.setInputFormatClass(XMLInputFormat.class);  //set your own input format class
			// name of driver class
			job.setJarByClass(XMLDriverCustomInputFormat.class);
			// name of mapper class
			job.setMapperClass(XMLMapper.class);
			
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, inputPath);
			FileOutputFormat.setOutputPath(job, outputDir);
			
			outputDir.getFileSystem(job.getConfiguration()).delete(outputDir,true);

			job.waitForCompletion(true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
}
}