//G:\BIG-DATA\ClouderaExport\Hadoop\ExportedEclipseJAR
package com.mrjob.demo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class MapreducerChaining {
	 public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	    {

		 Path inputPath = new Path(args[0]);
		 Path outputDir = new Path(args[1]);

		 Configuration conf = new Configuration();
		 Job job = new Job(conf, "Chain Job1");
		 System.out.println("************ INSIDE MAIN METHOD OF MapreducerChaining## **********************");
	     System.out.println("Input args is" + "---"+ inputPath );
	     System.out.println("Output args is" + "---"+ outputDir );
		  System.out.println("************ INSIDE conf**********************");
	      System.out.println("conf>>>>>>>"+ conf);
		job.setJarByClass(MapreducerChaining.class);
		/* add multiple chained mappers to run for this job */
		ChainMapper.addMapper(job,
				      FirstMapper.class, /* Mapper to add to chain */
				      LongWritable.class, /* Mapper input Key */
				      Text.class, /* Mapper input value */
				      Text.class, /* Mapper output key*/
				      IntWritable.class, /* Mapper output value */
				      conf /* job configuration to use */
				      );

		/* set second mapper in the chain */
		ChainMapper.addMapper(job,
				      SecondMapper.class, /* Mapper to add to chain */
				      Text.class, /* Mapper input Key */
				      IntWritable.class, /* Mapper input value */
				      Text.class, /* Mapper output key*/
				      IntWritable.class, /* Mapper output value */
				      conf /* job configuration to use */
				      );

		/* set reducer for the chain */
		ChainReducer.setReducer(job,
				    FirstReducer.class, /* Reducer to add to chain */
					Text.class, /* Reducer input Key */
					IntWritable.class, /* Reducer input value */
					Text.class, /* Reducer output key*/
					IntWritable.class, /* Reducer output value */
					conf /* job configuration to use */
					);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(outputDir + "_job1"));
		System.out.println("******* Job1 writting path ******" +FileOutputFormat.getOutputPath(job));
		outputDir.getFileSystem(job.getConfiguration()).delete(new Path(outputDir + "_job1"), true);

		/* run first MR */
		if (!job.waitForCompletion(true))
		{
		    System.out.println("ERROR completing first job");
		    System.exit(1);
		}

		// output of MR job 1 - outputDir + "_job1/part-r-00000"  --- is input to Job2 
		
		
		/* *******************  Job 2 ****************************  */
		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "Chain Job2");

		job2.setJarByClass(MapreducerChaining.class);
		job2.setMapperClass(MapperNextJob.class);
		job2.setReducerClass(ReducerNextJob.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		/* Set input path: Read from output of first MR */
		FileInputFormat.addInputPath(job2, new Path(outputDir + "_job1/part-r-00000"));
		System.out.println("******* Job2 input path ******"+FileOutputFormat.getOutputPath(job));
		FileOutputFormat.setOutputPath(job2, outputDir);
		outputDir.getFileSystem(job2.getConfiguration()).delete(outputDir,true);

		/* run second MR */
		job2.waitForCompletion(true);
	    }
}
