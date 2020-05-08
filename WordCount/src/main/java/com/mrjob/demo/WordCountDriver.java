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
			Path outputPath = new Path(args[1]);
			 System.out.println("************ INSIDE MAIN METHOD OF WordCountDriver **********************");
			 System.out.println("input path" +"\t"+ inputPath +"\t" + "output path" +"\t"+ outputPath);
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
			job.setMapOutputKeyClass(Text.class);    // mapper output (key)
			job.setMapOutputValueClass(IntWritable.class); // mapper output (Value)
			job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
			 FileInputFormat.addInputPath(job, inputPath);
		     FileOutputFormat.setOutputPath(job, outputPath);
		     FileSystem fs = FileSystem.get(conf);
		     fs.delete(outputPath);
			 job.waitForCompletion(true);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }
}
/*
Input file data :--------------
xxx often bad yyy ddd ccc acd often so bbb that we ccc xxx xxx xxx aaa perform index construction efficiently on a single machine aaa xxx aaa aaa aaa acd bad

Output 
a	1
aaa	5
acd	2
bad	2
bbb	1
ccc	2
construction	1
ddd	1
efficiently	1
index	1
machine	1
often	2
on	1
perform	1
single	1
so	1
that	1
we	1
xxx	5
yyy	1


 * 
*/
