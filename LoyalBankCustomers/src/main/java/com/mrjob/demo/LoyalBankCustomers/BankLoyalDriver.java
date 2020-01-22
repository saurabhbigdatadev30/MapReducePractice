package com.mrjob.demo.LoyalBankCustomers;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//chmod 777 /home/cloudera/BankLoyalDriver.jar
//sudo -u hdfs  hadoop jar BankLoyalDriver.jar /user/cloudera/account  /user/cloudera/person /output_BankLoyaCustomer
public class BankLoyalDriver {
	public static void main(String[] args) 
    {
		try {
    	Path accountPath = new Path(args[0]);
		Path personPath =  new Path(args[1]);
		Path outputDir =   new Path(args[2]);
	System.out.println("************ INSIDE MAIN METHOD OF BankLoyalDriver **********************");
	System.out.println("args is" +"," +accountPath + "," + personPath +","+ outputDir);
	 if(args.length !=3){
         System.err.println("Invalid Command");
         System.err.println("Usage: BankLoyalDriver <accountPath> <personPath> <outputDir>");
         System.exit(0);
	  }
	Configuration conf = new Configuration();
	Job job = new Job(conf, "Bank Customer Loyality");
	job.setJarByClass(BankLoyalDriver.class);
	// 2 mappers 1 reducer
	job.setMapperClass(AccountMapper.class);
	job.setMapperClass(PersonMapper.class);
	job.setReducerClass(BankReducer.class);
    // mapper output key
	job.setMapOutputKeyClass(Text.class);
	// mapper output value
	job.setMapOutputValueClass(Text.class);
	// reducer output key
	job.setOutputKeyClass(Text.class);
	// reducer output value
	job.setOutputValueClass(Text.class);

	MultipleInputs.addInputPath(job, accountPath,  TextInputFormat.class,  AccountMapper.class);
	MultipleInputs.addInputPath(job, personPath,   TextInputFormat.class,  PersonMapper.class);

	FileOutputFormat.setOutputPath(job, outputDir);
	outputDir.getFileSystem(job.getConfiguration()).delete(outputDir,true);
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
	catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}
}