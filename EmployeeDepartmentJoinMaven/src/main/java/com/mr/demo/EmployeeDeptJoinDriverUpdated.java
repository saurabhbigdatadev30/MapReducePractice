package com.mr.demo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class EmployeeDeptJoinDriverUpdated {
	public static void main(String[] args) {
		
	
	try {
		Path employeeFilePath = new Path(args[0]);
		Path deptFilePath =     new Path(args[1]);
		Path OutputDirectory =  new Path(args[2]);
		 System.out.println("************ INSIDE MAIN METHOD OF Employee Dept Join Driver **********************");
		 Job job;
		 System.out.println("args is" +"," +employeeFilePath + "," + deptFilePath +","+ OutputDirectory);
		  if(args.length !=3){
	          System.err.println("Invalid Command");
	          System.err.println("Usage: WordCount <employeeFilePath> <departmentFilePath> <OutputDirectory>");
	          System.exit(0);
		  }
		
		  Configuration C1 = new Configuration();
		  System.out.println("************ INSIDE conf**********************");
	      System.out.println("conf>>>>>>>"+ C1);
	      job = new Job(C1);
		  job.setJobName("Join Analysis");

		// name of driver class 
		job.setJarByClass(EmployeeDeptJoinDriverUpdated.class);
		//name of Mapper1 class 
		job.setMapperClass(EmployeeMapper.class);
		// name of Mapper2 class 
		job.setMapperClass(DepartmentMapper.class);
		// name of Reducer class 
		job.setReducerClass(EmpDeptReducer.class);

		job.setMapOutputKeyClass(Text.class);  // deptId
		job.setMapOutputValueClass(Text.class); // Employee & Dept tuple
		job.setOutputKeyClass(Text.class);  // deptId
		job.setOutputValueClass(Text.class);  // empList + Department

		MultipleInputs.addInputPath(job, employeeFilePath, TextInputFormat.class,EmployeeMapper.class);   // employee file will processed by Emp_Mapper
		MultipleInputs.addInputPath(job, deptFilePath, TextInputFormat.class, DepartmentMapper.class);    // dept file will processed by Dep_Mapper
		FileOutputFormat.setOutputPath(job, OutputDirectory);
		OutputDirectory.getFileSystem(job.getConfiguration()).delete(OutputDirectory,true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
	
	catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
}
}