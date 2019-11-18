package com.mr.demo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EmpDeptReducer extends Reducer<Text, Text, Text, Text> {

	
	/*
	output of shuffle & sort is - group by DeptID 
    deptID =10 , Iterable<Text> Values = contains  4 employee records corrosponding to deptID = 10 in dept file  
    10 [
    {department,INVENTORY HYDERABAD} { Employee,1781 John Developer 6500 1681 } {Employee,1681 Mira Mgr 5098 1481 } {Employee,1481 flink Mgr 9580 1681} {Employee,1281 Shawn Architect 7890 1481}
    ]
    input to reducerKEY-10+||department,INVENTORY HYDERABAD
	input to reducerKEY-10+||Employee,1781 John Developer 6500 1681
	input to reducerKEY-10+||Employee,1681 Mira Mgr 5098 1481
	input to reducerKEY-10+||Employee,1481 flink Mgr 9580 1681
	input to reducerKEY-10+||Employee,1281 Shawn Architect 7890 1481
	
	i.e  deptID =20 , Iterable<Text> Values = contains contains  1 employee records corrosponding to deptID = 20 in dept file
	20  [ 
	{Employee,1381 Jacob Admin 4560 1481} {dept,ACCOUNTS PUNE} 
	]
	input to reducerKEY-20+||Employee,1381 Jacob Admin 4560 1481
    input to reducerKEY-20+||department,ACCOUNTS PUNE
	
	 uncommon records ... 
	 deptID -30 , only in deptfile, not in employee file
	input to reducerKEY-30+||department,DEVELOPMENT CHENNAI
	deptId -40 , only in Employee file. not in in deptfile
	input to reducerKEY-40+||Employee,1581 Richard Developer 1000 1681
	
	 * */
	
	
	public void reduce(Text DeptNumber, Iterable<Text> Values, Context context)throws IOException, InterruptedException {
		//Primary key - deptID  . Values -  will have employee tuple & department tuple for that id
	/*
		10 [
	     {department,INVENTORY HYDERABAD} { Employee,1781 John Developer 6500 1681 } {Employee,1681 Mira Mgr 5098 1481 }{Employee,1281 Shawn Architect 7890 1481} {Employee,1481 flink Mgr 9580 1681}
	    ]
	    */
		List<String> Employee_List = new ArrayList<String>(); //
		String Department = "";
		Iterator<Text> Itr = Values.iterator(); // declaring iterator for incoming list of values
		// Value[] -- contains 
		// value[0]=   department,INVENTORY HYDERABAD 
		// value[1] =  Employee,1781 John Developer 6500 1681 
		// value[2]=   Employee,1681 Mira Mgr 5098 1481
		// value[3] =  Employee,1281 Shawn Architect 7890 1481
		// value[4] =  Employee,1481 flink Mgr 9580 1681
		
		// iterating on Value[] 
		 while(Itr.hasNext()) {
		Text data = Itr.next();  
		System.out.println("input to reducer"  + "KEY"+"-"+DeptNumber+  "+" +"||"  +data);
		// data contains 5 elements value[0].... value[4]
		
			String NewRecord[] = data.toString().split(","); // NewRecord = [ department,INVENTORY HYDERABAD  ]
			if (NewRecord[0].equalsIgnoreCase("Employee")) {
				Employee_List.add(NewRecord[1]);  // 
		// in the first iteration  value[0]=   department,INVENTORY HYDERABAD 
			} else if (NewRecord[0].equalsIgnoreCase("department")) {
				Department = NewRecord[1]; // INVENTORY HYDERABAD is added in the Departmemt string
			}
		}
        
		/*
		 * for reducer n= 1 => deptID =10   
		
		 * the Employee_List contains 
		 	 1781 John Developer 6500 1681 
			 1681 Mira Mgr 5098 1481
		     1281 Shawn Architect 7890 1481
		     
		 * while Department = INVENTORY HYDERABAD
		   ______________________________________
		   
		   for reducer n=2 => deptID =20 
		   20  [ 
				{Employee,1381 Jacob Admin 4560 1481} {dept,ACCOUNTS PUNE} 
				]
		     (Itr.hasNext()) 
		   		
		    the Employee_List contains Employee,1381 Jacob Admin 4560 1481
		   while Department =  ACCOUNTS PUNE
		 */
		
		for (String Empdata : Employee_List) {
			System.out.println("Emplist" + "\t" + Empdata);
		}
		
		
		System.out.println("Department data is " + "\t" + Department);
		
		if (!Employee_List.isEmpty() && !Department.isEmpty()) // Condition for
																// Inner join
		{
			for (String Empdata : Employee_List) {
				context.write(DeptNumber, new Text(Empdata + " " + Department)); // output
			}
		}

		if (!Employee_List.isEmpty() && Department.isEmpty()) // Condition for
																// Left Outer
																// Join join
		{
			for (String Empdata : Employee_List) {
				context.write(DeptNumber, new Text(Empdata + " "
						+ "null_value null_value")); // output
			}
		}
		// Condition for Right Outer join

		if (Employee_List.isEmpty() && !Department.isEmpty()) {
			context.write(DeptNumber, new Text(
					"null_value null_value null_value null_value null_value"
							+ " " + Department)); // output
		}
	}
}
