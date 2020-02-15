package com.mr.demo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/*  Output of shuffle & sort with contain group by DeptID
     (10 ,[{department,INVENTORY HYDERABAD} { Employee,1781 John Developer 6500 1681 } {Employee,1681 Mira Mgr 5098 1481 } 
            {Employee,1481 flink Mgr 9580 1681} {Employee,1281 Shawn Architect 7890 1481}]) 
     (20, [{Employee,1381 Jacob Admin 4560 1481} {dept,ACCOUNTS PUNE}] 
     (30, [{department,DEVELOPMENT CHENNAI}]  
     (40, [{Employee,1581 Richard Developer 1000 1681}]
   
   
   
 reducer iteration 1 -- (1 deptobj , 4 empobj)
	(10 ,[{department,INVENTORY HYDERABAD} { Employee,1781 John Developer 6500 1681 } {Employee,1681 Mira Mgr 5098 1481 } 
	{Employee,1481 flink Mgr 9580 1681} {Employee,1281 Shawn Architect 7890 1481}]) 
      
  reducer iteration 2  -- (1 deptobj ,1 empobj)
 (20, [{Employee,1381 Jacob Admin 4560 1481} {dept,ACCOUNTS PUNE}] 
	
 reducer iteration 3	-- no emp object for deptID =30 
  (30, [{department,DEVELOPMENT CHENNAI}]  
  
  reducer iteration 4	 -- no dept object for deptID =30 
  (40, [{Employee,1581 Richard Developer 1000 1681}]
  
 */

public class EmpDeptReducer extends Reducer<Text, Text, Text, Text> {

	
	public void reduce(Text DeptNumber, Iterable<Text> Values, Context context)throws IOException, InterruptedException {

	/*
		10 [
	     {department,INVENTORY HYDERABAD} { Employee,1781 John Developer 6500 1681 } 
	     {Employee,1681 Mira Mgr 5098 1481 }{Employee,1281 Shawn Architect 7890 1481} {Employee,1481 flink Mgr 9580 1681}]
	   */
		List<String> Employee_List = new ArrayList<String>(); //
		String Department = "";
		Iterator<Text> Itr = Values.iterator(); // declaring iterator for incoming list of values
		// iterating on Value[] 
		 while(Itr.hasNext()) {
		 Text data = Itr.next();   //{department,INVENTORY HYDERABAD}
			String NewRecord[] = data.toString().split(","); 
			if (NewRecord[0].equalsIgnoreCase("Employee")) {
				Employee_List.add(NewRecord[1]);  // 
		// in the first iteration  value[0]=   department,INVENTORY HYDERABAD 
			} else if (NewRecord[0].equalsIgnoreCase("department")) {
				Department = NewRecord[1]; // INVENTORY HYDERABAD is added in the Departmemt string
			}
		}
		
		   //  Employee_List &Department are now populated .... so now running the join condition
		  //   Condition for Inner join i.e both data structure should be non empty for a particular key	 
		
		 if (!Employee_List.isEmpty() && !Department.isEmpty())  
		{
			for (String Empdata : Employee_List) {
				context.write(DeptNumber, new Text(Empdata + " " + Department)); 
			}
		}
		
       // Condition for Left Outer Join  i.e Dept ID is present in emplist  but not in Department data structure
		
		if (!Employee_List.isEmpty() && Department.isEmpty()) 
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
	
	/*
Understanding Output
10	1781 John Developer 6500 1681 INVENTORY HYDERABAD
10	1681 Mira Mgr 5098 1481 INVENTORY HYDERABAD
10	1481 flink Mgr 9580 1681 INVENTORY HYDERABAD
10	1281 Shawn Architect 7890 1481 INVENTORY HYDERABAD
20	1381 Jacob Admin 4560 1481 ACCOUNTS PUNE
30	null_value null_value null_value null_value null_value DEVELOPMENT CHENNAI
40	1581 Richard Developer 1000 1681 null_value null_value




If you want left outer join then comment the section the right outer join
if (Employee_List.isEmpty() && !Department.isEmpty()) {


Left Outer Join
[cloudera@quickstart ~]$ hdfs dfs -cat /output_EmployeeDepartmentJoin/part-r-00000
10	1781 John Developer 6500 1681 INVENTORY HYDERABAD
10	1681 Mira Mgr 5098 1481 INVENTORY HYDERABAD
10	1481 flink Mgr 9580 1681 INVENTORY HYDERABAD
10	1281 Shawn Architect 7890 1481 INVENTORY HYDERABAD
20	1381 Jacob Admin 4560 1481 ACCOUNTS PUNE
40	1581 Richard Developer 1000 1681 null_value null_value
	 */
	
	
	
}
