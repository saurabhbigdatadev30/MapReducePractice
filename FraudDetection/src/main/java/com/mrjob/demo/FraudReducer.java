package com.mrjob.demo;
import java.util.*;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/*-------------------------------------------------------------------------------------------------------------------
 * input to reducer 
 * (customerID1, [FraudWritableobj1, FraudWritableobj2, FraudWritableobj3, FraudWritableobj4,.....] )
 * (customerID2, [FraudWritableobj1, FraudWritableobj2, FraudWritableobj3, FraudWritableobj4,.....] )
 * (customerID3, [FraudWritableobj1, FraudWritableobj2, FraudWritableobj3, FraudWritableobj4,.....] )
 * ------------------------------------------------------------------------------------------------------------------
 * 
 * for  Reducer input Key :-BGHY284294HR, 
Iterable<FraudWritable> values - contains array of FraudWritable object as below .
Reducer input value is 	FraudWritable [customerName=Josephine, receiveDate=09-01-2017, returned=true, returnDate=17-01-2017]
Reducer input value is 	FraudWritable [customerName=Josephine, receiveDate=09-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Josephine, receiveDate=14-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Josephine, receiveDate=08-01-2017, returned=true, returnDate=20-01-2017]
Reducer input value is 	FraudWritable [customerName=Josephine, receiveDate=08-01-2017, returned=true, returnDate=10-01-2017]
Reducer input value is 	FraudWritable [customerName=Josephine, receiveDate=06-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Josephine, receiveDate=06-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Josephine, receiveDate=14-01-2017, returned=true, returnDate=14-01-2017]
----------------------------------------------------------------------------------------------------------------------------
for  Reducer input Key :-BHEE999914ED, 
Iterable<FraudWritable> values - contains array of FraudWritable object as below .
Reducer input value is 	FraudWritable [customerName=Ana, receiveDate=13-01-2017, returned=true, returnDate=05-02-2017]
Reducer input value is 	FraudWritable [customerName=Ana, receiveDate=13-01-2017, returned=true, returnDate=14-01-2017]
Reducer input value is 	FraudWritable [customerName=Ana, receiveDate=08-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Ana, receiveDate=10-01-2017, returned=true, returnDate=17-01-2017]
Reducer input value is 	FraudWritable [customerName=Ana, receiveDate=26-01-2017, returned=true, returnDate=12-02-2017]
Reducer input value is 	FraudWritable [customerName=Ana, receiveDate=06-01-2017, returned=true, returnDate=10-01-2017]
Reducer input value is 	FraudWritable [customerName=Ana, receiveDate=14-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Ana, receiveDate=10-01-2017, returned=true, returnDate=17-01-2017]
----------------------------------------------------------------------------------
for  Reducer input Key :--BPLA457837LB
Iterable<FraudWritable> values - contains array of FraudWritable object as below 
Reducer input value is 	FraudWritable [customerName=Alex, receiveDate=09-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Alex, receiveDate=06-01-2017, returned=true, returnDate=09-01-2017]
Reducer input value is 	FraudWritable [customerName=Alex, receiveDate=13-01-2017, returned=true, returnDate=22-01-2017]
Reducer input value is 	FraudWritable [customerName=Alex, receiveDate=15-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Alex, receiveDate=08-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Alex, receiveDate=08-01-2017, returned=true, returnDate=18-01-2017]
Reducer input value is 	FraudWritable [customerName=Alex, receiveDate=06-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Alex, receiveDate=13-01-2017, returned=false, returnDate=null]
----------------------------------------------------------------------------------------------------------
for  Reducer input Key :--
Reducer input Key :-CCWO777171WT
Iterable<FraudWritable> values - contains array of FraudWritable object as below
Reducer input value is 	FraudWritable [customerName=Arthur, receiveDate=16-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Arthur, receiveDate=31-01-2017, returned=true, returnDate=01-02-2017]
Reducer input value is 	FraudWritable [customerName=Arthur, receiveDate=15-01-2017, returned=true, returnDate=16-01-2017]
Reducer input value is 	FraudWritable [customerName=Arthur, receiveDate=10-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Arthur, receiveDate=13-01-2017, returned=true, returnDate=24-01-2017]
Reducer input value is 	FraudWritable [customerName=Arthur, receiveDate=13-01-2017, returned=true, returnDate=09-02-2017]
Reducer input value is 	FraudWritable [customerName=Arthur, receiveDate=26-01-2017, returned=true, returnDate=02-02-2017]
Reducer input value is 	FraudWritable [customerName=Arthur, receiveDate=16-01-2017, returned=true, returnDate=21-01-2017]
Customer ID	CCWO777171WTCustomer Name	ArthurFraud Point is	12
Reducer input Key :-CVYO477759YX
Reducer input value is 	FraudWritable [customerName=Dean, receiveDate=13-01-2017, returned=true, returnDate=22-01-2017]
Reducer input value is 	FraudWritable [customerName=Dean, receiveDate=07-01-2017, returned=true, returnDate=08-01-2017]
Reducer input value is 	FraudWritable [customerName=Dean, receiveDate=09-01-2017, returned=true, returnDate=12-01-2017]
Reducer input value is 	FraudWritable [customerName=Dean, receiveDate=06-01-2017, returned=true, returnDate=06-01-2017]
Reducer input value is 	FraudWritable [customerName=Dean, receiveDate=15-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Dean, receiveDate=07-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Dean, receiveDate=16-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Dean, receiveDate=08-01-2017, returned=true, returnDate=10-01-2017]
Customer ID	CVYO477759YXCustomer Name	DeanFraud Point is	10
Reducer input Key :-FJCH868412CB
Reducer input value is 	FraudWritable [customerName=Ivan, receiveDate=08-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Ivan, receiveDate=08-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Ivan, receiveDate=07-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Ivan, receiveDate=07-01-2017, returned=true, returnDate=17-01-2017]
Reducer input value is 	FraudWritable [customerName=Ivan, receiveDate=07-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Ivan, receiveDate=14-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Ivan, receiveDate=06-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Ivan, receiveDate=07-01-2017, returned=false, returnDate=null]
Customer ID	FJCH868412CBCustomer Name	IvanFraud Point is	0
Reducer input Key :-GFKE380824KM
Reducer input value is 	FraudWritable [customerName=Rene, receiveDate=15-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Rene, receiveDate=08-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Rene, receiveDate=08-01-2017, returned=true, returnDate=20-01-2017]
Reducer input value is 	FraudWritable [customerName=Rene, receiveDate=13-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Rene, receiveDate=13-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Rene, receiveDate=06-01-2017, returned=true, returnDate=11-01-2017]
Reducer input value is 	FraudWritable [customerName=Rene, receiveDate=15-01-2017, returned=true, returnDate=19-01-2017]
Reducer input value is 	FraudWritable [customerName=Rene, receiveDate=13-01-2017, returned=false, returnDate=null]
Customer ID	GFKE380824KMCustomer Name	ReneFraud Point is	1
Reducer input Key :-GGYZ333519YS
Reducer input value is 	FraudWritable [customerName=Allison, receiveDate=07-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Allison, receiveDate=13-01-2017, returned=true, returnDate=15-01-2017]
Reducer input value is 	FraudWritable [customerName=Allison, receiveDate=06-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Allison, receiveDate=27-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Allison, receiveDate=08-01-2017, returned=true, returnDate=06-02-2017]
Reducer input value is 	FraudWritable [customerName=Allison, receiveDate=07-01-2017, returned=false, returnDate=null]
Reducer input value is 	FraudWritable [customerName=Allison, receiveDate=09-01-2017, returned=true, returnDate=18-01-2017]
Reducer input value is 	FraudWritable [customerName=Allison, receiveDate=26-01-2017, returned=true, returnDate=16-02-2017]
Customer ID	GGYZ333519YSCustomer Name	AllisonFraud Point is	12
 */


public class FraudReducer extends Reducer<Text, FraudWritable, Text, IntWritable>
{
    ArrayList<String> customers = new ArrayList<String>();
    
    //(customerID1, [FraudWritableobj1, FraudWritableobj2, FraudWritableobj3, FraudWritableobj4,.....] )
    @Override
    protected void reduce(Text key, Iterable<FraudWritable> values, Context c)	throws IOException, java.lang.InterruptedException
    {
	int fraudPoints = 0;
	int returnsCount = 0;
	int ordersCount = 0;

	FraudWritable data = null;        
	System.out.println("Reducer input Key " + ":-" +"" +""  +key );
	Iterator<FraudWritable> valuesIter = values.iterator();
	
	while (valuesIter.hasNext())
	{
	    data = valuesIter.next();  //customerName=Josephine, receiveDate=09-01-2017, returned=true, returnDate=17-01-2017 
	    ordersCount++;  // increment order count
	    System.out.println("Reducer input value is "+ "\t" + ""+ data.toString());
	    if (data.getReturned())
	    {
		returnsCount++;    // increment return count                        
		try
		{
		    SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
		    Date receiveDate = sdf.parse(data.getReceiveDate());
		    Date returnDate = sdf.parse(data.getReturnDate());
		    long diffInMillies = Math.abs(returnDate.getTime() - receiveDate.getTime());
		    long diffDays = TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);     
		    
		    /* assign 1 fraud point to a customer whose (refund_date - receiving_date) > 10 days */
		    if (diffDays > 10)
			fraudPoints++;            // fraudPoints  12
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	    }
	}
 // iteration on all the transactions  for a userID is complete and the vars returnsCount , ordersCount ,FP for transaction is set
// Check if  return rate is more than 50% &  add 10 fraud points to the customer whose
	double returnRate = (returnsCount/(ordersCount*1.0))*100;
	    if (returnRate >= 50)
	    fraudPoints += 10;
	System.out.println("Customer ID" + "\t" +""+ key.toString() + "Customer Name" + "\t" + data.getCustomerName() + "Fraud Point is" + "\t" + fraudPoints);
	
	//Customer list contains (CustomerID , CustomerName , FraudCount) for every customer on every reducer invocation
	customers.add(key.toString() + "," + data.getCustomerName() + "," + fraudPoints);
    }
     /* customers contains the reducer output for every key processed . At the end this list will contain sorted output of 
        reducer on basis of keys for all the rows
  
		(customerID1,[FraudWritableobj1, FraudWritableobj2, FraudWritableobj3, FraudWritableobj4,..])=> add reducer result in custlst
		(customerID2,[FraudWritableobj1, FraudWritableobj2, FraudWritableobj3, FraudWritableobj4,..])=> add reducer result in custlst
		(customerID3,[FraudWritableobj1, FraudWritableobj2, FraudWritableobj3, FraudWritableobj4,..])=> add reducer result in custlst
  
    // we will sort the customers list<String> in cleanup method.
    ]
*/
    
    
   // The reducer when complete will have customer list that will contain sorted data on basis of customerIDs
    
    
    @Override
    protected void cleanup(Context c)throws IOException, java.lang.InterruptedException
    {
	/* sort customers based on fraudpoints */
	 Collections.sort(customers, new Comparator<String>()
			{
		//Sort on decending order of Fraud count
		//customer1 = {BHEE999914ED,Ana,12}
		//customer2 = {CCWO777171WT,Arthur,12}
		public int compare(String customer1, String customer2)
		{
		    int fp1 = Integer.parseInt(customer1.split(",")[2]);
		    int fp2 = Integer.parseInt(customer2.split(",")[2]);
		    
		    return -(fp1-fp2);     /*For desscending order*/
		}});
	for (String f: customers)
	{
	    String[] words = f.split(",");
	    c.write(new Text(words[0] + "," + words[1]), new IntWritable(Integer.parseInt(words[2])));
	}                   // custID     // custname                                    // fraud points in sorted order
    }
}
