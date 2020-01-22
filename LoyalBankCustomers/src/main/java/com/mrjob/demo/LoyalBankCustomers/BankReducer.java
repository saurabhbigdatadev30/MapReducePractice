package com.mrjob.demo.LoyalBankCustomers;

import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*Reducer will join from AccountMapper & PersonMapper on customerID. For every key (customerID)
 * There will be 1 person object & 4 AccountObject (for every quater)
 * 
 * OMOI808692OZ 
  [{P,Allison,Abbott} {A,1245015582,3667,822,no} {A,1245015582,5806,1035,no} {A,1245015582,1601,635,no} {A,1245015582,4189,802,no} ]
*/
public class BankReducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context c)throws IOException,java.lang.InterruptedException
    {
    	/* reduce[n=1]
    	 * for Customer Id = OMOI808692OZ 
    	 * value =[{P,Allison,Abbott}{A,1245015582,3667,822,no} {A,1245015582,5806,1035,no} {A,1245015582,1601,635,no} 
    	 *         {A,1245015582,4189,802,no}]                                 
    	 */
	String fName = "";   
	String lName = "";
	String accountNumber = "";
	int totalDeposit = 0;
	
	Iterator<Text> valuesIter = values.iterator();
	while (valuesIter.hasNext())
	{
		
	    String val = valuesIter.next().toString();   
	    String [] words = val.split(",");           
	    if (words[0].equals("P")) 
	    {
		/* customer personal details from PersonMapper */
		fName = words[1];         
		lName = words[2];          
	    }
	    else if (words[0].equals("A"))  //{A,1245015582,3667,822,no}
	    {                               //{A,1245015582,5806,1035,no}
		/* customer account details from AccountMapper */
		accountNumber = words[1];                                    //accountNumber = 1245015582
		/* check for red flag */
		if (words[4].equalsIgnoreCase("yes"))
		{
		    /* 
		     * ignore any other processing since customer cannot be 
		     * loyal if red flag is true for any quarter
		     * No need to output anything for this customer
		     */
		    break;
		}
		/* check for quarter deposit and withdraw */
		int withdrawAmount = Integer.parseInt(words[3]);
		int depositAmount = Integer.parseInt(words[2]);
		if (withdrawAmount >= (depositAmount/2))
		{
		    /* No need to process further */
		    break;
		}
		totalDeposit += depositAmount;                    //totalDeposit  = 3667
	    }
	}
	/* End of iteration over the above 4 account objects i.e 
	 * {A,1245015582,3667,822,no} {A,1245015582,5806,1035,no} {A,1245015582,1601,635,no} {A,1245015582,4189,802,no}
	 * , we get the totalDeposit. 
	 *  if totalDeposit > 10,000$ then loyal customer
	 */
	if (totalDeposit >= 10000)
	{
	    /* id, fName, lName, accountNumber */
	    c.write(key, new Text(fName + "," + lName + "," + accountNumber));
	}
    }
}
/*
 * [cloudera@quickstart ~]$ hdfs dfs -cat /output_BankLoyaCustomer/part-r-00000
KGNZ630334NA	Arlene,Adkins,1366578552
OBIC304336IX	Bret,Allen,1400967582
OMOI808692OZ	Allison,Abbott,1245015582
POOH283211OT	Alex,Adams,1113028282
 */