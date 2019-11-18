package com.mrjob.demo;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
public class FraudMapper extends Mapper<LongWritable, Text, Text, FraudWritable>
{

    private Text custId = new Text();    //  GGYZ333519YS 
    private FraudWritable data = new FraudWritable();    // will create object of writable class and initialize it

    @Override
    protected void map(LongWritable key, Text value, Context c)	throws IOException, java.lang.InterruptedException
    {

	String line = value.toString();    //GGYZ333519YS,Allison,01-01-2017,03-01-2017,Fedx,06-01-2017,no,null,null
	/* Split csv string */
	String[] words = line.split(",");  // [{GGYZ333519YS} {Allison} {01-01-2017} {03-01-2017} {Fedx} {06-01-2017} {no} {null} {null}]

	custId.set(words[0]);             // custdid = GGYZ333519YS
	// set the input tokens from fle  to the custom writable
	// ** every time new dataObj is created automatically ????
	data.set(words[1], words[5], words[6], words[7]);
	System.out.println("Mapper output is "+ "\t" + "" +"["+words[0]+ "\t" + "]" + ""+ "["+ data.toString()+ "]");
	c.write(custId, data);
}
}

/*
 * Mapper output is 	[BPLA457837LB	][FraudWritable [customerName=Alex, receiveDate=09-01-2017, returned=false, returnDate=null]]
Mapper output is 	[BPLA457837LB	][FraudWritable [customerName=Alex, receiveDate=13-01-2017, returned=false, returnDate=null]]
Mapper output is 	[BPLA457837LB	][FraudWritable [customerName=Alex, receiveDate=06-01-2017, returned=false, returnDate=null]]
Mapper output is 	[GGYZ333519YS	][FraudWritable [customerName=Allison, receiveDate=06-01-2017, returned=false, returnDate=null]]
Mapper output is 	[GGYZ333519YS	][FraudWritable [customerName=Allison, receiveDate=13-01-2017, returned=true, returnDate=15-01-2017]]
Mapper output is 	[GGYZ333519YS	][FraudWritable [customerName=Allison, receiveDate=07-01-2017, returned=false, returnDate=null]]
Mapper output is 	[GGYZ333519YS	][FraudWritable [customerName=Allison, receiveDate=26-01-2017, returned=true, returnDate=16-02-2017]]
Mapper output is 	[CCWO777171WT	][FraudWritable [customerName=Arthur, receiveDate=16-01-2017, returned=false, returnDate=null]]
Mapper output is 	[CCWO777171WT	][FraudWritable [customerName=Arthur, receiveDate=31-01-2017, returned=true, returnDate=01-02-2017]]
Mapper output is 	[CCWO777171WT	][FraudWritable [customerName=Arthur, receiveDate=15-01-2017, returned=true, returnDate=16-01-2017]]
Mapper output is 	[CCWO777171WT	][FraudWritable [customerName=Arthur, receiveDate=10-01-2017, returned=false, returnDate=null]]
Mapper output is 	[CCWO777171WT	][FraudWritable [customerName=Arthur, receiveDate=13-01-2017, returned=true, returnDate=24-01-2017]]
Mapper output is 	[BHEE999914ED	][FraudWritable [customerName=Ana, receiveDate=13-01-2017, returned=true, returnDate=14-01-2017]]
Mapper output is 	[BHEE999914ED	][FraudWritable [customerName=Ana, receiveDate=08-01-2017, returned=false, returnDate=null]]
Mapper output is 	[BHEE999914ED	][FraudWritable [customerName=Ana, receiveDate=10-01-2017, returned=true, returnDate=17-01-2017]]
Mapper output is 	[BHEE999914ED	][FraudWritable [customerName=Ana, receiveDate=26-01-2017, returned=true, returnDate=12-02-2017]]
Mapper output is 	[BHEE999914ED	][FraudWritable [customerName=Ana, receiveDate=06-01-2017, returned=true, returnDate=10-01-2017]]
Mapper output is 	[GGYZ333519YS	][FraudWritable [customerName=Allison, receiveDate=09-01-2017, returned=true, returnDate=18-01-2017]]
Mapper output is 	[GGYZ333519YS	][FraudWritable [customerName=Allison, receiveDate=07-01-2017, returned=false, returnDate=null]]
Mapper output is 	[GGYZ333519YS	][FraudWritable [customerName=Allison, receiveDate=08-01-2017, returned=true, returnDate=06-02-2017]]
Mapper output is 	[GGYZ333519YS	][FraudWritable [customerName=Allison, receiveDate=27-01-2017, returned=false, returnDate=null]]
Mapper output is 	[BPLA457837LB	][FraudWritable [customerName=Alex, receiveDate=08-01-2017, returned=true, returnDate=18-01-2017]]
Mapper output is 	[BPLA457837LB	][FraudWritable [customerName=Alex, receiveDate=08-01-2017, returned=false, returnDate=null]]
Mapper output is 	[BPLA457837LB	][FraudWritable [customerName=Alex, receiveDate=15-01-2017, returned=false, returnDate=null]]
Mapper output is 	[BPLA457837LB	][FraudWritable [customerName=Alex, receiveDate=13-01-2017, returned=true, returnDate=22-01-2017]]
Mapper output is 	[BPLA457837LB	][FraudWritable [customerName=Alex, receiveDate=06-01-2017, returned=true, returnDate=09-01-2017]]
Mapper output is 	[RTUG908381UW	][FraudWritable [customerName=Cristobal, receiveDate=16-01-2017, returned=true, returnDate=28-01-2017]]
Mapper output is 	[RTUG908381UW	][FraudWritable [customerName=Cristobal, receiveDate=16-01-2017, returned=false, returnDate=null]]
Mapper output is 	[RTUG908381UW	][FraudWritable [customerName=Cristobal, receiveDate=10-01-2017, returned=false, returnDate=null]]
Mapper output is 	[RTUG908381UW	][FraudWritable [customerName=Cristobal, receiveDate=10-01-2017, returned=false, returnDate=null]]
Mapper output is 	[RTUG908381UW	][FraudWritable [customerName=Cristobal, receiveDate=06-01-2017, returned=true, returnDate=19-01-2017]]
Mapper output is 	[RTUG908381UW	][FraudWritable [customerName=Cristobal, receiveDate=09-01-2017, returned=false, returnDate=null]]
Mapper output is 	[RTUG908381UW	][FraudWritable [customerName=Cristobal, receiveDate=07-01-2017, returned=true, returnDate=18-01-2017]]
Mapper output is 	[RTUG908381UW	][FraudWritable [customerName=Cristobal, receiveDate=10-01-2017, returned=true, returnDate=13-01-2017]]
Mapper output is 	[WLUR292384UB	][FraudWritable [customerName=Chris, receiveDate=09-01-2017, returned=false, returnDate=null]]
Mapper output is 	[WLUR292384UB	][FraudWritable [customerName=Chris, receiveDate=14-01-2017, returned=false, returnDate=null]]
Mapper output is 	[WLUR292384UB	][FraudWritable [customerName=Chris, receiveDate=07-01-2017, returned=false, returnDate=null]]
Mapper output is 	[WLUR292384UB	][FraudWritable [customerName=Chris, receiveDate=14-01-2017, returned=false, returnDate=null]]
Mapper output is 	[WLUR292384UB	][FraudWritable [customerName=Chris, receiveDate=14-01-2017, returned=false, returnDate=null]]
Mapper output is 	[WLUR292384UB	][FraudWritable [customerName=Chris, receiveDate=09-01-2017, returned=false, returnDate=null]]
Mapper output is 	[WLUR292384UB	][FraudWritable [customerName=Chris, receiveDate=07-01-2017, returned=false, returnDate=null]]
Mapper output is 	[WLUR292384UB	][FraudWritable [customerName=Chris, receiveDate=16-01-2017, returned=true, returnDate=28-01-2017]]
Mapper output is 	[CVYO477759YX	][FraudWritable [customerName=Dean, receiveDate=07-01-2017, returned=false, returnDate=null]]
Mapper output is 	[CVYO477759YX	][FraudWritable [customerName=Dean, receiveDate=16-01-2017, returned=false, returnDate=null]]
Mapper output is 	[CVYO477759YX	][FraudWritable [customerName=Dean, receiveDate=08-01-2017, returned=true, returnDate=10-01-2017]]
Mapper output is 	[CVYO477759YX	][FraudWritable [customerName=Dean, receiveDate=13-01-2017, returned=true, returnDate=22-01-2017]]
Mapper output is 	[CVYO477759YX	][FraudWritable [customerName=Dean, receiveDate=07-01-2017, returned=true, returnDate=08-01-2017]]
Mapper output is 	[CVYO477759YX	][FraudWritable [customerName=Dean, receiveDate=09-01-2017, returned=true, returnDate=12-01-2017]]
Mapper output is 	[CVYO477759YX	][FraudWritable [customerName=Dean, receiveDate=06-01-2017, returned=true, returnDate=06-01-2017]]
Mapper output is 	[CVYO477759YX	][FraudWritable [customerName=Dean, receiveDate=15-01-2017, returned=false, returnDate=null]]
Mapper output is 	[YQTD373114TQ	][FraudWritable [customerName=Danny, receiveDate=08-01-2017, returned=true, returnDate=14-01-2017]]
Mapper output is 	[YQTD373114TQ	][FraudWritable [customerName=Danny, receiveDate=14-01-2017, returned=true, returnDate=20-01-2017]]
Mapper output is 	[YQTD373114TQ	][FraudWritable [customerName=Danny, receiveDate=16-01-2017, returned=false, returnDate=null]]
Mapper output is 	[YQTD373114TQ	][FraudWritable [customerName=Danny, receiveDate=09-01-2017, returned=false, returnDate=null]]
Mapper output is 	[YQTD373114TQ	][FraudWritable [customerName=Danny, receiveDate=09-01-2017, returned=false, returnDate=null]]
Mapper output is 	[YQTD373114TQ	][FraudWritable [customerName=Danny, receiveDate=06-01-2017, returned=true, returnDate=12-01-2017]]
Mapper output is 	[CCWO777171WT	][FraudWritable [customerName=Arthur, receiveDate=13-01-2017, returned=true, returnDate=09-02-2017]]
Mapper output is 	[CCWO777171WT	][FraudWritable [customerName=Arthur, receiveDate=26-01-2017, returned=true, returnDate=02-02-2017]]
Mapper output is 	[CCWO777171WT	][FraudWritable [customerName=Arthur, receiveDate=16-01-2017, returned=true, returnDate=21-01-2017]]
Mapper output is 	[YQTD373114TQ	][FraudWritable [customerName=Danny, receiveDate=14-01-2017, returned=false, returnDate=null]]
Mapper output is 	[YQTD373114TQ	][FraudWritable [customerName=Danny, receiveDate=10-01-2017, returned=false, returnDate=null]]
Mapper output is 	[SMNU471252NK	][FraudWritable [customerName=Dennis, receiveDate=14-01-2017, returned=true, returnDate=20-01-2017]]
Mapper output is 	[SMNU471252NK	][FraudWritable [customerName=Dennis, receiveDate=14-01-2017, returned=false, returnDate=null]]
Mapper output is 	[SMNU471252NK	][FraudWritable [customerName=Dennis, receiveDate=16-01-2017, returned=true, returnDate=27-01-2017]]
Mapper output is 	[SMNU471252NK	][FraudWritable [customerName=Dennis, receiveDate=16-01-2017, returned=false, returnDate=null]]
Mapper output is 	[BHEE999914ED	][FraudWritable [customerName=Ana, receiveDate=14-01-2017, returned=false, returnDate=null]]
Mapper output is 	[BHEE999914ED	][FraudWritable [customerName=Ana, receiveDate=10-01-2017, returned=true, returnDate=17-01-2017]]
Mapper output is 	[BHEE999914ED	][FraudWritable [customerName=Ana, receiveDate=13-01-2017, returned=true, returnDate=05-02-2017]]
Mapper output is 	[SMNU471252NK	][FraudWritable [customerName=Dennis, receiveDate=13-01-2017, returned=false, returnDate=null]]
Mapper output is 	[SMNU471252NK	][FraudWritable [customerName=Dennis, receiveDate=08-01-2017, returned=true, returnDate=14-01-2017]]
Mapper output is 	[SMNU471252NK	][FraudWritable [customerName=Dennis, receiveDate=06-01-2017, returned=false, returnDate=null]]
Mapper output is 	[SMNU471252NK	][FraudWritable [customerName=Dennis, receiveDate=16-01-2017, returned=false, returnDate=null]]
Mapper output is 	[RHMX673369MP	][FraudWritable [customerName=Isidore, receiveDate=08-01-2017, returned=true, returnDate=20-01-2017]]
Mapper output is 	[RHMX673369MP	][FraudWritable [customerName=Isidore, receiveDate=09-01-2017, returned=true, returnDate=17-01-2017]]
Mapper output is 	[RHMX673369MP	][FraudWritable [customerName=Isidore, receiveDate=07-01-2017, returned=false, returnDate=null]]
Mapper output is 	[RHMX673369MP	][FraudWritable [customerName=Isidore, receiveDate=07-01-2017, returned=false, returnDate=null]]
Mapper output is 	[RHMX673369MP	][FraudWritable [customerName=Isidore, receiveDate=14-01-2017, returned=false, returnDate=null]]
Mapper output is 	[RHMX673369MP	][FraudWritable [customerName=Isidore, receiveDate=10-01-2017, returned=true, returnDate=16-01-2017]]
Mapper output is 	[RHMX673369MP	][FraudWritable [customerName=Isidore, receiveDate=10-01-2017, returned=true, returnDate=24-01-2017]]
Mapper output is 	[RHMX673369MP	][FraudWritable [customerName=Isidore, receiveDate=06-01-2017, returned=false, returnDate=null]]
Mapper output is 	[RZMY426113MN	][FraudWritable [customerName=Isabel, receiveDate=16-01-2017, returned=false, returnDate=null]]
Mapper output is 	[RZMY426113MN	][FraudWritable [customerName=Isabel, receiveDate=10-01-2017, returned=false, returnDate=null]]
Mapper output is 	[RZMY426113MN	][FraudWritable [customerName=Isabel, receiveDate=15-01-2017, returned=false, returnDate=null]]
Mapper output is 	[RZMY426113MN	][FraudWritable [customerName=Isabel, receiveDate=10-01-2017, returned=false, returnDate=null]]
Mapper output is 	[RZMY426113MN	][FraudWritable [customerName=Isabel, receiveDate=10-01-2017, returned=true, returnDate=21-01-2017]]
Mapper output is 	[RZMY426113MN	][FraudWritable [customerName=Isabel, receiveDate=06-01-2017, returned=true, returnDate=16-01-2017]]
*/
