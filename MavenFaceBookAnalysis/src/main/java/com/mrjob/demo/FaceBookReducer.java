package com.mrjob.demo;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class FaceBookReducer extends Reducer<Text, Text, Text, Text>{
	
	/* output from shuffle & sort == input to reduce phase =>  < Text , Text >
	 Where  
	 		Text - key =>   Category of Advertisement (output of mapper) => Ecommerce
	 		Text - value => [ { Mumbai,39,13 }  Mumbai,281,5 } {Delhi,341,9} {Delhi,398,10}................]
	 
	
     Key -       Ecommerce 
	 values - 
				 array - [ 			 
				 Mumbai,39,13  
				 Mumbai,281,5,			 
				 Delhi,341,9} 
				 Delhi,398,10}
	 			 ................]
	These 

	*/
	 @Override
	    protected void reduce(Text key, Iterable<Text> values, Context c)throws IOException, java.lang.InterruptedException
	    {

	    // key: city value:total_success_rate, count
		HashMap<String, String> cityData = new HashMap<String, String>();   //  [{Mumbai:65.07,3 } {Delhi:69.65,7} {Bangalore:9.78,3} ...... ]
		System.out.println("reduce input Key data" + ":-" +"" +""  +key );
		Iterator<Text> itr = values.iterator();
		/* all data for each category */
		while (itr.hasNext())  //first iteration ..(  Mumbai,39,13 )  is complete
	   //  and after iteration 1 , map contains = ([mumbai] 33.33,1) we are now iterating  row 2 => Mumbai,281,5,	
		{
		    String f = itr.next().toString();       // f =  Mumbai,281,5
		    System.out.println("reduce input value data" + ":-" +""  +f );

		    String[] words = f.split(",");                 //   words = [ {Mumbai} {281} {5}]
		    
		    String location = words[0].trim();            // location = Mumbai
		    
		    int clickCount = Integer.parseInt(words[1]);   // clickCount= 281
		    
		    int conversionCount = Integer.parseInt(words[2]);   //  conversionCount = 5 
		    
		    Double succRate = new Double(conversionCount/(clickCount*1.0)*100);   // succRate = 1.77

		    if (cityData.containsKey(location))    
		    {
		    String s1 = cityData.get(location);  // in first iteration s1 =  33.3,1              in next iteration (35.07 ,2)
			String[] hValues = s1.split(",");         // hValues = [ {33.3} {1} ]            
			Double totalSuccRate = Double.parseDouble(hValues[0]) + succRate;    // totalSuccRate  = (33.3 + 1.77_ = 35.07)
			int totalCount = Integer.parseInt(hValues[1]) + 1;             // totalCount =  2

			cityData.put(location, totalSuccRate + "," + totalCount);

		    }else
		    {
			cityData.put(location, succRate + ",1");
		    }
		}
		System.out.println("cityData is ----");
		System.out.println(cityData.toString());
		for (Map.Entry<String, String> e : cityData.entrySet())        //Map.Entry e ==> Key-  Mumbai;  value= 65.07,3
		{
		    String[] V1 = e.getValue().split(",");        // V1  [{65.07} {3}]
		    Double avgSccRate = Double.parseDouble(V1[0])/Integer.parseInt(V1[1]);
		    c.write(key, new Text(e.getKey() + "," + avgSccRate));
		}
	    }
	

}
