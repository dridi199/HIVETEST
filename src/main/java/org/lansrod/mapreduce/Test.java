package org.lansrod.mapreduce;

import java.io.BufferedWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

public class Test {
	public static void main(String [] args){
//		 Text word = new Text(" Removes all of the mappings from this map optional operation.");
//		    StringTokenizer itr = new StringTokenizer(word.toString());
//		      while (itr.hasMoreTokens()) {
//		        word.set(itr.nextToken());
//		        System.out.println("("+word+",1)");
//		      }
		
		
	     Map<Integer, String> hm = new HashMap<Integer,String>();

	      hm.put(10, "1");

	      hm.put(10, "2");

	      hm.put(30, "3");

	      hm.put(40, "4");

	      hm.put(50, "5");

	      //Ceci va écraser la valeur 5

	      hm.put(50, "6");
	      System.out.println(hm.get(10));
		
	}

}
