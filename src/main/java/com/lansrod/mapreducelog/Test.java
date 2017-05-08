package com.lansrod.mapreducelog;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Test {

   public static void main( String args[] ) {
		Logger logger = LoggerFactory.getLogger(LogMapper.class);
		logger.info("Begining");

      // String to be scanned to find the pattern.
		String line0 = "This is the header Log file";
      String line1 = "64.242.88.10 – – [07/Mar/2014:22:12:28 -0800] “GET /twiki/bin/attach/TWiki/WebSearch HTTP/1.1” 401 12846";
     String line2 = "64.242.88.10 – – [07/Mar/2014:22:15:57 -0800] “GET /mailman/listinfo/hs_rcafaculty HTTP/1.1” 200 6345";

//      String pattern = "(.*)(\\d+)(.*)";
//        String pattern = "(\\w*)(\\s)(\\w*)(\\s)(\\w*)(\\s)(\\w*)(\\s)(\\w*)(\\s)(.*)";
//      String pattern = "(.*)(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])(\\s)";
      String pattern = "([0-9]*\\.+[0-9]*\\.+[0-9]*\\.+[0-9]*)(\\s)(\\– –)(\\s)(\\W)([0-9]{2}+/+\\w*/[0-9]*+:+[0-9]*:+[0-9]*:+[0-9]*)(\\s)(\\W+[0-9]*\\W)(\\s)"+
    		  		   "([A-Za-z\\s\\W+_]*\\W+[0-9]*\\W+[0-9]*\\W)([0-9\\s]*)";
      // Create a Pattern object
      Pattern r = Pattern.compile(pattern);

      // Now create matcher object.
      Matcher m = r.matcher(line1);
      if (m.find( )) {
         System.out.println("Found value: " + m.group(0) );
         System.out.println("Found value: " + m.group(1) );
//         System.out.println("Found value: " + m.group(2) );
         System.out.println("Found value: " + m.group(3) );
//         System.out.println("Found value: " + m.group(4) );
         System.out.println("Found value: " + m.group(5) );
         System.out.println("Found value: " + m.group(6) );
         System.out.println("Found value: " + m.group(7) );
         System.out.println("Found value: " + m.group(8) );
         System.out.println("Found value: " + m.group(9) );
         System.out.println("Found value: " + m.group(10) );
         System.out.println("Found value: " + m.group(11) );
//         System.out.println("Found value: " + m.group(12) );
//         System.out.println("Found value: " + m.group(13) );
         String timestamp = m.group(6);
         System.out.println("timestamp : "+timestamp);
         IntWritable hour = new IntWritable(Integer.parseInt(m.group(6).substring(12, 14)));
	      System.out.println("hour : "+hour);
	      String visitors = m.group(11).substring(1, m.group(11).length());
	      System.out.println("one : "+m.group(11).substring(visitors.indexOf(" ")+1, visitors.length()));
      }else {
         System.out.println("NO MATCH");
      }
   

	
//		Pattern logPattern = Pattern.compile("([^ ]*) ([^ ]*) ([^ ]*) \\[([^]]*)\\]" + " \"([^\"]*)\"" + " ([^ ]*) ([^ ]*).*");
//		String value = " 64.242.88.10 – – [07/Mar/2014:22:12:28 -0800] “GET /twiki/bin/attach/TWiki/WebSearch HTTP/1.1” 401 12846";
//		IntWritable hour = new IntWritable();
//		IntWritable one = new IntWritable(1);
//		System.out.println("hello man");
//		String line = value.toString();
//		Matcher matcher = logPattern.matcher(line);
//		System.out.println(matcher);
//		System.out.println(logPattern);
//		if (matcher.matches()) {
//			String timestamp = matcher.group(4);
//			System.out.println("(" + hour + "," + one + ")");
//			System.out.println(timestamp);
//		}
		logger.info("Ending");
	}
}
