package com.lansrod.mapreducelog;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	 private static Logger logger = LoggerFactory.getLogger(LogMapperPerso.class);
		private IntWritable hour = new IntWritable();
		private final static IntWritable one = new IntWritable(1);
//		private static Pattern logPattern = Pattern.compile("([^ ]*) ([^ ]*) ([^ ]*) \\[([^]]*)\\]" + " \"([^\"]*)\"" + " ([^ ]*) ([^ ]*).*");
		private static Pattern logPattern = Pattern.compile( "([0-9]*\\.+[0-9]*\\.+[0-9]*\\.+[0-9]*)(\\s)(\\– –)(\\s)(\\W)([0-9]{2}+/+\\w*/[0-9]*+:+[0-9]*:+[0-9]*:+[0-9]*)(\\s)(\\W+[0-9]*\\W)(\\s)"+
		  		   "([A-Za-z\\s\\W+_]*\\W+[0-9]*\\W+[0-9]*\\W)([0-9\\s]*)");
			public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
			     logger.info("Mapper started");
			      Text word = new Text();
			     String line = ((Text) value).toString();
			     Matcher matcher = logPattern.matcher(line);
			     if (matcher.matches()) {
			         String timestamp = matcher.group(6);
			         hour.set(Integer.parseInt(timestamp.substring(12, 14)));
			         String visitors = matcher.group(11).substring(1, matcher.group(11).length()); // Because m.group(11) contain space in the beginning 
			         visitors=visitors.substring(visitors.indexOf(" ")+1, visitors.length()); //Then get the second int
			         
			         one.set(Integer.parseInt(visitors));
			         context.write(hour, one);
			     }
			     
			     logger.info("Mapper Completed");
			    }
}
