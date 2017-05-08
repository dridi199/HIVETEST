package com.lansrod.mapreducelog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
public class LogDriver {
 
    private static Logger logger = LoggerFactory.getLogger(LogDriver.class);
 
    public static void main(String[] args) throws Exception {        
     logger.info("Code started");
     Configuration conf = new Configuration();
     Job job =  Job.getInstance(conf) ;
     job.setJarByClass(LogDriver.class);
     job.setJobName("Log Analyzer ");
 
     job.setMapperClass(LogMapper.class);
     job.setCombinerClass(LogReducer.class);
     job.setReducerClass(LogReducer.class);
 
     job.setOutputKeyClass(IntWritable.class);
     job.setOutputValueClass(IntWritable.class);
 
     FileInputFormat.addInputPath(job, new Path(args[0]));
     FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
     job.waitForCompletion(true);
     logger.info("Code ended");
    }
 
}
