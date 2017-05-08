package com.lansrod.mapreducelog;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

   private static Logger logger = LoggerFactory.getLogger(LogReducer.class);

   public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

    logger.info("Reducer started");
    int sum = 0;
    for (IntWritable value : values) {
        sum = sum + value.get();
    }
    context.write(key, new IntWritable(sum));
    logger.info("Reducer completed");

   }
}
