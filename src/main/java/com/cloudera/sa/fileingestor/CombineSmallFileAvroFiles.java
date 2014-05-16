package com.cloudera.sa.fileingestor;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;


public class CombineSmallFileAvroFiles {
static Logger logger = Logger.getLogger(CombineSmallFileSeqFiles.class);
  
  
  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    
    if (args.length == 0) {
      System.out.println("CombineSmallFileAvroFiles");
      System.out.println();
      System.out.println("CombineSmallFileAvroFiles {inputPath} {outputPath} {numberOfReducers}");
      return;
    }
    
    String inputPath = args[0];
    String outputPath = args[1];
    int numberOfReducers = Integer.parseInt(args[2]);
    
    // Create job
    Job job = new Job();

    job.setJarByClass(CombineSmallFileSeqFiles.class);
    job.setJobName("CombineSmallFileSeqFiles");

    // Define input format and path
    job.setInputFormatClass(AvroKeyInputFormat.class);
    AvroKeyInputFormat.addInputPath(job, new Path(inputPath));

    // Define output format and path
    job.setOutputFormatClass(AvroKeyOutputFormat.class);
    AvroKeyOutputFormat.setOutputPath(job, new Path(outputPath));
    AvroKeyOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

    // Define the mapper and reducer
    job.setMapperClass(CustomMapper.class);
    job.setReducerClass(CustomReducer.class);

    // Define the key and value format
    job.setOutputKeyClass(AvroKey.class);
    job.setOutputValueClass(NullWritable.class);
    job.setMapOutputKeyClass(AvroKey.class);
    job.setMapOutputValueClass(NullWritable.class);

    job.setNumReduceTasks(numberOfReducers);

    // Exit
    job.waitForCompletion(true);

  }
  
  public static class CustomMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable> {
    
    @Override
    public void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
      context.write(key, value);
    }
  }
  
  public static class CustomReducer extends Reducer<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable> {
    @Override
    public void reduce(AvroKey<GenericRecord> key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
      for (NullWritable value: values) {
        context.write(key, value);
      }
    }
  }
}
