package org.apache.hadoop.examples;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MyWordCountMultiFile {


  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    //private final static IntWritable one = new IntWritable(1);
    //private Text word = new Text();

    public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
      
    /*StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }*/
      
      String line = value.toString().trim();
      String[] fields = line.split(";");
      String distrito = fields[17];
      //word.set(distrito);
      context.write(new Text(distrito), new IntWritable(1));

    }
  }
  
 

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
      //context.write(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    //conf.set("key.value.separator.in.input.line", ";");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    
    Job job = new Job(conf, "MyWordCountMultiFile");

    job.setJarByClass(MyWordCountMultiFile.class);
    //job.setMapperClass(TokenizerMapper.class);
    //job.setMapperClass(Map.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setNumReduceTasks(2);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    //MultipleInputs.addInputPath(job, new Path(args[0]), FileInputFormat.class, TokenizerMapper.class);
    //MultipleInputs.addInputPath(conf, new Path(args[1]), FileInputFormat.class);
    
    if (args.length>=1 ) {
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
    } 

    if (args.length>=2 ) {
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), FileInputFormat.class, TokenizerMapper.class);
        job.setMapperClass(TokenizerMapper.class);
    }

    if (args.length>=3 ) {
        MultipleInputs.addInputPath(job, new Path(otherArgs[2]), FileInputFormat.class, TokenizerMapper.class);
        job.setMapperClass(TokenizerMapper.class);
    }

    if (args.length>=4 ) {
        MultipleInputs.addInputPath(job, new Path(otherArgs[3]), FileInputFormat.class, TokenizerMapper.class);
        job.setMapperClass(TokenizerMapper.class);
    }
    
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}