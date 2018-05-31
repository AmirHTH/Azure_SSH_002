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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;


public class MyWordCountMultiFile extends Configured implements Tool {


  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    //private final static IntWritable one = new IntWritable(1);
    //private Text word = new Text();

    public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
      
      String line = value.toString().trim();
      String[] fields = line.split(";");
      String distrito = fields[17];
      //word.set(distrito);
      context.write(new Text(distrito), new IntWritable(1));

    }
  }
 

  public static class IntSumReducer   extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);

    }
  }

  public static void main(String[] args) throws Exception {

    System.exit(ToolRunner.run(new MyWordCountMultiFile(), args));
   
  }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgStrings = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgStrings.length != 3) {
            System.out.println("USAGE: org.apache.hadoop.examples.MyWordCountMultiFile <IN_1_FILE_CSV> <IN_2_FILE_CSV> <OUT_DESTINATION_FILE>");
            return 2;
        }

        Job job = new Job(conf, "MyWordCountMultiFile");
    
        job.setJarByClass(MyWordCountMultiFile.class);
       
        MultipleInputs.addInputPath(job, new Path(otherArgStrings[0]), TextInputFormat.class, TokenizerMapper.class);
        MultipleInputs.addInputPath(job, new Path(otherArgStrings[1]), TextInputFormat.class, TokenizerMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgStrings[2]));
        
        job.setReducerClass(IntSumReducer.class);

       //job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
    
        return(job.waitForCompletion(true) ? 0 : 1);
        

    }
}