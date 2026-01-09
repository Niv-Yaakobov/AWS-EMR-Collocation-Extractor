package com.assignment2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Step1_CalcN {

    // CHANGED: Input Key is now LongWritable, not Text
    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Text decadeText = new Text();
        private LongWritable countWritable = new LongWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Value format: "ngram TAB year TAB match_count TAB page_count"
            String valueStr = value.toString();
            String[] parts = valueStr.split("\t");

            // We need at least the year (index 1 usually, or index 0 if the word is in the key... 
            // BUT since key is LongWritable, the word must be in the value string at index 0)
            
            // Standard format when Key is LongWritable:
            // "word \t year \t match_count \t page_count"
            if (parts.length >= 3) {
                try {
                    int year = Integer.parseInt(parts[1]); 
                    long count = Long.parseLong(parts[2]); 
                    
                    int decade = year - (year % 10);
                    
                    decadeText.set(String.valueOf(decade));
                    countWritable.set(count);
                    context.write(decadeText, countWritable);
                    
                } catch (NumberFormatException e) {
                    // Ignore header or malformed lines
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 1: Calculate N per Decade");
        
        job.setJarByClass(Step1_CalcN.class);
        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(ReducerClass.class); 
        job.setReducerClass(ReducerClass.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class); 

        SequenceFileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}