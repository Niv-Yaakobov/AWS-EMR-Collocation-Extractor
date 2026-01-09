package com.assignment2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Step2_JoinC1 {

    public static class Mapper1Gram extends Mapper<LongWritable, Text, Text, Text> {
        private Text joinKey = new Text();
        private Text outValue = new Text();
        private StopWordsUtility stopWordsUtility = new StopWordsUtility();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length >= 3) {
                String word = parts[0];
                if (stopWordsUtility.isStopWord(word)) return;

                int year = Integer.parseInt(parts[1]);
                int decade = year - (year % 10);
                String count = parts[2];

                joinKey.set(word + "\t" + decade);
                outValue.set("1\t" + count);
                context.write(joinKey, outValue);
            }
        }
    }

    public static class Mapper2Gram extends Mapper<LongWritable, Text, Text, Text> {
        private Text joinKey = new Text();
        private Text outValue = new Text();
        private StopWordsUtility stopWordsUtility = new StopWordsUtility();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length >= 3) {
                String bigram = parts[0];
                int year = Integer.parseInt(parts[1]);
                String count = parts[2];
                int decade = year - (year % 10);

                String[] words = bigram.split(" ");
                if (words.length != 2) return;
                if (stopWordsUtility.isStopWord(words[0]) || stopWordsUtility.isStopWord(words[1])) return;

                joinKey.set(words[0] + "\t" + decade);
                outValue.set("2\t" + words[1] + "\t" + count);
                context.write(joinKey, outValue);
            }
        }
    }

    // FIX APPLIED HERE: Aggregating counts per Decade
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sumC1 = 0;
            Map<String, Long> bigramCounts = new HashMap<>();

            // 1. Sum up all counts for this decade
            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                if (parts[0].equals("1")) {
                    // It's a 1-gram count: Add to Decade Total C1
                    sumC1 += Long.parseLong(parts[1]);
                } else if (parts[0].equals("2")) {
                    // It's a 2-gram: Add to Decade Total for this specific bigram
                    String w2 = parts[1];
                    long count = Long.parseLong(parts[2]);
                    bigramCounts.put(w2, bigramCounts.getOrDefault(w2, 0L) + count);
                }
            }

            // 2. Emit aggregated results
            if (sumC1 > 0 && !bigramCounts.isEmpty()) {
                String[] keyParts = key.toString().split("\t");
                String w1 = keyParts[0];
                String decade = keyParts[1];

                outKey.set(decade);
                for (Map.Entry<String, Long> entry : bigramCounts.entrySet()) {
                    String w2 = entry.getKey();
                    long sumC12 = entry.getValue();
                    // Output: w1 \t w2 \t TotalC12 \t TotalC1
                    outValue.set(w1 + "\t" + w2 + "\t" + sumC12 + "\t" + sumC1);
                    context.write(outKey, outValue);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 2: Join Bigrams with C1");
        job.setJarByClass(Step2_JoinC1.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), SequenceFileInputFormat.class, Mapper1Gram.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), SequenceFileInputFormat.class, Mapper2Gram.class);
        
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}