package com.assignment2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Step3_JoinC2AndCalcLLR {

    public static class MapperStep2 extends Mapper<LongWritable, Text, Text, Text> {
        private Text joinKey = new Text();
        private Text outValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length >= 5) {
                String decade = parts[0];
                String w1 = parts[1];
                String w2 = parts[2];
                String c12 = parts[3];
                String c1 = parts[4];

                joinKey.set(w2 + "\t" + decade);
                outValue.set("2\t" + w1 + "\t" + c12 + "\t" + c1);
                context.write(joinKey, outValue);
            }
        }
    }

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

    // FIX APPLIED HERE: Aggregating C2 per Decade
    public static class ReducerClass extends Reducer<Text, Text, Text, DoubleWritable> {
        private Text outKey = new Text();
        private DoubleWritable outValue = new DoubleWritable();
        private Map<String, Long> decadeNMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String step1OutputPath = context.getConfiguration().get("step1.output.path");
            Path path = new Path(step1OutputPath + "/part-r-00000");
            FileSystem fs = path.getFileSystem(context.getConfiguration());
            
            if (fs.exists(path)) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split("\t");
                        if (parts.length == 2) {
                            decadeNMap.put(parts[0], Long.parseLong(parts[1]));
                        }
                    }
                }
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sumC2 = 0;
            ArrayList<String> step2Records = new ArrayList<>();

            // 1. Sum up C2 for the entire decade
            for (Text val : values) {
                String s = val.toString();
                String[] parts = s.split("\t");
                
                if (parts[0].equals("1")) {
                    sumC2 += Long.parseLong(parts[1]);
                } else if (parts[0].equals("2")) {
                    // Collect Step 2 records (w1, c12, c1) - these are already aggregated from Step 2
                    step2Records.add(parts[1] + "\t" + parts[2] + "\t" + parts[3]);
                }
            }

            // 2. Calculate LLR
            if (sumC2 > 0 && !step2Records.isEmpty()) {
                String[] keyParts = key.toString().split("\t");
                String w2 = keyParts[0];
                String decade = keyParts[1];
                
                if (!decadeNMap.containsKey(decade)) return;
                long N = decadeNMap.get(decade);

                for (String record : step2Records) {
                    String[] recParts = record.split("\t");
                    String w1 = recParts[0];
                    long c12 = Long.parseLong(recParts[1]);
                    long c1 = Long.parseLong(recParts[2]);

                    double llr = calculateLogLikelihoodRatio(c12, c1, sumC2, N);

                    outKey.set(decade + "\t" + w1 + "\t" + w2);
                    outValue.set(llr);
                    context.write(outKey, outValue);
                }
            }
        }

        private double calculateLogLikelihoodRatio(long c12, long c1, long c2, long N) {
            double p = (double) c2 / N;
            double p1 = (double) c12 / c1;
            double p2 = (double) (c2 - c12) / (N - c1);

            double term1 = logL(c12, c1, p);
            double term2 = logL(c2 - c12, N - c1, p);
            double term3 = logL(c12, c1, p1);
            double term4 = logL(c2 - c12, N - c1, p2);

            return term1 + term2 - term3 - term4;
        }

        private double logL(long k, long n, double x) {
            if (x == 0 || x == 1) return 0;
            return k * Math.log(x) + (n - k) * Math.log(1 - x);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("step1.output.path", args[2]);

        Job job = Job.getInstance(conf, "Step 3: Join C2 and Calc LLR");
        job.setJarByClass(Step3_JoinC2AndCalcLLR.class);
        job.setReducerClass(ReducerClass.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperStep2.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), SequenceFileInputFormat.class, Mapper1Gram.class);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}