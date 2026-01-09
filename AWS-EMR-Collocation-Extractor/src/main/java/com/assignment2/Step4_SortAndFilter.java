package com.assignment2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Step4_SortAndFilter {

    // 1. Custom Key for Secondary Sort
    public static class DecadeLLRPair implements WritableComparable<DecadeLLRPair> {
        private String decade;
        private double llr;

        public DecadeLLRPair() {
            this.decade = "";
            this.llr = 0.0;
        }

        public DecadeLLRPair(String decade, double llr) {
            this.decade = decade;
            this.llr = llr;
        }

        public void set(String decade, double llr) {
            this.decade = decade;
            this.llr = llr;
        }

        public String getDecade() { return decade; }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(decade);
            out.writeDouble(llr);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            decade = in.readUTF();
            llr = in.readDouble();
        }

        @Override
        public int compareTo(DecadeLLRPair other) {
            // Sort by Decade ASC
            int decadeCmp = this.decade.compareTo(other.decade);
            if (decadeCmp != 0) {
                return decadeCmp;
            }
            // If decades are equal, sort by LLR DESC
            return Double.compare(this.llr, other.llr);
            
        }

        @Override
        public String toString() {
            return decade + "\t" + llr;
        }
    }

    // 2. Mapper - FIX APPLIED HERE
    // Changed input key from Text -> LongWritable
    public static class MapperClass extends Mapper<LongWritable, Text, DecadeLLRPair, Text> {
        private DecadeLLRPair sortKey = new DecadeLLRPair();
        private Text outValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Input Line Format from Step 3: 
            // Decade \t w1 \t w2 \t LLR
            
            String line = value.toString();
            String[] parts = line.split("\t");
            
            if (parts.length >= 4) {
                String decade = parts[0];
                String w1 = parts[1];
                String w2 = parts[2];
                double llr = Double.parseDouble(parts[3]);

                // We sort by Decade + LLR
                sortKey.set(decade, llr);
                
                // We output "w1 w2" (and LLR for display)
                outValue.set(w1 + " " + w2 + "\t" + llr); 
                
                context.write(sortKey, outValue);
            }
        }
    }

    // 3. Partitioner: Ensures same Decade goes to same Reducer
    public static class DecadePartitioner extends Partitioner<DecadeLLRPair, Text> {
        @Override
        public int getPartition(DecadeLLRPair key, Text value, int numPartitions) {
            return Math.abs(key.getDecade().hashCode()) % numPartitions;
        }
    }

    // 4. Grouping Comparator: Groups values by Decade only
    public static class GroupingComparator extends WritableComparator {
        protected GroupingComparator() {
            super(DecadeLLRPair.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            DecadeLLRPair k1 = (DecadeLLRPair) w1;
            DecadeLLRPair k2 = (DecadeLLRPair) w2;
            return k1.getDecade().compareTo(k2.getDecade());
        }
    }

    // 5. Reducer
    public static class ReducerClass extends Reducer<DecadeLLRPair, Text, Text, Text> {
        private Text outKey = new Text();

        @Override
        public void reduce(DecadeLLRPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Values arrive sorted by LLR Descending because of the Secondary Sort
            int count = 0;
            outKey.set(key.getDecade());

            for (Text val : values) {
                if (count < 100) {
                    context.write(outKey, val); // Output: Decade \t w1 w2 LLR
                    count++;
                } else {
                    break;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 4: Sort and Top-100");
        job.setJarByClass(Step4_SortAndFilter.class);

        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(DecadePartitioner.class);
        job.setGroupingComparatorClass(GroupingComparator.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(DecadeLLRPair.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}