import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.log4j.BasicConfigurator;

public class Task_E {

    // Mapper emits (ByWho, WhatPage)
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private final Text ownerId = new Text();
        private final Text pageId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length >= 5) {
                ownerId.set(tokens[1].trim());
                pageId.set(tokens[2].trim());
                context.write(ownerId, pageId);
            }
        }
    }

    // Reducer emits (OwnerID, TotalAccesses and DistinctPages count)
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private final Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> distinctPages = new HashSet<>();
            int totalAccesses = 0;

            for (Text val : values) {
                distinctPages.add(val.toString());
                totalAccesses++;
            }

            result.set("TotalAccesses: " + totalAccesses + ", DistinctPages: " + distinctPages.size());
            context.write(key, result);
        }
    }

    // Optional custom comparator (ascending)
    public static class AscendingKeyComparator extends Text.Comparator {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task_E");

        job.setJarByClass(Task_E.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(HashPartitioner.class);
        job.setSortComparatorClass(AscendingKeyComparator.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/project1/AccessLogs.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project1/Output05"));

        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        System.out.println("Total execution time for Task_E: " + (endTime - startTime) + " ms.");
        System.exit(success ? 0 : 1);
    }
}

// New Execution Time: 901ms (improved)
