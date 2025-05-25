import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.log4j.BasicConfigurator;

public class Task_E {

    // Mapper: emits (ByWho, WhatPage)
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private final Text userId = new Text();
        private final Text pageId = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 5) {
                userId.set(fields[1].trim());  // ByWho
                pageId.set(fields[2].trim());  // WhatPage
                context.write(userId, pageId);
            }
        }
    }

    // Reducer: computes total accesses and unique pages accessed
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private final Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> uniquePages = new HashSet<>();
            int total = 0;

            for (Text val : values) {
                uniquePages.add(val.toString());
                total++;
            }

            result.set("TotalAccesses: " + total + ", DistinctPages: " + uniquePages.size());
            context.write(key, result);
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

        Path inputPath = new Path("hdfs://localhost:9000/project1/AccessLogs.csv");
        Path outputPath = new Path("hdfs://localhost:9000/project1/Output05");

        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputPath, true);  // cleanup if exists

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();

        System.out.println("Total execution time for Task E: " + (endTime - startTime) + " ms");
        System.exit(success ? 0 : 1);
    }
}

// Execution Time: 1734
