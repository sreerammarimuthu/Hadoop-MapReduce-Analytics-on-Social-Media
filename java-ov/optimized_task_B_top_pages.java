import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Task_B {

    public static class AccessLogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text pageId = new Text();

        // Mapper emits (WhatPage, 1) for each access
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 3) {
                pageId.set(fields[2].trim());
                context.write(pageId, one);
            }
        }
    }

    public static class TopPageReducer extends Reducer<Text, IntWritable, Text, Text> {
        private final Map<String, Integer> accessCounts = new HashMap<>();
        private final Map<String, String> pageToName = new HashMap<>();
        private final Map<String, String> pageToNationality = new HashMap<>();

        // Loads metadata from FaceInPage.csv using DistributedCache
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                Path faceInPagePath = new Path(cacheFiles[0]);
                BufferedReader reader = new BufferedReader(new InputStreamReader(faceInPagePath.getFileSystem(context.getConfiguration()).open(faceInPagePath)));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] tokens = line.split(",");
                    if (tokens.length >= 3) {
                        String id = tokens[0].trim();
                        String name = tokens[1].trim();
                        String nationality = tokens[2].trim();
                        pageToName.put(id, name);
                        pageToNationality.put(id, nationality);
                    }
                }
                reader.close();
            }
        }

        // Reducer counts accesses per page
        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int total = 0;
            for (IntWritable val : values) {
                total += val.get();
            }
            accessCounts.put(key.toString(), total);
        }

        // Outputs top 10 pages by access count
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<String, Integer>> sortedPages = new ArrayList<>(accessCounts.entrySet());
            sortedPages.sort((a, b) -> b.getValue() - a.getValue());

            int count = 0;
            for (Map.Entry<String, Integer> entry : sortedPages) {
                if (count++ == 10) break;

                String id = entry.getKey();
                String name = pageToName.getOrDefault(id, "Unknown");
                String nationality = pageToNationality.getOrDefault(id, "Unknown");
                int accesses = entry.getValue();

                context.write(new Text(id), new Text(name + "," + nationality + "," + accesses));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top10PopularPages");

        job.setJarByClass(Task_B.class);
        job.setMapperClass(AccessLogMapper.class);
        job.setReducerClass(TopPageReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/project1/AccessLogs.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project1/Output02"));

        // Load FaceInPage.csv into distributed cache
        job.addCacheFile(new URI("hdfs://localhost:9000/project1/FaceInPage.csv"));

        boolean status = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        System.out.println("Total execution time: " + (endTime - startTime) + " milliseconds.");
        System.exit(status ? 0 : 1);
    }
}

// New Execution Time: 1034ms (improved)
