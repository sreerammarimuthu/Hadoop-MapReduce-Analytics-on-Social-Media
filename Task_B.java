import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.BasicConfigurator;

import java.io.*;
import java.net.URI;
import java.util.*;

/**
 * Task_B is a Hadoop MapReduce program that identifies the top 10 most popular FaceInPages
 * based on the number of accesses from the AccessLogs dataset.
 * It reads the AccessLogs dataset, counts accesses to each FaceInPage, and outputs the top 10 pages
 * along with their names, nationalities, and access counts.
 *
 * Usage:
 * - Input: AccessLogs dataset (Assumed to have columns: AccessId, ByWho, WhatPage, TypeOfAccess, AccessTime)
 * - Distributed Cache: FaceInPage dataset (Assumed to have columns: ID, Name, Nationality, CountryCode, Hobby)
 * - Output: The top 10 most popular FaceInPages with names, nationalities, and access counts.
 *
 * Dependencies:
 * - Hadoop MapReduce libraries.
 *
 * Execution:
 * - The program configuration and job setup are performed in the main method.
 * - The Mapper class (MapClass) extracts FaceInPage IDs from the AccessLogs dataset.
 * - The Reducer class (ReduceClass) calculates the total access count for each FaceInPage
 *   and outputs the top 10 most popular pages.
 * - Input and output paths, as well as the distributed cache file, should be specified in the job configuration.
 *
 * Note:
 * - The program assumes that the FaceInPage dataset is available in the distributed cache.
 */


import java.io.*;
import java.util.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import java.net.URI;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.BasicConfigurator;

public class Task_B {
    public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text outputKey = new Text();
        private final static IntWritable one = new IntWritable(1);
        /**
         * Mapper class for extracting FaceInPage IDs from the AccessLogs dataset.
         * Emits (FaceInPage ID, 1) for each access.
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the input from the AccessLogs dataset
            String[] tokens = value.toString().split(",");

            if (tokens.length >= 3) {
                String whatPage = tokens[2]; // Assuming whatPage is the ID of the FaceInPage

                // Emit (whatPage, 1) for each access
                outputKey.set(whatPage);
                context.write(outputKey, one);
            }
        }
    }

    public static class ReduceClass extends Reducer<Text, IntWritable, Text, Text> {
        private Map<Text, IntWritable> pageToAccessCountMap = new HashMap<>();
        private Map<Text, String> pageToNameMap = new HashMap<>();
        private Map<Text, String> pageToNationalityMap = new HashMap<>();

        // Load FaceInPage data from DistributedCache
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                // Read data from the cache file (assuming it's in CSV format)
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path filePath = new Path(cacheFiles[0]);

                // Open and read the cache file
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
                String line;

                while ((line = reader.readLine()) != null) {
                    // Split the line into tokens (assuming CSV format)
                    String[] tokens = line.split(",");

                    // Ensure the input line has the expected number of tokens
                    if (tokens.length == 5) { // Assuming ID, Name, Nationality, CountryCod, Hobby columns
                        String pageID = tokens[0];
                        String name = tokens[1];
                        String nationality = tokens[2];

                        // Add the data to the map
                        pageToNameMap.put(new Text(pageID), name);
                        pageToNationalityMap.put(new Text(pageID), nationality);
                    }
                }
            }
        }
        /**
         * Reducer class for calculating access counts and identifying the top 10 most popular FaceInPages.
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int accessCount = 0;

            // Calculate the total number of accesses for the page
            for (IntWritable value : values) {
                accessCount += value.get();
            }

            // Store the access count in the map with the page ID as the key
            pageToAccessCountMap.put(new Text(key), new IntWritable(accessCount));
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Sort the pages by access count in descending order
            List<Map.Entry<Text, IntWritable>> sortedList = new ArrayList<>(pageToAccessCountMap.entrySet());
            sortedList.sort((entry1, entry2) -> entry2.getValue().compareTo(entry1.getValue()));

            // Output the top 10 most popular pages along with name and nationality
            int count = 0;
            for (Map.Entry<Text, IntWritable> entry : sortedList) {
                if (count >= 10) {
                    break; // Stop after the top 10
                }

                Text pageID = entry.getKey();
                String name = pageToNameMap.get(pageID);
                String nationality = pageToNationalityMap.get(pageID);

                // Emit the page ID as the key and name, nationality, and access count as the value
                context.write(pageID, new Text(name + "," + nationality + "," + entry.getValue()));
                count++;
            }
        }
    }
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top10FaceInPages");
        job.setJarByClass(Task_B.class);

        // Input and Output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Add the FaceInPage dataset file to the distributed cache
        DistributedCache.addCacheFile(new Path("hdfs://localhost:9000/project1/FaceInPage.csv").toUri(), job.getConfiguration());

        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        // Output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input and output paths
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/project1/AccessLogs.csv")); // Input path for AccessLogs
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project1/Output02")); // Output path for the result
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;

        System.out.println("Total execution time for TaskD: " + elapsedTime + " milliseconds.");

        // Wait for the job to complete and return the status
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
//1034ms