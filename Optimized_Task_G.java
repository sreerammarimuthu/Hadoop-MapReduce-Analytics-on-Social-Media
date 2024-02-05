import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

/**
 * Task_G is a Hadoop MapReduce program that identifies inactive persons based on their access
 * times in the AccessLogs dataset.
 *
 * Usage:
 * - Input: AccessLogs dataset (Assumed to have columns: AccessId, ByWho, WhatPage, TypeOfAccess, AccessTime)
 * - Output: A list of inactive persons who haven't accessed any page for the last 90 days.
 *
 * Dependencies:
 * - Hadoop MapReduce libraries.
 *
 * Execution:
 * - The program configuration and job setup are performed in the main method.
 * - The Mapper class (MapClass) extracts person IDs and their access times.
 * - The Reducer class (ReduceClass) identifies inactive persons based on access times.
 * - Output is written to a "inactivePersons" directory using MultipleOutputs.
 * - Input and output paths should be specified in the job configuration.
 *
 * @version 1.1
 */
public class Task_G {
    /**
     * Mapper class for extracting person IDs and access times.
     */
    public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Text outputKey = new Text();
        private LongWritable outputValue = new LongWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input line into tokens
            String[] tokens = value.toString().split(",");

            // Ensure the input line has the expected number of tokens (e.g., AccessLogs)
            if (tokens.length == 5) {
                // Extract the person's ID and AccessTime
                String personID = tokens[1];
                String accessTimeStr = tokens[4];

                try {
                    // Convert the access time to milliseconds (assuming it's in milliseconds)
                    long accessTimeMillis = Long.parseLong(accessTimeStr);

                    // Emit (personID, accessTimeMillis)
                    outputKey.set(personID);
                    outputValue.set(accessTimeMillis);
                    context.write(outputKey, outputValue);
                } catch (NumberFormatException e) {
                    // Handle parsing error, e.g., log it or ignore the invalid value
                    // Log the error
                    context.getCounter("ErrorCounterGroup", "NumberFormatException").increment(1);
                }
            }
        }
    }

    /**
     * Reducer class for identifying inactive persons.
     */
    public static class ReduceClass extends Reducer<Text, LongWritable, Text, NullWritable> {
        private Text outputKey = new Text();
        private static final long NINETY_DAYS_MILLIS = 90L * 24L * 60L * 60L * 1000L; // 90 days in milliseconds

        private MultipleOutputs<Text, NullWritable> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            // Find the latest access time for each person
            long latestAccessTimeMillis = 0L;

            for (LongWritable value : values) {
                long accessTimeMillis = value.get();

                if (accessTimeMillis > latestAccessTimeMillis) {
                    latestAccessTimeMillis = accessTimeMillis;
                }
            }

            // Get the current time in milliseconds
            long currentTimeMillis = System.currentTimeMillis();

            if (currentTimeMillis - latestAccessTimeMillis > NINETY_DAYS_MILLIS) {
                // Emit the person's ID as the output
                outputKey.set(key);
                multipleOutputs.write("inactivePersons", outputKey, NullWritable.get());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskG");

        job.setJarByClass(Task_G.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/project1/AccessLogs.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project1/Output07"));

        // Define named outputs for MultipleOutputs
        MultipleOutputs.addNamedOutput(job, "inactivePersons", TextOutputFormat.class, Text.class, NullWritable.class);
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;

        System.out.println("Total execution time for TaskG: " + elapsedTime + " milliseconds.");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
//829ms