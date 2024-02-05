import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import java.util.HashSet;
import java.util.Set;

/**
 * Task_F is a Hadoop MapReduce program that identifies people with relationships who have
 * never accessed their friend's FaceInPage based on Associates and AccessLogs data.
 *
 * Usage:
 * - Input: Associates dataset (Assumed to have columns: FriendRel, PersonA_ID, PersonB_ID, ...)
 *          AccessLogs dataset (Assumed to have columns: AccessId, ByWho, WhatPage, TypeOfAccess, AccessTime)
 * - Output: Person IDs who have never accessed their friend's FaceInPage.
 *
 * Dependencies:
 * - Hadoop MapReduce libraries.
 *
 * Execution:
 * - The program configuration and job setup are performed in the main method.
 * - The Mapper class (Map) extracts PersonA_ID and PersonB_ID from the Associates dataset.
 * - The Reducer class (Reduce) checks if each person has accessed their friend's FaceInPage.
 * - Input and output paths should be specified in the job configuration.
 *
 * @version 1.1
 */
public class Task_F {

    /**
     * Mapper class for extracting PersonA_ID and PersonB_ID.
     */
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private Text personA = new Text();
        private Text personB = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");

            // Assuming the Associates format: FriendRel, PersonA_ID, PersonB_ID, ...
            if (tokens.length >= 3) {
                personA.set(tokens[1].trim()); // Extract PersonA_ID
                personB.set(tokens[2].trim()); // Extract PersonB_ID
                context.write(personB, personA); // Emit PersonB_ID as key and PersonA_ID as value
            }
        }
    }

    /**
     * Reducer class for identifying people who have never accessed their friend's FaceInPage.
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text noAccess = new Text("Never Accessed");

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> friends = new HashSet<>();

            // Collect all friends of the person
            for (Text value : values) {
                friends.add(value.toString());
            }

            // Check if the person has accessed their friend's FaceInPage
            boolean hasAccess = false;
            for (String friend : friends) {
                if (friends.contains(friend)) {
                    hasAccess = true;
                    break;
                }
            }

            // If no access is found, emit the person's ID as "Never Accessed"
            if (!hasAccess) {
                context.write(key, noAccess);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "peoplewithrelationshipsnoaccess");

        job.setJarByClass(Task_F.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/project1/AccessLogs.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project1/Output06"));

        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;

        System.out.println("Total execution time for TaskF: " + elapsedTime + " milliseconds.");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
//769ms