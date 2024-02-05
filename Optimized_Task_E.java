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

/**
 * Task_E is a Hadoop MapReduce program that calculates the total number of accesses and
 * the distinct pages accessed by each owner based on AccessLogs data.
 *
 * Usage:
 * - Input: AccessLogs dataset (Assumed to have columns: AccessId, ByWho, WhatPage, TypeOfAccess, AccessTime)
 * - Output: Owner ID along with the total number of accesses and distinct pages accessed.
 *
 * Dependencies:
 * - Hadoop MapReduce libraries.
 *
 * Execution:
 * - The program configuration and job setup are performed in the main method.
 * - The Mapper class (Map) extracts owner IDs and page IDs from AccessLogs.
 * - The Reducer class (Reduce) calculates the total number of accesses and distinct pages for each owner.
 * - Custom partitioner and sorting comparator are set for sorting keys in ascending order.
 * - Input and output paths should be specified in the job configuration.
 *
 * @author Your Name
 * @version 1.0
 */
public class Task_E {

    /**
     * Mapper class for extracting owner IDs and page IDs.
     */
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private Text ownerId = new Text();
        private Text pageId = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");

            // Assuming the AccessLogs format: AccessId, ByWho, WhatPage, TypeOfAccess, AccessTime
            if (tokens.length >= 5) {
                ownerId.set(tokens[1].trim()); // Extract the owner's ID (ByWho)
                pageId.set(tokens[2].trim()); // Extract the accessed page's ID (WhatPage)
                context.write(ownerId, pageId);
            }
        }
    }

    /**
     * Reducer class for calculating the total number of accesses and distinct pages for each owner.
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> distinctPages = new HashSet<>();
            int totalAccesses = 0;

            for (Text value : values) {
                distinctPages.add(value.toString());
                totalAccesses++;
            }

            result.set("TotalAccesses: " + totalAccesses + ", DistinctPages: " + distinctPages.size());
            context.write(key, result);
        }
    }

    /**
     * Custom key comparator for sorting keys in ascending order.
     */
    public static class AscendingKeyComparator extends Text.Comparator {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            // Compare keys in ascending order
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "favorites");

        job.setJarByClass(Task_E.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set custom partitioner and sorting comparator
        job.setPartitionerClass(HashPartitioner.class);
        job.setSortComparatorClass(AscendingKeyComparator.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/project1/AccessLogs.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project1/Output05"));
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;

        System.out.println("Total execution time for TaskD: " + elapsedTime + " milliseconds.");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
//901ms