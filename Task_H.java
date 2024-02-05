import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * Task_H is a Hadoop MapReduce program that determines whether owners in the dataset are more popular
 * based on their relationships in the Associates dataset.
 *
 * Usage:
 * - Input: Associates dataset (Assumed to have columns: FriendRel, PersonA_ID, PersonB_ID, ...)
 * - Output: Owners labeled as "More Popular" or "Not More Popular" based on their relationship count.
 *
 * Dependencies:
 * - Hadoop MapReduce libraries.
 *
 * Execution:
 * - The program configuration and job setup are performed in the main method.
 * - The Mapper class (MapClass) distinguishes data from FaceInPage and Associates, emitting (ownerID, 0) or (ownerID, 1) respectively.
 * - The Reducer class (ReduceClass) calculates the average number of relationships and labels owners accordingly.
 * - Input and output paths should be specified in the job configuration.
 *
 * @author Your Name
 * @version 1.0
 */
public class Task_H {
    /**
     * Mapper class for distinguishing data from FaceInPage and Associates and emitting appropriate values.
     */
    public static class MapClass extends Mapper<Object, Text, Text, IntWritable> {
        private Text ownerID = new Text();
        private final IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(","); // Assuming data is comma-separated

            // Check the source of the data (FaceInPage or Associates)
            if (fields.length >= 2) {
                // Data from FaceInPage
                String ownerId = fields[0]; // Assuming ID is in the first column
                ownerID.set(ownerId);
                context.write(ownerID, new IntWritable(0)); // Emit (ownerID, 0)
            } else if (fields.length >= 3) {
                // Data from Associates
                String personAId = fields[1]; // Assuming PersonA_ID is in the second column
                ownerID.set(personAId);
                context.write(ownerID, one); // Emit (ownerID, 1)
            }
        }
    }

    /**
     * Reducer class for calculating the average number of relationships and labeling owners.
     */
    public static class ReduceClass extends Reducer<Text, IntWritable, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int relationshipCount = 0;
            int userCount = 0;

            for (IntWritable value : values) {
                if (value.get() == 1) {
                    // Count relationships (value=1)
                    relationshipCount++;
                } else {
                    // Count users (value=0)
                    userCount++;
                }
            }

            // Calculate the average number of relationships
            int totalOwners = userCount + relationshipCount;
            double averageRelationships = (double) relationshipCount / totalOwners;

            // Compare the number of relationships for each owner with the average
            if (relationshipCount > averageRelationships) {
                // Owner is more popular than the average
                result.set("More Popular");
            } else {
                result.set("Not More Popular");
            }

            // Emit the result
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskH");
        job.setJarByClass(Task_H.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/project1/Associates.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project1/Output08"));
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;

        System.out.println("Total execution time for TaskH: " + elapsedTime + " milliseconds.");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
//805ms