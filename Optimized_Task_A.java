import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;


/**
 * This program is a Hadoop MapReduce job designed to find users with the same nationality,
 * based on a given nationality criteria. It reads data from a CSV file containing user information
 * and filters users based on their nationality, outputting user names and hobbies of users matching
 * the specified nationality.
 */
public class Task_A {

    /**
     * The main entry point of the Hadoop MapReduce job.
     *
     * @param args Command-line arguments for the job.
     * @throws Exception If an error occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        // Record the start time before starting the job
        long startTime = System.currentTimeMillis();

        // Configure log4j for logging
        BasicConfigurator.configure();

        // Create a Hadoop configuration object
        Configuration conf = new Configuration();

        // Create a new MapReduce job instance with a descriptive name
        Job job = Job.getInstance(conf, "Task_A");

        // Clean up the output directory before running the job
        FileSystem.get(conf).delete(new Path("C://Users//user//IdeaProjects//Project01"), true);

        // Set the main class for the job
        job.setJarByClass(Task_A.class);

        // Set a custom job name for clarity
        job.setJobName("FindUsersWithSameNationality");

        // Set the Mapper class for this job
        job.setMapperClass(NationalityMapper.class);

        // Set the output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the input and output paths for the job
        FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/project1/FaceInPage.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project1/Output01"));

        // Record the end time after configuring the job
        long endTime = System.currentTimeMillis();

        // Calculate the elapsed time
        long elapsedTime = endTime - startTime;

        // Print the total execution time for Task A
        System.out.println("Total execution time for Task A: " + elapsedTime + " milliseconds.");

        // Wait for the job to complete and exit with a success status
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * This Mapper class extracts user information from input records and emits user names and hobbies
     * for users with the specified nationality criteria.
     */
    public static class NationalityMapper extends Mapper<Object, Text, Text, Text> {
        // Define the nationality to filter by (change this to your desired nationality)
        private static final String YOUR_NATIONALITY = "India";

        /**
         * The map function processes input records and emits key-value pairs.
         *
         * @param key     The current input key (unused).
         * @param value   The current input value (a line from the CSV file).
         * @param context The context for emitting key-value pairs.
         * @throws IOException          If an I/O error occurs.
         * @throws InterruptedException If the operation is interrupted.
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input line into fields
            String[] fields = value.toString().split(",");

            // Check if the input record has at least four fields
            if (fields.length >= 4) {
                String nationality = fields[2].trim();
                String name = fields[1].trim();
                String hobby = fields[4].trim();

                // Check if the user's nationality matches the specified criteria
                if (nationality.equals(YOUR_NATIONALITY)) {
                    // Emit the user's name as the key and hobby as the value
                    context.write(new Text(name), new Text(hobby));
                }
            }
        }
    }
}
//800ms