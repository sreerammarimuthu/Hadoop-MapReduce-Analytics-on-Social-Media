import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.Arrays;

public class Task_D {
    /**
     * Determine which people have favorites. That is, for each FaceInPage owner, determine
     * how many total accesses to FaceInPage they have made (as reported in the AccessLog)
     * and how many distinct FaceInPages they have accessed in total.
     */
    public static class AssociateMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().split(",").length >= 4) {
                context.write(new Text(value.toString().split(",")[1]), new Text("A_flag"));
                context.write(new Text(value.toString().split(",")[2]), new Text("A_flag"));
            }
        }
    }

    public static class FaceInPagesMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().split(",").length >= 4) {
                context.write(new Text(value.toString().split(",")[0]), new Text("F_flag," + value.toString().split(",")[1] + "," + value));
            }
        }
    }

    public static class AssociateReducer extends Reducer<Text, Text, Text, Text> {
        /**
         * If receives A_flag, then calculate the totalAccesses and distinctPages
         * If receives F_flag, then do nothing and forward the tuple to the next job
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalAccesses = 0;
            Text F_value = new Text();

            // Get the access count for each pageId
            for (Text value : values) {
                if (value.toString().equals("A_flag")) { // Corrected comparison here
                    // From the AccessLogs table then we calculate the totalAccessCount
                    totalAccesses++;
                } else if (value.toString().startsWith("F_flag")) {
                    // From the FaceInPages.csv so just output it directly to context
                    F_value.set(value.toString());
                }
            }

            // Emit values
            context.write(key, new Text("A_flag," + totalAccesses));
            context.write(key, F_value);
        }
    }

    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        // Emits key_value pairs from the temp_output folder for the next reducer to ingest

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text(value.toString().split(",")[0]), new Text(String.join(",", Arrays.copyOfRange(value.toString().split(","), 1, value.toString().split(",").length))));
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String name = null;
            String happinessFactor = null;

            // Iterate over values and split them based on the flag
            for (Text val : values) {
                if (val.toString().startsWith("F_flag")) {
                    // Parse the "F_flag" value to extract the name and happiness factor
                    String[] parts = val.toString().split(",");
                    if (parts.length >= 8) {
                        name = parts[2].trim();
                        happinessFactor = parts[7].trim();
                    }
                }
            }

            // Check if both name and happinessFactor are not null, and emit them
            if (name != null && happinessFactor != null) {
                context.write(new Text(name), new Text(happinessFactor));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        // Clean up output directory
        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path("hdfs://localhost:9000/project1/Output04");
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        // Clean up temporary directory
        Path tempPath = new Path("hdfs://localhost:9000/project1/Output04/Temp");
        if (fs.exists(tempPath)) {
            fs.delete(tempPath, true);
        }

        // Job1: Access Count
        Job job1 = Job.getInstance(conf, "Access Count");

        job1.setJarByClass(Task_D.class);
        // Configure multiple input for the first job
        MultipleInputs.addInputPath(job1, new Path("hdfs://localhost:9000/project1/Associates.csv"), TextInputFormat.class, Task_D.AssociateMapper.class);
        MultipleInputs.addInputPath(job1, new Path("hdfs://localhost:9000/project1/FaceInPage.csv"), TextInputFormat.class, Task_D.FaceInPagesMapper.class);

        job1.setReducerClass(Task_D.AssociateReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job1, tempPath); // Temporary output path

        // Record start time before starting job1
        long startTime = System.currentTimeMillis();

        boolean job1Success = job1.waitForCompletion(false);
        System.out.println("OK");
        if (job1Success) {
            // Job2: Join Results
            Job job2 = Job.getInstance(conf, "Join Results");
            job2.setJarByClass(Task_D.class);
            job2.setMapperClass(JoinMapper.class);
            job2.setReducerClass(JoinReducer.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, tempPath); // Temporary output path
            FileOutputFormat.setOutputPath(job2, outPath); // Final output path

            boolean success = job2.waitForCompletion(false);
            System.out.println("OK1");

            // Calculate and print the elapsed time after job2 finishes
            long endTime = System.currentTimeMillis();
            long elapsedTime = endTime - startTime;

            System.out.println("Total execution time for TaskD: " + elapsedTime + " milliseconds.");
            System.exit(success ? 0 : 1);
        } else {
            System.exit(1);
        }
    }
}
//16566