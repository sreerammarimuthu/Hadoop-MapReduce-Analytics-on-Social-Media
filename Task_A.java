import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Task_A {

    public static void main(String[] args) throws Exception {
        // Record the start time before starting the job
        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task_A");
        BasicConfigurator.configure();
        job.setJarByClass(Task_A.class);
        job.setJobName("FindUsersWithSameNationality");

        job.setMapperClass(NationalityMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Specify the input and output paths using Hadoop Path objects
        Path inputPath = new Path("hdfs://localhost:9000/project1/FaceInPage.csv"); // Adjust the input path
        Path outputPath = new Path("hdfs://localhost:9000/project1/Output01"); // Adjust the output path
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Record the end time after configuring the job
        long endTime = System.currentTimeMillis();

        // Calculate the elapsed time
        long elapsedTime = endTime - startTime;

        // Print the total execution time for Task A
        System.out.println("Total execution time for Task A: " + elapsedTime + " milliseconds.");
        // Wait for the job to complete and exit with a success status
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class NationalityMapper extends Mapper<Object, Text, Text, Text> {
        private static final String YOUR_NATIONALITY = "India";

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (fields.length >= 4) {
                String nationality = fields[2].trim();
                String name = fields[1].trim();
                String hobby = fields[4].trim();

                if (nationality.equals(YOUR_NATIONALITY)) {
                    context.write(new Text(name), new Text(hobby));
                }
            }
        }
    }
}
//1067ms