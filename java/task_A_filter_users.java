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

public class Task_A {

    public static void main(String[] args) throws Exception {
        // Starting timer
        long startTime = System.currentTimeMillis();
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task_A");

        job.setJarByClass(Task_A.class);
        job.setJobName("FindUsersWithSameNationality");

        job.setMapperClass(NationalityMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input and output paths
        Path inputPath = new Path("hdfs://localhost:9000/project1/FaceInPage.csv");
        Path outputPath = new Path("hdfs://localhost:9000/project1/Output01");

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Clean output path if it already exists
        FileSystem.get(conf).delete(outputPath, true);

        // Submit job and wait for completion
        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        System.out.println("Total execution time for Task A: " + (endTime - startTime) + " ms");

        System.exit(success ? 0 : 1);
    }

    public static class NationalityMapper extends Mapper<Object, Text, Text, Text> {
        private static final String YOUR_NATIONALITY = "India";

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (fields.length >= 5) {
                String name = fields[1].trim();
                String nationality = fields[2].trim();
                String hobby = fields[4].trim();

                if (nationality.equals(YOUR_NATIONALITY)) {
                    context.write(new Text(name), new Text(hobby));
                }
            }
        }
    }
}

// Execution Time: 1067ms
