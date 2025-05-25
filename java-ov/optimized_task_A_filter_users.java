import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Task_A {

    public static class NationalityMapper extends Mapper<Object, Text, Text, Text> {
        private static final String YOUR_NATIONALITY = "India";

        // Mapper emits (Name, Hobby) if nationality matches
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (fields.length >= 5) {
                String nationality = fields[2].trim();
                String name = fields[1].trim();
                String hobby = fields[4].trim();

                if (nationality.equalsIgnoreCase(YOUR_NATIONALITY)) {
                    context.write(new Text(name), new Text(hobby));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "NationalityFilter");

        job.setJarByClass(Task_A.class);
        job.setMapperClass(NationalityMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input/output paths
        Path inputPath = new Path("hdfs://localhost:9000/project1/FaceInPage.csv");
        Path outputPath = new Path("hdfs://localhost:9000/project1/Output01");

        FileInputFormat.addInputPath(job, inputPath);
        FileSystem.get(conf).delete(outputPath, true);  // Ensure fresh output
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();

        System.out.println("Total execution time: " + (endTime - startTime) + " milliseconds.");
        System.exit(success ? 0 : 1);
    }
}

// New Execution Time: 800ms (improved)
