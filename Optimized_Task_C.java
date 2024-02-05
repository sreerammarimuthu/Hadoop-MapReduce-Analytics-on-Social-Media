import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

/**
 * Task_C is a Hadoop MapReduce program that counts the number of FaceInPages for each country code.
 * It reads the FaceInPage dataset and outputs the count of FaceInPages per country code.
 *
 * Usage:
 * - Input: FaceInPage dataset (Assumed to have columns: ID, Name, CountryCode, Nationality, Hobby)
 * - Output: The count of FaceInPages for each country code.
 *
 * Dependencies:
 * - Hadoop MapReduce libraries.
 *
 * Execution:
 * - The program configuration and job setup are performed in the main method.
 * - The Mapper class (FaceInPageMapper) extracts country codes from the FaceInPage dataset.
 * - The Reducer class (FaceInPageReducer) counts the occurrences of each country code.
 * - Input and output paths should be specified in the job configuration.
 *
 * @author Your Name
 * @version 1.0
 */
public class Task_C {

    /**
     * Mapper class for extracting country codes from the FaceInPage dataset.
     * Emits (country code, 1) for each FaceInPage.
     */
    public static class FaceInPageMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text country = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");
            if (parts.length == 5) {
                String countryCod = parts[2].trim();
                country.set(countryCod);
                context.write(country, one);
            }
        }
    }

    /**
     * Reducer class for counting the number of FaceInPages per country code.
     */
    public static class FaceInPageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task_C");
        job.setJarByClass(Task_C.class);
        job.setMapperClass(FaceInPageMapper.class);
        job.setCombinerClass(FaceInPageReducer.class);
        job.setReducerClass(FaceInPageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/project1/FaceInPage.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project1/Output03"));
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;

        System.out.println("Total execution time for TaskD: " + elapsedTime + " milliseconds.");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
//844ms