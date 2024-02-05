import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Task_C {

    public static class FaceInPageMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final Text country = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");
            if (parts.length == 5) {
                String countryCode = parts[2].trim();
                country.set(countryCode);
                context.write(country, one);
            }
        }
    }

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
        job.setCombinerClass(FaceInPageReducer.class); // Use the same class as a combiner
        job.setReducerClass(FaceInPageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Enable output compression
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.GzipCodec.class);

        // Use HDFS paths for input and output
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/project1/FaceInPage.csv")); // Adjust the input path
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project1/Output03")); // Adjust the output path

        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;

        System.out.println("Total execution time for Task_C: " + elapsedTime + " milliseconds.");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
//1309ms