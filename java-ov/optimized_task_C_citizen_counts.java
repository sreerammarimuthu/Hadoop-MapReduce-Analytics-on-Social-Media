import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Task_C {

    // Mapper extracts (CountryCode, 1) from each FaceInPage entry.
    public static class FaceInPageMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text countryCode = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length == 5) {
                String code = fields[2].trim(); // CountryCode is at index 2
                countryCode.set(code);
                context.write(countryCode, one);
            }
        }
    }

    // Reducer aggregates FaceInPage counts per country code.
    public static class FaceInPageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FaceInPages Per Country");

        job.setJarByClass(Task_C.class);
        job.setMapperClass(FaceInPageMapper.class);
        job.setCombinerClass(FaceInPageReducer.class);  // Optimization: Combiner used to reduce shuffle
        job.setReducerClass(FaceInPageReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/project1/FaceInPage.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project1/Output03"));

        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        System.out.println("Total execution time: " + (endTime - startTime) + " milliseconds.");

        System.exit(success ? 0 : 1);
    }
}

// New Execution Time: 844ms (improved)
