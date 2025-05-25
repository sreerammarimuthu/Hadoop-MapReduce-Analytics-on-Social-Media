import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.log4j.BasicConfigurator;

public class Task_C {

    // Mapper: emits (Nationality, 1) for each user
    public static class FaceInPageMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final Text nationality = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length == 5) {
                nationality.set(parts[2].trim());
                context.write(nationality, one);
            }
        }
    }

    // Reducer: sums the number of users per nationality
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
        Job job = Job.getInstance(conf, "Task_C");

        job.setJarByClass(Task_C.class);
        job.setMapperClass(FaceInPageMapper.class);
        job.setCombinerClass(FaceInPageReducer.class);
        job.setReducerClass(FaceInPageReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path inputPath = new Path("hdfs://localhost:9000/project1/FaceInPage.csv");
        Path outputPath = new Path("hdfs://localhost:9000/project1/Output03");

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Clean output path if it exists
        FileSystem.get(conf).delete(outputPath, true);

        // Enable gzip compression
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        System.out.println("Total execution time for Task C: " + (endTime - startTime) + " ms");

        System.exit(success ? 0 : 1);
    }
}

//Execution Time: 1309ms
