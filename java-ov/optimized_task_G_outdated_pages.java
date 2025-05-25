import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Task_G {

    // Mapper emits (PersonID, AccessTime)
    public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Text outputKey = new Text();
        private final LongWritable outputValue = new LongWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");

            if (tokens.length == 5) {
                try {
                    outputKey.set(tokens[1].trim());
                    outputValue.set(Long.parseLong(tokens[4].trim()));
                    context.write(outputKey, outputValue);
                } catch (NumberFormatException e) {
                    context.getCounter("ErrorCounter", "InvalidTimestamp").increment(1);
                }
            }
        }
    }

    // Reducer emits (PersonID) if last access > 90 days ago
    public static class ReduceClass extends Reducer<Text, LongWritable, Text, NullWritable> {
        private final static long NINETY_DAYS_MS = 90L * 24 * 60 * 60 * 1000;
        private MultipleOutputs<Text, NullWritable> mos;

        @Override
        protected void setup(Context context) {
            mos = new MultipleOutputs<>(context);
        }

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long maxAccessTime = 0;
            for (LongWritable val : values) {
                maxAccessTime = Math.max(maxAccessTime, val.get());
            }

            long currentTime = System.currentTimeMillis();
            if (currentTime - maxAccessTime > NINETY_DAYS_MS) {
                mos.write("inactivePersons", key, NullWritable.get());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "InactiveUsers");

        job.setJarByClass(Task_G.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/project1/AccessLogs.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project1/Output07"));

        MultipleOutputs.addNamedOutput(job, "inactivePersons", TextOutputFormat.class, Text.class, NullWritable.class);

        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        System.out.println("Total execution time for Task_G: " + (endTime - startTime) + " milliseconds.");
        System.exit(success ? 0 : 1);
    }
}

// New Execution Time: 829ms (improved)
