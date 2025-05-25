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

    public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Text personID = new Text();
        private final LongWritable accessTime = new LongWritable();

        // Mapper emits (PersonID, AccessTime) for each access record
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length == 5) {
                try {
                    personID.set(fields[1].trim());
                    accessTime.set(Long.parseLong(fields[4].trim()));
                    context.write(personID, accessTime);
                } catch (NumberFormatException e) {
                    context.getCounter("ErrorCounterGroup", "InvalidAccessTime").increment(1);
                }
            }
        }
    }

    public static class ReduceClass extends Reducer<Text, LongWritable, Text, NullWritable> {
        private static final long NINETY_DAYS_MS = 90L * 24 * 60 * 60 * 1000;  // 90 days in milliseconds
        private MultipleOutputs<Text, NullWritable> mos;

        // Setup initializes MultipleOutputs for writing inactive users
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<>(context);
        }

        // Reducer emits (PersonID, null) only if their latest access was more than 90 days ago
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long latestAccess = 0L;

            for (LongWritable val : values) {
                if (val.get() > latestAccess) {
                    latestAccess = val.get();
                }
            }

            long now = System.currentTimeMillis();
            if ((now - latestAccess) > NINETY_DAYS_MS) {
                mos.write("inactivePersons", key, NullWritable.get());
            }
        }

        // Cleanup closes MultipleOutputs
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task_G");
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
        System.out.println("Total execution time: " + (endTime - startTime) + " milliseconds.");
        System.exit(success ? 0 : 1);
    }
}

// Execution Time: 997ms
