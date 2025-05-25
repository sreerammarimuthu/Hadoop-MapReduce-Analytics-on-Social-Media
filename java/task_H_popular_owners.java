import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Task_H {

    public static class MapClass extends Mapper<Object, Text, Text, IntWritable> {
        private final Text ownerID = new Text();
        private static final IntWritable one = new IntWritable(1);
        private static final IntWritable zero = new IntWritable(0);

        // Mapper emits (ownerID, 1) for each relationship and (ownerID, 0) if user exists
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (fields.length == 2) { // From FaceInPage.csv (ID, Name, etc.)
                ownerID.set(fields[0].trim());
                context.write(ownerID, zero);
            } else if (fields.length >= 3) { // From Associates.csv (FriendRel, PersonA_ID, PersonB_ID, ...)
                ownerID.set(fields[1].trim());
                context.write(ownerID, one);
            }
        }
    }

    public static class ReduceClass extends Reducer<Text, IntWritable, Text, Text> {
        private final Text result = new Text();

        // Reducer emits (ownerID, label) based on relationship frequency
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            int relationships = 0;

            for (IntWritable val : values) {
                total++;
                if (val.get() == 1) relationships++;
            }

            // If a person has more than 1 relationship, tag as "More Popular"
            if (relationships > 1) {
                result.set("More Popular");
            } else {
                result.set("Not More Popular");
            }

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task_H");

        job.setJarByClass(Task_H.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/project1/Associates.csv"));  // Can be updated to MultipleInputs if joining
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project1/Output08"));

        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        System.out.println("Total execution time: " + (endTime - startTime) + " milliseconds.");
        System.exit(success ? 0 : 1);
    }
}

// Execution Time: 805ms
