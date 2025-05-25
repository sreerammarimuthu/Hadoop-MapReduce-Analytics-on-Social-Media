import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Task_H {

    // Mapper emits (OwnerID, 1) if from Associates, (OwnerID, 0) if from FaceInPage
    public static class MapClass extends Mapper<Object, Text, Text, IntWritable> {
        private final Text ownerID = new Text();
        private final IntWritable one = new IntWritable(1);
        private final IntWritable zero = new IntWritable(0);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (fields.length == 2) {
                // FaceInPage (assuming ID, Name)
                ownerID.set(fields[0].trim());
                context.write(ownerID, zero);
            } else if (fields.length >= 3) {
                // Associates (assuming FriendRel, PersonA_ID, PersonB_ID)
                ownerID.set(fields[1].trim());
                context.write(ownerID, one);
            }
        }
    }

    // Reducer emits (OwnerID, "More Popular") or (OwnerID, "Not More Popular")
    public static class ReduceClass extends Reducer<Text, IntWritable, Text, Text> {
        private final Text result = new Text();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int relationships = 0;
            int ownerPresence = 0;

            for (IntWritable value : values) {
                if (value.get() == 1) relationships++;
                else ownerPresence++;
            }

            // Emit popularity label based on relationship count
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
        Job job = Job.getInstance(conf, "PopularOwners");

        job.setJarByClass(Task_H.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/project1/Associates.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project1/Output08"));

        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        System.out.println("Total execution time for Task_H: " + (endTime - startTime) + " milliseconds.");
        System.exit(success ? 0 : 1);
    }
}

// New Execution Time: 738ms (improved)
