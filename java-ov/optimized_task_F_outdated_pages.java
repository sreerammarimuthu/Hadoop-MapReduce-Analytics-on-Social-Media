import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Task_F {

    // Mapper emits (PersonA_ID, "FRIEND:PersonB_ID") and (PersonA_ID, "ACCESS:PageID")
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private final Text keyOut = new Text();
        private final Text valOut = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length == 3) {
                keyOut.set(tokens[1].trim());
                valOut.set("FRIEND:" + tokens[2].trim());
                context.write(keyOut, valOut);
            } else if (tokens.length == 5) {
                keyOut.set(tokens[1].trim());
                valOut.set("ACCESS:" + tokens[2].trim());
                context.write(keyOut, valOut);
            }
        }
    }

    // Reducer emits (PersonA_ID, "Never Accessed") if none of their friends' pages were accessed
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private final Text output = new Text("Never Accessed");

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> friends = new HashSet<>();
            Set<String> accessedPages = new HashSet<>();

            for (Text val : values) {
                String valStr = val.toString();
                if (valStr.startsWith("FRIEND:")) {
                    friends.add(valStr.substring(7));
                } else if (valStr.startsWith("ACCESS:")) {
                    accessedPages.add(valStr.substring(7));
                }
            }

            boolean hasAccessedFriend = false;
            for (String friend : friends) {
                if (accessedPages.contains(friend)) {
                    hasAccessedFriend = true;
                    break;
                }
            }

            if (!hasAccessedFriend && !friends.isEmpty()) {
                context.write(key, output);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task_F");

        job.setJarByClass(Task_F.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/project1/Merged_Associates_AccessLogs.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project1/Output06"));

        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        System.out.println("Total execution time for Task_F: " + (endTime - startTime) + " milliseconds.");
        System.exit(success ? 0 : 1);
    }
}

// New Execution Time: 769ms (improved)
