import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Task_F {
    // Mapper: Emits (PersonB_ID, "ASSOC:PersonA_ID")
    public static class AssociatesMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length >= 3) {
                String personA = tokens[1].trim();
                String personB = tokens[2].trim();
                context.write(new Text(personB), new Text("ASSOC:" + personA));
            }
        }
    }

    // Mapper: Emits (WhatPage, "ACCESS:ByWho")
    public static class AccessLogsMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length >= 3) {
                String whatPage = tokens[2].trim();  // page being accessed
                String byWho = tokens[1].trim();     // person accessing
                context.write(new Text(whatPage), new Text("ACCESS:" + byWho));
            }
        }
    }

    // Reducer: if PersonA never accessed PersonB's page, write PersonA
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> friends = new HashSet<>();
            Set<String> accessors = new HashSet<>();

            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("ASSOC:")) {
                    friends.add(value.substring(6));
                } else if (value.startsWith("ACCESS:")) {
                    accessors.add(value.substring(7));
                }
            }

            for (String friend : friends) {
                if (!accessors.contains(friend)) {
                    context.write(new Text(friend), new Text("Never Accessed Friend’s Page"));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task_F");

        job.setJarByClass(Task_F.class);
        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path associatesPath = new Path("hdfs://localhost:9000/project1/Associates.csv");
        Path accessLogsPath = new Path("hdfs://localhost:9000/project1/AccessLogs.csv");
        Path outputPath = new Path("hdfs://localhost:9000/project1/Output06");

        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputPath, true);  // clean previous output

        MultipleInputs.addInputPath(job, associatesPath, TextInputFormat.class, AssociatesMapper.class);
        MultipleInputs.addInputPath(job, accessLogsPath, TextInputFormat.class, AccessLogsMapper.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        System.out.println("Total execution time for Task F: " + (endTime - startTime) + " ms");
        System.exit(success ? 0 : 1);
    }
}

// Execution Time: 798ms
