import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Task_D {

    // Mapper reads Associates.csv and emits (PersonID, "A_flag").
    public static class AssociateMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 3) {
                context.write(new Text(fields[1]), new Text("A_flag"));
                context.write(new Text(fields[2]), new Text("A_flag"));
            }
        }
    }

    // Mapper reads FaceInPage.csv and emits (ID, "F_flag,<Name>").
    public static class FaceInPageMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 2) {
                context.write(new Text(fields[0]), new Text("F_flag," + fields[1]));
            }
        }
    }

    // Reducer joins Associate and FaceInPage info, outputs (ID, "A_flag,<count>") and (ID, "F_flag,<name>").
    public static class AssociateReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int accessCount = 0;
            String faceInName = null;

            for (Text val : values) {
                String[] split = val.toString().split(",", 2);
                if (split[0].equals("A_flag")) {
                    accessCount++;
                } else if (split[0].equals("F_flag")) {
                    faceInName = split.length > 1 ? split[1] : null;
                }
            }

            context.write(key, new Text("A_flag," + accessCount));
            if (faceInName != null) {
                context.write(key, new Text("F_flag," + faceInName));
            }
        }
    }

    /**
     * Mapper emits key-value pairs for JoinReducer: (ID, rest of line).
     */
    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",", 2);
            if (fields.length == 2) {
                context.write(new Text(fields[0]), new Text(fields[1]));
            }
        }
    }

    // Reducer joins both lines (from previous reducer output) and emits (Name, HappinessFactor).

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String name = null;
            String accessCount = null;

            for (Text val : values) {
                String[] parts = val.toString().split(",", 2);
                if (parts[0].equals("F_flag") && parts.length > 1) {
                    name = parts[1];
                } else if (parts[0].equals("A_flag") && parts.length > 1) {
                    accessCount = parts[1];
                }
            }

            if (name != null && accessCount != null) {
                context.write(new Text(name), new Text(accessCount));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Path tempPath = new Path("hdfs://localhost:9000/project1/Output04/Temp");
        Path finalPath = new Path("hdfs://localhost:9000/project1/Output04");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(tempPath)) fs.delete(tempPath, true);
        if (fs.exists(finalPath)) fs.delete(finalPath, true);

        long startTime = System.currentTimeMillis();

        // Job 1: Join Associates and FaceInPage
        Job job1 = Job.getInstance(conf, "JoinAssociatesFaceInPage");
        job1.setJarByClass(Task_D.class);
        MultipleInputs.addInputPath(job1, new Path("hdfs://localhost:9000/project1/Associates.csv"),
                TextInputFormat.class, AssociateMapper.class);
        MultipleInputs.addInputPath(job1, new Path("hdfs://localhost:9000/project1/FaceInPage.csv"),
                TextInputFormat.class, FaceInPageMapper.class);
        job1.setReducerClass(AssociateReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job1, tempPath);
        boolean success1 = job1.waitForCompletion(true);

        if (success1) {
            // Job 2: Join Results & Final Output
            Job job2 = Job.getInstance(conf, "JoinResults");
            job2.setJarByClass(Task_D.class);
            job2.setMapperClass(JoinMapper.class);
            job2.setReducerClass(JoinReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.setInputPaths(job2, tempPath);
            FileOutputFormat.setOutputPath(job2, finalPath);
            boolean success2 = job2.waitForCompletion(true);

            long endTime = System.currentTimeMillis();
            System.out.println("Total execution time for TaskD: " + (endTime - startTime) + " ms.");
            System.exit(success2 ? 0 : 1);
        } else {
            System.exit(1);
        }
    }
}

// New Execution Time: 16203ms (improved)
