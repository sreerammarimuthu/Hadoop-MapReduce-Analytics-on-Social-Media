import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.log4j.BasicConfigurator;

public class Task_D {

    // Mapper for Associates.csv – emits (PersonID, A_flag)
    public static class AssociateMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 3) {
                context.write(new Text(fields[1].trim()), new Text("A_flag"));
                context.write(new Text(fields[2].trim()), new Text("A_flag"));
            }
        }
    }

    // Mapper for FaceInPage.csv – emits (ID, F_flag, Name, full line)
    public static class FaceInPagesMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 5) {
                context.write(new Text(fields[0].trim()), new Text("F_flag," + fields[1].trim() + "," + value.toString()));
            }
        }
    }

    // Reducer for access count & passing forward FaceInPage info
    public static class AssociateReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int accessCount = 0;
            String faceInPageRecord = null;

            for (Text val : values) {
                String value = val.toString();
                if (value.equals("A_flag")) {
                    accessCount++;
                } else if (value.startsWith("F_flag")) {
                    faceInPageRecord = value;
                }
            }

            if (faceInPageRecord != null) {
                // Emit intermediate result
                context.write(key, new Text("A_flag," + accessCount));
                context.write(key, new Text(faceInPageRecord));
            }
        }
    }

    // Mapper for joining intermediate output – emits (ID, rest of line)
    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",", 2);
            if (fields.length == 2) {
                context.write(new Text(fields[0]), new Text(fields[1]));
            }
        }
    }

    // Final reducer – outputs (Name, HappinessFactor)
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String name = null;
            String happinessFactor = null;

            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("F_flag")) {
                    String[] parts = value.split(",");
                    if (parts.length >= 3) {
                        name = parts[1].trim();
                    }
                } else if (value.startsWith("A_flag")) {
                    String[] parts = value.split(",");
                    if (parts.length == 2) {
                        happinessFactor = parts[1].trim();
                    }
                }
            }

            if (name != null && happinessFactor != null) {
                context.write(new Text(name), new Text(happinessFactor));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Path associatesPath = new Path("hdfs://localhost:9000/project1/Associates.csv");
        Path faceInPagePath = new Path("hdfs://localhost:9000/project1/FaceInPage.csv");
        Path tempPath = new Path("hdfs://localhost:9000/project1/Output04/Temp");
        Path finalPath = new Path("hdfs://localhost:9000/project1/Output04");

        FileSystem fs = FileSystem.get(conf);
        fs.delete(tempPath, true);
        fs.delete(finalPath, true);

        // Job 1: Aggregates access counts per user
        Job job1 = Job.getInstance(conf, "Access Count");
        job1.setJarByClass(Task_D.class);

        MultipleInputs.addInputPath(job1, associatesPath, TextInputFormat.class, AssociateMapper.class);
        MultipleInputs.addInputPath(job1, faceInPagePath, TextInputFormat.class, FaceInPagesMapper.class);

        job1.setReducerClass(AssociateReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job1, tempPath);

        long startTime = System.currentTimeMillis();
        boolean job1Success = job1.waitForCompletion(true);

        if (job1Success) {
            // Job 2: Join name with access count
            Job job2 = Job.getInstance(conf, "Join Results");
            job2.setJarByClass(Task_D.class);

            job2.setMapperClass(JoinMapper.class);
            job2.setReducerClass(JoinReducer.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job2, tempPath);
            FileOutputFormat.setOutputPath(job2, finalPath);

            boolean job2Success = job2.waitForCompletion(true);
            long endTime = System.currentTimeMillis();

            System.out.println("Total execution time for Task D: " + (endTime - startTime) + " ms");
            System.exit(job2Success ? 0 : 1);
        } else {
            System.exit(1);
        }
    }
}

// Execution Time: 16566
