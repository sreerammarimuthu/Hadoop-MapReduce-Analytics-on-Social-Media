import java.io.*;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.log4j.BasicConfigurator;

public class Task_B {

    // Mapper: Emits (WhatPage, 1) for each access log entry
    public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text pageId = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 3) {
                pageId.set(fields[2].trim());
                context.write(pageId, one);
            }
        }
    }

    // Reducer: Loads FaceInPage data, aggregates counts, and emits top 10 pages
    public static class ReduceClass extends Reducer<Text, IntWritable, Text, Text> {
        private final Map<String, Integer> pageCountMap = new HashMap<>();
        private final Map<String, String> nameMap = new HashMap<>();
        private final Map<String, String> nationalityMap = new HashMap<>();

        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path path = new Path(cacheFiles[0]);
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line;

                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split(",");
                    if (fields.length == 5) {
                        String id = fields[0].trim();
                        nameMap.put(id, fields[1].trim());
                        nationalityMap.put(id, fields[2].trim());
                    }
                }
                reader.close();
            }
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            pageCountMap.put(key.toString(), sum);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<String, Integer>> sortedPages = new ArrayList<>(pageCountMap.entrySet());
            sortedPages.sort((a, b) -> b.getValue() - a.getValue());

            int count = 0;
            for (Map.Entry<String, Integer> entry : sortedPages) {
                if (count++ >= 10) break;

                String id = entry.getKey();
                String name = nameMap.getOrDefault(id, "Unknown");
                String nationality = nationalityMap.getOrDefault(id, "Unknown");
                int accessCount = entry.getValue();

                context.write(new Text(id), new Text(name + "," + nationality + "," + accessCount));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task_B");

        job.setJarByClass(Task_B.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Input/output paths
        Path inputPath = new Path("hdfs://localhost:9000/project1/AccessLogs.csv");
        Path outputPath = new Path("hdfs://localhost:9000/project1/Output02");

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Clean output directory if it exists
        FileSystem.get(conf).delete(outputPath, true);

        // Add FaceInPage.csv to distributed cache
        job.addCacheFile(new URI("hdfs://localhost:9000/project1/FaceInPage.csv"));

        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        System.out.println("Total execution time for Task B: " + (endTime - startTime) + " ms");

        System.exit(success ? 0 : 1);
    }
}

// Execution Time: 1034ms
