package segundosemestre.graphmaker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import segundosemestre.NodeInfluence;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

public class MainGraphMaker {

    private static final String dir = System.getProperty("user.dir");
    static final int TOTAL_VERTICES = 10;
    private static final String FILE_NAME = "src/main/resources/graphVertices.csv";

    public static void main(String[] args) throws Exception {
        Logger.getRootLogger().addAppender(new ConsoleAppender(new PatternLayout()));

        generateGraphVertices();

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://localhost:9000");
        Job job = Job.getInstance(conf, "GraphInfluenceMaker");

        job.setJarByClass(MainGraphMaker.class);
        job.setMapperClass(GraphMaker.class);
        job.setMapOutputValueClass(NodeInfluence.class);
        job.setReducerClass(GraphMakerReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        Path path = new Path("file:///" + dir + "/src/main/resources/graphVertices.csv");
        FileInputFormat.addInputPath(job, path);
        FileOutputFormat.setOutputPath(job, new Path("file:///" + dir + "/src/main/resources/graphMade"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void generateGraphVertices() throws IOException {
        PrintWriter writer = new PrintWriter(FILE_NAME, "UTF-8");
        for (int i = 0; i < TOTAL_VERTICES * TOTAL_VERTICES; i++) {
            writer.println(i);
        }
        writer.close();

        new File(FILE_NAME).deleteOnExit();
    }
}
