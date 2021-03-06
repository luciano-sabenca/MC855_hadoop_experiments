package segundosemestre;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * @author Luciano P. Sabenca (luciano.sabenca@movile.com)
 *         Creation Date: 04/09/16
 */
public class MainGraphInfluence {
    public static void main(String[] args) throws Exception {
        Logger.getRootLogger().addAppender(new ConsoleAppender(new PatternLayout()));

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://localhost:9000");
        Job job = Job.getInstance(conf, "GraphInfluence");

        ChainMapper.addMapper(job, GraphReader.class, Object.class, Text.class, IntWritable.class, Node.class, new Configuration(false));
        ChainMapper.addMapper(job, GraphInfluenceMapper.class, IntWritable.class, Node.class, IntWritable.class, NodeInfluence.class, new Configuration(false));

        job.setJarByClass(MainGraphInfluence.class);
        job.setReducerClass(GraphInfluenceReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        String dir = System.getProperty("user.dir");

        String operation = System.getProperty("operation") == null ? "" : System.getProperty("operation");
        switch (operation) {
        case "state":
            FileInputFormat.setInputPaths(job, "file:///" + dir + "/src/main/resources/graph.csv" + "," + "file:///" + dir + "/src/main/resources/output/part-r-00000");
            FileOutputFormat.setOutputPath(job, new Path("file:///" + dir + "/src/main/resources/output_state"));
            break;
        default:
            Path path = new Path("file:///" + dir + "/src/main/resources/graph.csv");
            FileOutputFormat.setOutputPath(job, new Path("file:///" + dir + "/src/main/resources/output"));
            FileInputFormat.addInputPath(job, path);
        }



        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
