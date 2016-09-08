package segundosemestre;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class MainAnalysis {
  public static void main(String[] args) throws Exception {
    Logger.getRootLogger().addAppender(new ConsoleAppender(new PatternLayout()));
    
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "hdfs://localhost:9000");
    Job job = Job.getInstance(conf, "GraphInfluence");
    
    job.setJarByClass(MainAnalysis.class);
    job.setMapperClass(GraphAnalysisMapper.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setReducerClass(GraphAnalysisReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    
    String dir = System.getProperty("user.dir");
    Path inputPath = new Path("file:///" + dir + "/src/main/resources/output/part-r-00000");
    FileInputFormat.addInputPath(job, inputPath);
    
    Path outputPath = new Path("file:///" + dir + "/src/main/resources/output/analysis");
    FileOutputFormat.setOutputPath(job, outputPath); 
    
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
