package trabalho1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ReverseIndex {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = value.toString().split(",");
            IntWritable id = new IntWritable(Integer.parseInt(str[0]));
            MapWritable mapWritable = new MapWritable();
            mapWritable.put(new Text("occur"), new Text(str[2]));
            mapWritable.put(new Text("hist_id"), id);
            Text value2 = new Text(str[1]);
            context.write(value2, mapWritable);
        }
    }

    public static class IndexReducer extends Reducer<Text, MapWritable, Text, Text> {

        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

            String valuesDiff = StreamSupport.stream(Spliterators.spliteratorUnknownSize(values.iterator(), Spliterator.ORDERED), false)
                                             .map(mapWritable -> mapWritable.get(new Text("occur")).toString() + "-" + mapWritable.get(new Text("hist_id"))
                                                                                                                                  .toString())
                                             .collect(Collectors.joining(","));

            Text text = new Text(valuesDiff);
            context.write(new Text(key.toString().replaceAll(" ", "-")), text);
        }

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        //        conf.set("fs.default.name", "hdfs://localhost:9000");
        conf.set("mapreduce.output.textoutputformat.separator", ":");
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(ReverseIndex.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String dir = System.getProperty("user.dir");

        Path path = new Path("file:///" + dir + "/src/main/resources/historico_sample_data.csv");
        FileInputFormat.addInputPath(job, path);
        FileOutputFormat.setOutputPath(job, new Path("file:///" + dir + "/src/main/resources/reverse_index"));
        //        FileInputFormat.addInputPath(job, new Path("/input_b"));
        //        FileOutputFormat.setOutputPath(job, new Path("/reverse_index"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}