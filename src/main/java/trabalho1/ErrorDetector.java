package trabalho1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Luciano P. Sabenca (luciano.sabenca@movile.com)
 *         Creation Date: 26/03/16
 */
public class ErrorDetector {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = value.toString().split(":");
            String[] str2 = str[1].split(",");

            if (str2.length > 1) {
                context.write(new Text(str[0]), new Text(Stream.of(str2).collect(Collectors.joining(","))));
            }

        }
    }

    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String rightValue = StreamSupport.stream(Spliterators.spliteratorUnknownSize(values.iterator(), Spliterator.ORDERED), false).map(text -> {
                Stream<String> strings = Stream.of(text.toString().split(","));
                Map<Integer, String> myMap = strings.map(s -> s.split("-"))
                                                    .collect(Collectors.toMap(strings1 -> Integer.parseInt(strings1[1]), strings1 -> strings1[0]));
                Optional<Map.Entry<Integer, String>> oldest = myMap.entrySet().stream()
                                                                   .collect(Collectors.minBy((o1, o2) -> o1.getKey().compareTo(o2.getKey())));
                return oldest.get().getValue();

            }).collect(Collectors.joining(""));

            context.write(key, new Text(rightValue));
        }

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
//        conf.set("fs.default.name", "hdfs://localhost:9000");
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(ReverseIndex.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String dir = System.getProperty("user.dir");

        // Rodando localmente sem HDFS/YARN
        Path path = new Path("file:///" + dir + "/src/main/resources/reverse_index/part-r-00000");
        FileInputFormat.addInputPath(job, path);
        FileOutputFormat.setOutputPath(job, new Path("file:///" + dir + "/src/main/resources/error"));

        //        FileInputFormat.addInputPath(job, new Path("/input_b"));
        //        FileOutputFormat.setOutputPath(job, new Path("/reverse_index"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
