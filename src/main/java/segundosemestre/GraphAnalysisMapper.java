package segundosemestre;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GraphAnalysisMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
  
  @Override
  protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String[] nodeInfluences = value.toString().split(",");
   
    for (int i = 1; i < nodeInfluences.length; i++) { //the 0 position contains the node id

      String influence[] = nodeInfluences[i].split("->");

      context.write(new IntWritable(Integer.parseInt(influence[0].trim())), new DoubleWritable(new Double(influence[1].trim())));
    }
  }

}
