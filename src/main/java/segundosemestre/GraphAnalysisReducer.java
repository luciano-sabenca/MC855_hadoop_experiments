package segundosemestre;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GraphAnalysisReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, Text> {
  
  private static int nodeId = 0;
  private static double maxNodeInfluence = 0.0;
  
  @Override
  protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
    Double nodeInfluence = 0.0;
    for(DoubleWritable influenceValue : values) {
      nodeInfluence = nodeInfluence + influenceValue.get();
    }
    
    if(nodeInfluence > maxNodeInfluence) {
      maxNodeInfluence = nodeInfluence;
      nodeId = key.get();
    }
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
      context.write(new IntWritable(nodeId), new Text(Double.toString(maxNodeInfluence)));
  }

}
