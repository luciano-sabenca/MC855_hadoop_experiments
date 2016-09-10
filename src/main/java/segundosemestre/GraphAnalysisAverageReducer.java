package segundosemestre;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GraphAnalysisAverageReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, Text> {
  
  @Override
  protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
    Double nodeInfluence = 0.0;
    Double average = 0.0;
    Integer qtyInfluences = 0;
    
    for(DoubleWritable influenceValue : values) {
      nodeInfluence = nodeInfluence + influenceValue.get();
      qtyInfluences++;
    }
    
    average = qtyInfluences != 0 ? nodeInfluence / qtyInfluences : 0.0;
    
    context.write(key, new Text(average.toString()));
  }

}
