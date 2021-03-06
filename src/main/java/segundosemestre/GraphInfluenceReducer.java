package segundosemestre;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class GraphInfluenceReducer extends Reducer<IntWritable, NodeInfluence, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<NodeInfluence> values, Context context) throws IOException, InterruptedException {

        Map<Integer, Double> state = new HashMap<>();
        String out = ",";
        for (NodeInfluence nodeInfluence : values) {
            Double influenceBy = state.get(nodeInfluence.getInfluencedBy()) == null ? 0.0 : state.get(nodeInfluence.getInfluencedBy());
            influenceBy += nodeInfluence.getInfluence();
            state.put(nodeInfluence.getInfluencedBy(), influenceBy);
        }

        out += state.entrySet().stream().map(entry -> entry.getKey() + "->" + entry.getValue()).collect(Collectors.joining(", "));

        context.write(key, new Text(out));
    }
}
