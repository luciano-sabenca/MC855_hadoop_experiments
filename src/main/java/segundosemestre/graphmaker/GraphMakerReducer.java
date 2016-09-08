package segundosemestre.graphmaker;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import segundosemestre.Node;
import segundosemestre.NodeInfluence;

import java.io.IOException;
import java.util.LinkedHashMap;

class GraphMakerReducer extends Reducer<IntWritable, NodeInfluence, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<NodeInfluence> values, Context context) throws IOException, InterruptedException {
        Node node = new Node(key.get());
        node.setAdjacency(new LinkedHashMap<>());
        values.forEach(nodeInfluence -> node.addAdjacency(new Node(nodeInfluence), nodeInfluence.getInfluence()));
        System.out.println(key.get() + " " + node.getAdjacency().keySet().size());
        node.normalize();

        final String[] out = {""};
        node.getAdjacency().forEach((influenced, influence) -> out[0] += ", " + influenced.getId() + "->" + influence);
        context.write(key, new Text(out[0]));
    }
}
