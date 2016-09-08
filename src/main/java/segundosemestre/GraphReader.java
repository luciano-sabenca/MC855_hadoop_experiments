package segundosemestre;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedHashMap;

public class GraphReader extends Mapper<Object, Text, IntWritable, Node> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] lineSplit = value.toString().split(",");

        Node node = new Node(Integer.parseInt(lineSplit[0]));

        LinkedHashMap<Node, Double> adjacency = new LinkedHashMap<>();
        for (int i = 1; i < lineSplit.length; i++) {

            String aux[] = lineSplit[i].split("->");
            Node node1 = new Node(Integer.parseInt(aux[0].trim()));
            adjacency.put(node1, Double.parseDouble(aux[1].trim()));
        }

        node.setAdjacency(adjacency);

        context.write(new IntWritable(node.getId()), node);

    }
}
