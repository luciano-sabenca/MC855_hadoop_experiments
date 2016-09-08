package segundosemestre;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedHashMap;

/**
 * @author Luciano P. Sabenca (luciano.sabenca@movile.com)
 *         Creation Date: 04/09/16
 */
public class GraphReader extends Mapper<Object, Text, IntWritable, Node> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] lineSplit = value.toString().split(",");

        Node node = new Node(Integer.parseInt(lineSplit[0].trim()));

        LinkedHashMap<Integer, Double> adjacency = new LinkedHashMap<>();
        for (int i = 1; i < lineSplit.length; i++) {

            String aux[] = lineSplit[i].split("->");

            adjacency.put(Integer.parseInt(aux[0].trim()), Double.parseDouble(aux[1].trim()));
        }

        node.setAdjacency(adjacency);

        context.write(new IntWritable(node.getId()), node);

    }
}
