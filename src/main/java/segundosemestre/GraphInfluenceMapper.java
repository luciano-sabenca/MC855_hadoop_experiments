package segundosemestre;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Luciano P. Sabenca (luciano.sabenca@movile.com)
 *         Creation Date: 04/09/16
 */
public class GraphInfluenceMapper extends Mapper<IntWritable, Node, Node, NodeInfluence> {

    @Override
    protected void map(IntWritable key, Node value, Context context) throws IOException, InterruptedException {

        value.getAdjacency().forEach((k, v) -> {
            try {
                NodeInfluence nodeInfluence = new NodeInfluence(k.getId(), value.getId(), v);
                context.write(k, nodeInfluence);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

}
