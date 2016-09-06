package segundosemestre;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Luciano P. Sabenca (luciano.sabenca@movile.com)
 *         Creation Date: 04/09/16
 */
public class GraphInfluenceMapper extends Mapper<IntWritable, Node, IntWritable, NodeInfluence> {

    @Override
    protected void map(IntWritable key, Node value, Context context) throws IOException, InterruptedException {

        value.getAdjacency().forEach((k, v) -> {
            try {
                NodeInfluence nodeInfluence = new NodeInfluence(k, value.getId(), v);
                context.write(new IntWritable(k), nodeInfluence);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

}
