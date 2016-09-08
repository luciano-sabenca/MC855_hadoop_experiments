package segundosemestre.graphmaker;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import segundosemestre.NodeInfluence;

import java.io.IOException;

import static segundosemestre.graphmaker.MainGraphMaker.TOTAL_VERTICES;

class GraphMaker extends Mapper<Object, Text, IntWritable, NodeInfluence> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        NodeInfluence node = new NodeInfluence(getRandomInt(), getRandomInt(), Math.random());
        context.write(new IntWritable(node.getInfluencedBy()), node);
    }

    private int getRandomInt() {
        return (int) (Math.random() * TOTAL_VERTICES);
    }
}
