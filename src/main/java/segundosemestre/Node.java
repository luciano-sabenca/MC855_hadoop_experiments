package segundosemestre;

import org.apache.hadoop.io.WritableComparable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Node implements Serializable, WritableComparable<Node> {

    private Integer id;

    private LinkedHashMap<Integer, Double> adjacency = new LinkedHashMap<>();
    private Map<Integer, Double> state = new HashMap<>();

    public Node(){

    }

    public Node(Integer id) {
        this.id = id;
    }

    public Node(NodeInfluence nodeInfluence) {
        this(nodeInfluence.getInfluenced());
    }

    public Map<Integer, Double> getState() {
        return state;
    }

    public void setState(Map<Integer, Double> state) {
        this.state = state;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public LinkedHashMap<Integer, Double> getAdjacency() {
        return adjacency;
    }

    public void setAdjacency(LinkedHashMap<Integer, Double> adjacency) {
        this.adjacency = adjacency;
    }

    public Node addAdjacency(Node node, Double influence) {
        if (adjacency == null) {
            adjacency = new LinkedHashMap<>();
        }

        if (!this.equals(node)) {
            adjacency.put(node.getId(), influence);
        }

        return this;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Node))
            return false;

        Node node = (Node) o;

        return id != null ? id.equals(node.id) : node.id == null;

    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    @Override
    public int compareTo(Node o) {
        return this.equals(o) ? 0 : this.id - o.getId();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        ByteArrayOutputStream outB = new ByteArrayOutputStream();
        ObjectOutputStream objOut = new ObjectOutputStream(outB);
        objOut.writeObject(this.adjacency);
        objOut.close();
        out.writeInt(outB.size());
        out.write(outB.toByteArray());
        outB = new ByteArrayOutputStream();
        objOut = new ObjectOutputStream(outB);
        objOut.writeObject(this.state);
        objOut.close();
        out.writeInt(outB.size());
        out.write(outB.toByteArray());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        int sizeOfMap = in.readInt();
        byte[] bytes = new byte[sizeOfMap];
        for (int i = 0; i < sizeOfMap; i++)
            bytes[i] = in.readByte();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        try {
            this.adjacency = (LinkedHashMap<Integer, Double>) objectInputStream.readObject();
        } catch (ClassNotFoundException e) {

        }
        sizeOfMap = in.readInt();
        bytes = new byte[sizeOfMap];
        for (int i = 0; i < sizeOfMap; i++)
            bytes[i] = in.readByte();
        byteArrayInputStream = new ByteArrayInputStream(bytes);
        objectInputStream = new ObjectInputStream(byteArrayInputStream);
        try {
            this.state = (Map<Integer, Double>) objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    public void normalize() {
        final double[] totalInfluence = {0.0d};
        getAdjacency().forEach((node, influence) -> totalInfluence[0] += influence);

        getAdjacency().forEach((node, influence) -> getAdjacency().put(node, influence / totalInfluence[0]));
    }
}
