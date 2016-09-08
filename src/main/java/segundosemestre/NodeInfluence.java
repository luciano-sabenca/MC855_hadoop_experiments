package segundosemestre;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class NodeInfluence implements Serializable, WritableComparable<NodeInfluence> {

    private Integer influencedBy;
    private Integer influenced;

    private Double influence;

    public NodeInfluence(){

    }

    public NodeInfluence(Integer influenced, Integer influencedBy, Double influence) {
        this.influenced = influenced;
        this.influencedBy = influencedBy;
        this.influence = influence;
    }

    public Integer getInfluencedBy() {
        return influencedBy;
    }

    public void setInfluencedBy(Integer influencedBy) {
        this.influencedBy = influencedBy;
    }

    public Double getInfluence() {
        return influence;
    }

    public void setInfluence(Double influence) {
        this.influence = influence;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof NodeInfluence))
            return false;

        NodeInfluence that = (NodeInfluence) o;

        if (influencedBy != null ? !influencedBy.equals(that.influencedBy) : that.influencedBy != null)
            return false;
        if (influenced != null ? !influenced.equals(that.influenced) : that.influenced != null)
            return false;
        return influence != null ? influence.equals(that.influence) : that.influence == null;

    }

    @Override
    public int hashCode() {
        int result = influencedBy != null ? influencedBy.hashCode() : 0;
        result = 31 * result + (influenced != null ? influenced.hashCode() : 0);
        result = 31 * result + (influence != null ? influence.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(NodeInfluence o) {
        return this.equals(o)? 0 : this.influenced - o.influenced;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(influenced);
        out.writeInt(influencedBy);

        out.writeDouble(this.influence);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.influenced = in.readInt();
        this.influencedBy = in.readInt();
        this.influence = in.readDouble();
    }

    public Integer getInfluenced() {
        return influenced;
    }

    @Override
    public String toString() {
        return influenced + "->" + influence;
    }
}
