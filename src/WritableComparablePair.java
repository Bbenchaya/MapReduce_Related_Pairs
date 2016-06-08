import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by asafchelouche on 8/6/16.
 */
public abstract class WritableComparablePair<K1, K2> implements WritableComparable<WritableComparablePair> {

    K1 k1;
    K2 k2;

    @Override
    public abstract int compareTo(WritableComparablePair o);

    @Override
    public abstract void write(DataOutput dataOutput) throws IOException;

    @Override
    public abstract void readFields(DataInput dataInput) throws IOException;

    public K1 getK1() {
        return k1;
    }

    public K2 getK2() {
        return k2;
    }

    public void setK1(K1 k1) {
        this.k1 = k1;
    }

    public void setK2(K2 k2) {
        this.k2 = k2;
    }

}
