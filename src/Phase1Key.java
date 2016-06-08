import org.apache.hadoop.io.LongWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by asafchelouche on 7/6/16.
 */
public class Phase1Key extends WritableComparablePair<LongWritable, TextPair> {

    @Override
    public int compareTo(WritableComparablePair o) {
        int k1comp = (int)(k1.get() - ((LongWritable) o.getK1()).get());
        if (k1comp == 0)
            return k2.compareTo((TextPair) o.getK2());
        else
            return k1comp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        k1.write(dataOutput);
        k2.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        k1 = new LongWritable(dataInput.readLong());
        k2 = new TextPair();
        k2.readFields(dataInput);
    }

}
