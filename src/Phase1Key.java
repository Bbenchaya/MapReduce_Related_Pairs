import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by asafchelouche on 7/6/16.
 */
public class Phase1Key implements WritableComparable<Phase1Key> {

    private int year;
    private TextPair textPair;

    @Override
    public int compareTo(Phase1Key o) {
        return year - o.year;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        textPair.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readInt();
        // TODO read textPair
    }

}
