import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by asafchelouche on 6/6/16.
 */

public class WritableLongPair implements Writable {

    private long l1;
    private long l2;

    public WritableLongPair() {
        l1 = 0;
        l2 = 0;
    }

    public WritableLongPair(long l1, long l2) {
        this.l1 = l1;
        this.l2 = l2;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(l1);
        dataOutput.writeLong(l2);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        l1 = dataInput.readLong();
        l2 = dataInput.readLong();
    }
}
