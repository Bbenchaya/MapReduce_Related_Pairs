import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by asafchelouche on 6/6/16.
 */

public class TextPair implements WritableComparable<TextPair> {

    private Text t1;
    private Text t2;

    @Override
    public int compareTo(TextPair o) {
        Text o1 = o.t1;
        Text o2 = o.t2;
        if (t1.compareTo(o1) == 0) {
            if (t2.toString().equals("*")) {
                if (o2.toString().equals("*"))
                    return 0;
                else
                    return -1;
            }
            else
                if (o2.toString().equals("*"))
                    return 1;
                else
                    return t2.compareTo(o2);
        }
        else
            return t1.compareTo(o1);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        t1.write(dataOutput);
        t2.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        t1 = new Text(dataInput.readUTF());
        t2 = new Text(dataInput.readUTF());
    }

}
