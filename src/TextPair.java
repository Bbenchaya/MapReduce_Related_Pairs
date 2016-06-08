import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by asafchelouche on 6/6/16.
 */

public class TextPair extends WritableComparablePair<Text,Text> {

    @Override
    public int compareTo(WritableComparablePair o) {
        Text o1 = (Text)o.getK1();
        Text o2 = (Text)o.getK2();
        if (k1.compareTo(o1) == 0) {
            if (k2.toString().equals("*")) {
                if (o2.toString().equals("*"))
                    return 0;
                else
                    return -1;
            }
            else
            if (o2.toString().equals("*"))
                return 1;
            else
                return k2.compareTo(o2);
        }
        else
            return k1.compareTo(o1);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        k1.write(dataOutput);
        k2.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        k1 = new Text();
        k1.readFields(dataInput);
        k2 = new Text();
        k2.readFields(dataInput);
    }

    public void setK1(String s) {
        k1 = new Text(s);
    }

    public void setK2(String s) {
        k2 = new Text(s);
    }

    @Override
    public String toString(){
        return "" + k1 + " " + k2 ;
    }
}
