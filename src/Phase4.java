/**
 * Created by asafchelouche on 6/6/16.
 */

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;

public class Phase4 {

    private static final int ASCII_OFFSET = 97;

    public static class Mapper4
            extends Mapper<DoubleWritable, Text, DoubleWritable, Text>{


        @Override
        public void map(DoubleWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class Partitioner4 extends Partitioner<DoubleWritable, Text> {

        @Override
        public int getPartition(DoubleWritable key, Text value, int i) {
            return (int)(value.toString().charAt(0)) - ASCII_OFFSET;
        }
    }

    public static class Reducer4
            extends Reducer<DoubleWritable, Text, Text, Text> {

        public void reduce(DoubleWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (Text value : values)
                context.write(value, new Text(key.toString()));
        }
    }

    public static class Comparator4 extends WritableComparator {

        @Override
        public int compare(WritableComparable o1, WritableComparable o2) {
            return -o1.compareTo(o2);
        }

    }

}
