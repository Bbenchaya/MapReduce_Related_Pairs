/**
 * Created by asafchelouche on 6/6/16.
 */

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

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
            String[] components = value.toString().split("[$]");
            int year = Integer.parseInt(components[0]);
            year -= 1900;
            return year / 10;
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
        public int compare(byte[] bytes1, int s1, int l1, byte[] bytes2, int s2, int l2) {
            System.out.println("raw");
            double d1 = readDouble(bytes1, s1);
            double d2 = readDouble(bytes2, l2);
            // invert the comparison outcome in order to sort the output in descending order of PMI
            return (int) (d2 - d1);
        }
    }

}
