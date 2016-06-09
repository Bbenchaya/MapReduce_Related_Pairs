/**
 * Created by asafchelouche on 6/6/16.
 */

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Phase2 {

    private static final int ASCII_OFFSET = 97;

    public static class Mapper2
            extends Mapper<Object, Text, Text, LongWritable>{

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] valueAsStrings = value.toString().split("\t");
            String actualKey = valueAsStrings[0];
            LongWritable actualValue = new LongWritable(Long.parseLong(valueAsStrings[1]));
            String[] components = actualKey.toString().split("[$]");
            if (!components[1].equals("*")) {
                context.write(new Text(components[1] + "$" + components[0]), actualValue);
                context.write(new Text(components[1] + "$*"), actualValue);
            }
            context.write(new Text(actualKey), actualValue);
        }
    }

    public static class Partitioner2 extends Partitioner<Text, LongWritable> {

        @Override
        public int getPartition(Text text, LongWritable longWritable, int i) {
            return (int)(text.toString().charAt(0)) - ASCII_OFFSET;
        }
    }

    public static class Reducer2
            extends Reducer<Text, LongWritable,Text, WritableLongPair> {

            private Text currentKey;
            private long sum;

        @Override
        public void setup(Context context) {
            currentKey = new Text();
            currentKey.set("");
            sum = 0l;
        }

        private String textContent(String w1, String w2) {
            if (w2.equals("*"))
                return w1 + "$*";
            if (w1.compareTo(w2) < 0)
                return w1 + "$" + w2;
            else
                return w2 + "$" + w1;
        }

        public void reduce(Text key, Iterable<LongWritable> counts,
                           Context context
        ) throws IOException, InterruptedException {
            long sumPair = 0l;
            String[] components = key.toString().split("[$]");
            for (LongWritable count : counts) {
                if (currentKey.equals(components[0])) {
                    if (components[1].equals("*"))
                        sum += count.get();
                    else
                        sumPair += count.get();
                }
                else {
                    sum = count.get();
                    currentKey.set(components[0]);
                }
            }
            if (!components[1].equals("*"))
                context.write(new Text(textContent(components[0], components[1])), new WritableLongPair(sumPair, sum));
        }
    }

    public static class Comparator2 extends WritableComparator {

        @Override
        public int compare(WritableComparable o1, WritableComparable o2) {
            String[] components1 = o1.toString().split("[$]");
            String[] components2 = o2.toString().split("[$]");
            if (components1[1].equals("*") && components2[1].equals("*"))
                return components1[0].compareTo(components2[0]);
            if (components1[1].equals("*")) {
                if (components1[0].equals(components2[0]))
                    return -1;
                else
                    return components1[0].compareTo(components2[0]);
            }
            if (components2[1].equals("*")) {
                if (components1[0].equals(components2[0]))
                    return 1;
                else
                    return components1[0].compareTo(components2[0]);
            }
            return components1[0].compareTo(components2[0]);
        }

    }

}
