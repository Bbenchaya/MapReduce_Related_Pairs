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
            extends Mapper<Text, LongWritable, Text, LongWritable>{

        @Override
        public void map(Text key, LongWritable value, Context context
        ) throws IOException, InterruptedException {
            String[] components = key.toString().split("[$]");
            if (!components[2].equals("*")) {
                context.write(new Text(components[0] + "$" + components[2] + "$" + components[1]), value);
                context.write(new Text(components[0] + "$" + components[2] + "$*"), value);
            }
            context.write(key, value);
        }
    }

    public static class Partitioner2 extends Partitioner<Text, LongWritable> {

        @Override
        public int getPartition(Text key, LongWritable value, int i) {
            String[] components = key.toString().split("[$]");
            return (int)(components[1].charAt(0)) - ASCII_OFFSET;
        }
    }

    public static class Reducer2
            extends Reducer<Text, LongWritable, Text, WritableLongPair> {

            private String currentKey;
            private long sum;

        @Override
        public void setup(Context context) {
            currentKey = "";
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
                if (currentKey.equals(components[1])) {
                    if (components[2].equals("*"))
                        sum += count.get();
                    else
                        sumPair += count.get();
                }
                else {
                    sum = count.get();
                    currentKey = components[1];
                }
            }
            if (!components[2].equals("*"))
                context.write(new Text(components[0] + "$" + textContent(components[1], components[2])), new WritableLongPair(sumPair, sum));
        }
    }

}
