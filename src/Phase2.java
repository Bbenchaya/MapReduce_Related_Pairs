/**
 * Created by asafchelouche on 6/6/16.
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Phase2 {

    public static class Mapper2
            extends Mapper<Object, Text, Text, IntWritable>{



        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {

            // read the stop words file from S3, and convert it to a hash table of stop words

        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

        }
    }

    public static class reducer2
            extends Reducer<Text,IntWritable,Text,IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

        }
    }

}
