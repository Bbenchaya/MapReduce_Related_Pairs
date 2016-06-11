/**
 * Created by asafchelouche on 6/6/16.
 */

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Phase3 {

    private static final int ASCII_OFFSET = 97;

    public static class Mapper3
            extends Mapper<Text, WritableLongPair, Text, WritableLongPair>{


        @Override
        public void map(Text key, WritableLongPair value, Context context
        ) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }


    public static class Partitioner3 extends Partitioner<Text, WritableLongPair> {

        @Override
        public int getPartition(Text key, WritableLongPair value, int i) {
            return (int)(key.toString().charAt(0)) - ASCII_OFFSET;
        }
    }

    public static class Reducer3
            extends Reducer<Text, WritableLongPair, DoubleWritable, Text> {

            private long numOfWordsInCorpus;

        @Override
        public void setup(Context context) {
            numOfWordsInCorpus = Long.parseLong(context.getConfiguration().get("NUM_OF_WORDS_IN_CORPUS"));
        }

        @Override
        public void reduce(Text key, Iterable<WritableLongPair> counts,
                           Context context
        ) throws IOException, InterruptedException {
            long pairCount = 0;
            long firstWordCount = 0;
            long secondWordCount = 0;
            WritableLongPair pair1 = null;
            WritableLongPair pair2 = null;
            for (WritableLongPair count : counts) {
                if (pair1 == null)
                    pair1 = count;
                else if (pair2 == null)
                    pair2 = count;
                else
                    throw new IOException("Phase 3: Reducer: more then 2 values for key: " + key.toString() + " value: " + count.toString());
            }
            if (pair2 == null) {
                /*
                If both words in the pair are actually the same word, then Mapper1 causes their count as a pair to
                double, and Mapper2 causes their single word count to triple.
                 */
                pairCount = pair1.getL1();
                firstWordCount = pairCount;
                pairCount /= 2;
                double PMI = Math.log(pairCount) + Math.log(2 * firstWordCount) + Math.log(numOfWordsInCorpus);
                context.write(new DoubleWritable(PMI), key);
            }
            else {
                pairCount = pair1.getL1();
                firstWordCount = pair1.getL2();
                secondWordCount = pair2.getL2();
                double PMI = Math.log(pairCount) + Math.log(firstWordCount) + Math.log(secondWordCount) + Math.log(numOfWordsInCorpus);
                context.write(new DoubleWritable(PMI), key);
            }

        }
    }

}
