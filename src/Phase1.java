/**
 * Created by asafchelouche on 6/6/16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Phase1 {

    public static class Mapper1
            extends Mapper<Object, Text, WritableComparablePair, LongWritable>{

        static enum CountersEnum {NUM_OF_WORDS_IN_CORPUS, NUM_OF_PAIRS_PHASE_ONE}
        private final static LongWritable one = new LongWritable(1);
        private WritableComparablePair key = new Phase1Key();
        private TextPair textPair = new TextPair();
        private boolean caseSensitive;
        private Configuration conf;
        private Map<String, Boolean> stopwords = new HashMap<>();
        private final String REGEX = "[^a-zA-Z ]+";
        private Counter counter;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {

            // populate the stop-words hashmap
            File file = new File("stopwords.txt");
            FileReader fr = new FileReader(file);
            Scanner sc = new Scanner(fr);
            while (sc.hasNextLine())
                stopwords.put(sc.nextLine().replaceAll(REGEX, ""), true);
            sc.close();

            // construct patterns hashset
            conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("phase1.case.sensitive", true);
            counter = context.getCounter(CountersEnum.class.getName(), CountersEnum.NUM_OF_WORDS_IN_CORPUS.toString());
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = (caseSensitive) ?
                    value.toString() : value.toString().toLowerCase();

            String[] parts = line.split("\t");
//            for (String part : parts)
//                System.out.println(part);
            parts[0] = parts[0].replaceAll(REGEX, "");
            parts[0] = parts[0].replaceAll(" +", " ");
            if (parts[0].equals(" ") || parts[0].isEmpty())
                return;
            if (parts[0].charAt(0) == ' ')
                parts[0] = parts[0].substring(1);
//            System.out.println("after:" + parts[0]);
            // context, text, year, no. of occurrences
            writeToContext(context, parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
//            System.out.println("\n");
        }

        private void writeToContext(Context context, String line, int year, int num) throws IOException, InterruptedException {
//            System.out.println("length: " + line.split(" ").length);
            if (year < 1900)
                return;
            String[] components = line.split(" ");
            if (components.length == 1)
                return;
            int middle = 0;
            switch (components.length) {
                case 2:
                    key.setK1(new LongWritable(year - (year % 10)));
                    textPair.setK1(components[0]);
                    textPair.setK2(components[1]);
                    key.setK2(textPair);
                    context.write(key, one);
                    textPair.setK2("*");
                    context.write(key, one);
                    counter.increment(1);
                    textPair.setK1(components[1]);
                    context.write(key, one);
                    counter.increment(1);
                    return;
                case 3:
                    middle = 1;
                    break;
                case 4:
                    middle = 1;
                    break;
                case 5:
                    middle = 2;
                    break;
            }
            if (stopwords.get(components[middle]) != null) // middle word is a stopword
                return;
            int index = 0;
            key.setK1(new LongWritable(year - (year % 10)));
            textPair.setK1(components[middle]);
            for (String component : components) { // emit pairs
                if (middle != index && stopwords.get(component) == null) {
                    textPair.setK1(components[middle]);
                    textPair.setK2(component);
                    key.setK2(textPair);
                    context.write(key, one);
                    counter.increment(1);
                    textPair.setK1(component);
                    textPair.setK2("*");
                    context.write(key, one);
                }
                index++;
            }
            textPair.setK1(components[middle]);
            textPair.setK2("*");
            key.setK2(textPair);
            context.write(key, one);

//            System.out.println("Written entry: " + key.getK1() + " " + key.getK2().toString());

            counter.increment(1);
        }
    }

    public static class Partitioner1 extends Partitioner<Phase1Key, LongWritable> {

        @Override
        public int getPartition(Phase1Key key, LongWritable value, int numOfPartitions) {
            long decade = ((LongWritable) key.getK1()).get();
            decade -= 1900;
            return (int)(decade / 10l);
        }
    }

    public static class Reducer1
            extends Reducer<Phase1Key, LongWritable, TextPair, LongWritable> {

        public void reduce(Phase1Key key, Iterable<LongWritable> counts,
                           Context context
        ) throws IOException, InterruptedException {

            TextPair textPair = key.getK2();
            long sum = 0l;

            for (LongWritable count : counts)
                sum += count.get();

            context.write(textPair, new LongWritable(sum));
        }
    }

}
