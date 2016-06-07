/**
 * Created by asafchelouche on 6/6/16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Phase1 {

    public static class Mapper1
            extends Mapper<Object, Text, TextPair, IntWritable>{

        static enum CountersEnum {NUM_OF_WORDS_IN_CORPUS, NUM_OF_PAIRS_PHASE_ONE}
        private final static IntWritable one = new IntWritable(1);
        private TextPair key = new TextPair();
        private boolean caseSensitive;
        private Configuration conf;
        private Map<String, Boolean> stopwords = new HashMap<>();
        private final String REGEX = "[^a-zA-Z ]+";

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {

            // populate the stop-words hashmap
            File file = new File("stopwords.txt");
            FileReader fr = new FileReader(file);
            Scanner sc = new Scanner(fr);
            while (sc.hasNextLine())
                stopwords.put(sc.nextLine(), true);
            sc.close();

            // construct patterns hashset
            conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("phase1.case.sensitive", true);
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = (caseSensitive) ?
                    value.toString() : value.toString().toLowerCase();

            String[] parts = line.split("\t");
            for (String part : parts)
                System.out.println(part);
            parts[0] = parts[0].replaceAll(REGEX, "");
            parts[0] = parts[0].replaceAll(" +", " ");
            if (parts[0].equals(" ") || parts[0].isEmpty())
                return;
            if (parts[0].charAt(0) == ' ')
                parts[0] = parts[0].substring(1);
            System.out.println("after:" + parts[0]);
            // context, text, year, no. of occurrences
            writeToContext(context, parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
            System.out.println("\n");
        }

        private void writeToContext(Context context, String line, int year, int num) throws IOException, InterruptedException {
            System.out.println("length: " + line.split(" ").length);
            if (year < 1900)
                return;
            String[] components = line.split(" ");
            if (components.length == 1)
                return;
            int middle = 0;
            switch (components.length) {
                case 2:
                    // write
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
            if (stopwords.get(components[middle])) // middle word is a stopword
                return;
            int index = 0;
            for (String component : components) { // emit pairs
                if (middle != index && stopwords.get(component) == null) {
                    context.write(key, one);
                    Counter counter = context.getCounter(CountersEnum.class.getName(),
                            CountersEnum.NUM_OF_WORDS_IN_CORPUS.toString());
                    counter.increment(1);
                }
                middle++;
            }
        }
    }

    public static class Reducer1
            extends Reducer<Text,IntWritable,Text,IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

        }
    }

}
