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
            extends Mapper<Object, Text, Text, LongWritable>{

        static enum CountersEnum {NUM_OF_WORDS_IN_CORPUS, NUM_OF_PAIRS_PHASE_ONE}
        private final static LongWritable one = new LongWritable(1);
        private Text text;
        private boolean caseSensitive;
        private Configuration conf;
        private Map<String, Boolean> stopwords;
        private final String REGEX = "[^a-zA-Z ]+";
        private Counter counter;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {

            text = new Text();
            stopwords = new HashMap<>();

            // populate the stop-words hashmap
            File file = new File("stopwords.txt");
            FileReader fr = new FileReader(file);
            Scanner sc = new Scanner(fr);
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                line = line.replaceAll(REGEX, "");
                stopwords.put(line, true);
            }
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
            writeToContext(context, parts[0], parts[1], parts[2]);
//            System.out.println("\n");
        }

        private void writeToContext(Context context, String line, String year, String occurrences) throws IOException, InterruptedException {
//            System.out.println("length: " + line.split(" ").length);
            if (year.compareTo("1900") < 0)
                return;
            String[] components = line.split(" ");
            if (components.length == 1)
                return;
            int middle = 0;
            switch (components.length) {
                case 2:

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
            int index = 0;
            if (stopwords.get(components[middle]) != null) { // middle word is a stopword
                for (String component : components) { // emit pairs
                    if (middle != index && stopwords.get(component) == null) {
                        text.set(year + "$" + component + "$*");
                        context.write(text, one); // write the second word only
                        counter.increment(1);
                    }
                    index++;
                }
                return;
            }
            year = year.substring(0, 3) + "0"; // lower year's resolution to decade
            String major = components[middle];
            for (String component : components) { // emit pairs
                if (middle != index && stopwords.get(component) == null) {
                    text.set(year + "$" + major + "$" + component);
                    context.write(text, one); // write the pair of words
                    text.set(year + "$" + component + "$*");
                    context.write(text, one); // write the second word only
                    counter.increment(1);
                }
                index++;
            }
            text.set(year + "$" + major + "$*");
            context.write(text, one); // write the middle word
            counter.increment(1);
        }
    }

    public static class Partitioner1 extends Partitioner<Text, LongWritable> {

        @Override
        public int getPartition(Text text, LongWritable longWritable, int i) {
            int year = Integer.parseInt(text.toString().split("[$]")[0]);
            year -= 1900;
            return year / 10; // 1900-1909 go to reducer 0, 1910-1919 go to reducer 1 and so on...
        }
    }

    public static class Reducer1
            extends Reducer<Text, LongWritable, Text, LongWritable> {

        public void reduce(Text key, Iterable<LongWritable> counts,
                           Context context
        ) throws IOException, InterruptedException {

            String[] components = key.toString().split("[$]");
            long sum = 0l;

            for (LongWritable count : counts)
                sum += count.get();
            if (components.length == 2)
                // If a combiner went into action, the year was removed from the key, so the split yielded a different
                // String array.
                context.write(new Text(components[0] + "$" + components[1]), new LongWritable(sum));
            else
                context.write(new Text(components[1] + "$" + components[2]), new LongWritable(sum));
        }
    }

}
