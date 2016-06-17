/**
 * Created by asafchelouche on 6/6/16.
 */

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Phase1 {

    private static final String NUM_OF_PAIRS_SENT_TO_REDUCERS_FILENAME = "Phase 1 - num of pairs sent to reducers.txt";
    public static final String WORDS_PER_DECADE_FILENAME = "wordsPerDecade.txt";
    private static final int NUM_OF_DECADES = 12;

    public static class Mapper1
            extends Mapper<Object, Text, Text, LongWritable>{

        enum CountersEnum {DECADE_0, DECADE_1, DECADE_2, DECADE_3, DECADE_4, DECADE_5, DECADE_6, DECADE_7, DECADE_8, DECADE_9, DECADE_10, DECADE_11}
        private Text text;
        private Map<String, Boolean> stopwords;
        private final String REGEX = "[^a-zA-Z ]+";
        private Counter[] counters;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {

            text = new Text();
            stopwords = new HashMap<>();

            // populate the stop-words hashmap

            AmazonS3 s3 = new AmazonS3Client();
            Region usEast1 = Region.getRegion(Regions.US_EAST_1);
            s3.setRegion(usEast1);

            System.out.print("Downloading corpus description file from S3... ");
            S3Object object = s3.getObject(new GetObjectRequest("dsps162assignment2benasaf/resource", "stopwords.txt"));
            System.out.println("Done.");
            Scanner sc = new Scanner(new InputStreamReader(object.getObjectContent()));
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                line = line.replaceAll(REGEX, "");
                stopwords.put(line, true);
            }
            sc.close();

            counters = new Counter[NUM_OF_DECADES];
            counters[0] = context.getCounter(CountersEnum.class.getName(), CountersEnum.DECADE_0.toString());
            counters[1] = context.getCounter(CountersEnum.class.getName(), CountersEnum.DECADE_1.toString());
            counters[2] = context.getCounter(CountersEnum.class.getName(), CountersEnum.DECADE_2.toString());
            counters[3] = context.getCounter(CountersEnum.class.getName(), CountersEnum.DECADE_3.toString());            counters[0] = context.getCounter(CountersEnum.class.getName(), CountersEnum.DECADE_0.toString());
            counters[4] = context.getCounter(CountersEnum.class.getName(), CountersEnum.DECADE_4.toString());
            counters[5] = context.getCounter(CountersEnum.class.getName(), CountersEnum.DECADE_5.toString());
            counters[6] = context.getCounter(CountersEnum.class.getName(), CountersEnum.DECADE_6.toString());            counters[0] = context.getCounter(CountersEnum.class.getName(), CountersEnum.DECADE_0.toString());
            counters[7] = context.getCounter(CountersEnum.class.getName(), CountersEnum.DECADE_7.toString());
            counters[8] = context.getCounter(CountersEnum.class.getName(), CountersEnum.DECADE_8.toString());
            counters[9] = context.getCounter(CountersEnum.class.getName(), CountersEnum.DECADE_9.toString());
            counters[10] = context.getCounter(CountersEnum.class.getName(), CountersEnum.DECADE_10.toString());
            counters[11] = context.getCounter(CountersEnum.class.getName(), CountersEnum.DECADE_11.toString());
            System.out.println("Phase 1: finished setup");
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            String[] parts = line.split("\t");
            parts[0] = parts[0].replaceAll(REGEX, "");
            parts[0] = parts[0].replaceAll(" +", " ");
            if (parts[0].equals(" ") || parts[0].isEmpty())
                return;
            if (parts[0].charAt(0) == ' ')
                parts[0] = parts[0].substring(1);
            writeToContext(context, parts[0], parts[1], parts[2]);
        }

        private void writeToContext(Context context, String line, String year, String occurrences) throws IOException, InterruptedException {
            if (year.compareTo("1900") < 0)
                return;
            String[] components = line.split(" ");
            if (components.length == 1)
                return;
            int middle = 0;
            long numOfOccurences = Long.parseLong(occurrences);
            int countersIndex = (Integer.parseInt(year) - 1900) / 10;
            switch (components.length) {
                case 2:
                    if (stopwords.get(components[0]) == null) {
                        text.set(year + "$" + components[0] + "$*");
                        context.write(text, new LongWritable(numOfOccurences)); // write the second word only
                        counters[countersIndex].increment(numOfOccurences);
                        if (stopwords.get(components[1]) == null) {
                            text.set(year + "$" + components[1] + "$*");
                            context.write(text, new LongWritable(numOfOccurences)); // write the second word only
                            counters[countersIndex].increment(numOfOccurences);
                            text.set(year + "$" + components[0] + "$" + components[1]);
                            context.write(text, new LongWritable(numOfOccurences)); // write the second word only
                        }
                    }
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
                        context.write(text, new LongWritable(numOfOccurences)); // write the second word only
                        counters[countersIndex].increment(1);
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
                    context.write(text, new LongWritable(numOfOccurences)); // write the pair of words
                    text.set(year + "$" + component + "$*");
                    context.write(text, new LongWritable(numOfOccurences)); // write the second word only
                    counters[countersIndex].increment(numOfOccurences);
                }
                index++;
            }
            text.set(year + "$" + major + "$*");
            context.write(text, new LongWritable(numOfOccurences)); // write the middle word
            counters[countersIndex].increment(numOfOccurences);
        }

        @Override
        public void cleanup(Context context) {
            for (int i = 0; i < NUM_OF_DECADES; i++)
                System.out.println("Num of words in decade " + i + ": " + counters[i].getValue());
        }

    }

    public static class Reducer1
            extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> counts,
                           Context context
        ) throws IOException, InterruptedException {

            long sum = 0l;
            for (LongWritable count : counts)
                sum += count.get();
            context.write(key, new LongWritable(sum));
        }

    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2)
            throw new IOException("Phase 1: supply 2 arguments");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Phase 1");
        job.setJarByClass(Phase1.class);
        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.out.println("Phase 1 - input path: " + args[0] + ", output path: " + args[1]);
        if (job.waitForCompletion(true))
            System.out.println("Phase 1: job completed successfully");
        else
            System.out.println("Phase 1: job completed unsuccessfully");
        Counter counter = job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS");
        System.out.println("Num of pairs sent to reducers in phase 1: " + counter.getValue());
        AmazonS3 s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);
        try {
            System.out.print("Uploading the corpus description file to S3... ");
            File file = new File(WORDS_PER_DECADE_FILENAME);
            FileWriter fw = new FileWriter(file);
            for (int i = 0; i < NUM_OF_DECADES; i++)
                fw.write(Long.toString(job.getCounters().findCounter("Phase1$Mapper1$CountersEnum", "DECADE_" + i).getValue()) + "\n");
            fw.flush();
            fw.close();
            s3.putObject(new PutObjectRequest("dsps162assignment2benasaf/results/", WORDS_PER_DECADE_FILENAME, file));
            System.out.println("Done.");
            System.out.print("Uploading Phase 1 description file to S3... ");
            file = new File(NUM_OF_PAIRS_SENT_TO_REDUCERS_FILENAME);
            fw = new FileWriter(file);
            fw.write(Long.toString(counter.getValue()) + "\n");
            fw.flush();
            fw.close();
            s3.putObject(new PutObjectRequest("dsps162assignment2benasaf/results/", NUM_OF_PAIRS_SENT_TO_REDUCERS_FILENAME, file));
            System.out.println("Done.");
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

}
