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

    public static class Mapper1
            extends Mapper<Object, Text, Text, LongWritable>{

        static enum CountersEnum {NUM_OF_WORDS_IN_CORPUS}
        private final static LongWritable one = new LongWritable(1);
        private Text text;
        private Configuration conf;
        private Map<String, Boolean> stopwords;
        private final String REGEX = "[^a-zA-Z ]+";
        private Counter numOfWordsInCorpus;

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
            S3Object object = s3.getObject(new GetObjectRequest("dsps162assignment2benasaf/resource/", "stopwords.txt"));
            System.out.println("Done.");
            Scanner sc = new Scanner(new InputStreamReader(object.getObjectContent()));
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                line = line.replaceAll(REGEX, "");
                stopwords.put(line, true);
            }
            sc.close();

            // construct patterns hashset
            conf = context.getConfiguration();
            numOfWordsInCorpus = context.getCounter(CountersEnum.class.getName(), CountersEnum.NUM_OF_WORDS_IN_CORPUS.toString());
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
            switch (components.length) {
                case 2:
                    if (stopwords.get(components[0]) == null) {
                        text.set(year + "$" + components[0] + "$*");
                        context.write(text, new LongWritable(numOfOccurences)); // write the second word only
                        numOfWordsInCorpus.increment(numOfOccurences);
                        if (stopwords.get(components[1]) == null) {
                            text.set(year + "$" + components[1] + "$*");
                            context.write(text, new LongWritable(numOfOccurences)); // write the second word only
                            numOfWordsInCorpus.increment(numOfOccurences);
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
                        numOfWordsInCorpus.increment(1);
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
                    numOfWordsInCorpus.increment(numOfOccurences);
                }
                index++;
            }
            text.set(year + "$" + major + "$*");
            context.write(text, one); // write the middle word
            numOfWordsInCorpus.increment(numOfOccurences);
        }

        @Override
        public void cleanup(Context context) {
            System.out.println("Num of words in corpus: " + numOfWordsInCorpus.getValue());
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
        job.setMapperClass(Phase1.Mapper1.class);
        job.setReducerClass(Phase1.Reducer1.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path output = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, output);
        boolean result = job.waitForCompletion(true);
        Counter counter = job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS");
        System.out.println("Num of pairs sent to reducers in phase 1: " + counter.getValue());
        long numOfWordsInCorpus = job.getCounters().findCounter("Phase1$Mapper1$CountersEnum", "NUM_OF_WORDS_IN_CORPUS").getValue();
        AmazonS3 s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);
        try {
            System.out.print("Uploading the corpus description file to S3... ");
            File file = new File("numOfWordsInCorpus.txt");
            FileWriter fw = new FileWriter(file);
            fw.write(Long.toString(numOfWordsInCorpus) + "\n");
            fw.flush();
            fw.close();
            s3.putObject(new PutObjectRequest("dsps162assignment2benasaf/results/", "numOfWordsInCorpus.txt", file));
            System.out.println("Done.");
            System.out.print("Uploading Phase 1 description file to S3... ");
            file = new File("Phase1Results.txt");
            fw = new FileWriter(file);
            fw.write(Long.toString(counter.getValue()) + "\n");
            fw.flush();
            fw.close();
            s3.putObject(new PutObjectRequest("dsps162assignment2benasaf/results/", "Phase1Results.txt", file));
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
