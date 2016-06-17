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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

public class Phase3 {

    private static final int ASCII_OFFSET = 97;
    private static final String NUM_OF_PAIRS_SENT_TO_REDUCERS_FILENAME = "Phase 3 - num of pairs sent to reducers.txt";

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
            String[] components = key.toString().split("[$]");
            return (int)(components[1].charAt(0)) - ASCII_OFFSET;
        }
    }

    public static class Reducer3
            extends Reducer<Text, WritableLongPair, DoubleWritable, Text> {

        private final int NUM_OF_DECADES = 12;
        private long[] wordsPerYear;
        public static final String WORDS_PER_DECADE_FILENAME = "wordsPerDecade.txt";

        @Override
        public void setup(Context context) {
            AmazonS3 s3 = new AmazonS3Client();
            Region usEast1 = Region.getRegion(Regions.US_EAST_1);
            s3.setRegion(usEast1);
            S3Object object = s3.getObject(new GetObjectRequest("dsps162assignment2benasaf/results/", WORDS_PER_DECADE_FILENAME));
            Scanner scanner = new Scanner(new InputStreamReader(object.getObjectContent()));
            wordsPerYear = new long[NUM_OF_DECADES];
            for (int i = 0; i < NUM_OF_DECADES; i++) {
                String line = scanner.nextLine();
                wordsPerYear[i] = Long.parseLong(line);
            }
            scanner.close();
        }

        @Override
        public void reduce(Text key, Iterable<WritableLongPair> counts,
                           Context context
        ) throws IOException, InterruptedException {
            long pairCount, firstWordCount, secondWordCount;
            WritableLongPair pair1 = null;
            WritableLongPair pair2 = null;
            int index = (Integer.parseInt(key.toString().split("[$]")[0]) - 1900) / 10;
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
                double PMI = Math.log(pairCount) + Math.log(2 * firstWordCount) + Math.log(wordsPerYear[index]);
                context.write(new DoubleWritable(PMI), key);
            }
            else {
                pairCount = pair1.getL1();
                firstWordCount = pair1.getL2();
                secondWordCount = pair2.getL2();
                double PMI = Math.log(pairCount) + Math.log(firstWordCount) + Math.log(secondWordCount) + Math.log(wordsPerYear[index]);
                context.write(new DoubleWritable(PMI), key);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2)
            throw new IOException("Phase 3: supply 2 arguments");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Phase 3");
        job.setJarByClass(Phase3.class);
        job.setMapperClass(Mapper3.class);
        job.setPartitionerClass(Partitioner3.class);
        job.setReducerClass(Reducer3.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WritableLongPair.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(26);
        System.out.println("Phase 3 - input path: " + args[0] + ", output path: " + args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (job.waitForCompletion(true))
            System.out.println("Phase 3: job completed successfully");
        else
            System.out.println("Phase 3: job completed unsuccessfully");
        Counter counter = job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS");
        System.out.println("Num of pairs sent to reducers in phase 3: " + counter.getValue());
        AmazonS3 s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);
        try {
            System.out.print("Uploading Phase 3 description file to S3... ");
            File file = new File(NUM_OF_PAIRS_SENT_TO_REDUCERS_FILENAME);
            FileWriter fw = new FileWriter(file);
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
