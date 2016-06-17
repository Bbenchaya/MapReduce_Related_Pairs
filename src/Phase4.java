/**
 * Created by asafchelouche on 6/6/16.
 */

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Phase4 {

    private static final String NUM_OF_PAIRS_SENT_TO_REDUCERS_FILENAME = "Phase 4 - num of pairs sent to reducers.txt";

    public static class Mapper4
            extends Mapper<DoubleWritable, Text, DoubleWritable, Text>{


        @Override
        public void map(DoubleWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class Partitioner4 extends Partitioner<DoubleWritable, Text> {

        @Override
        public int getPartition(DoubleWritable key, Text value, int i) {
            String[] components = value.toString().split("[$]");
            int year = Integer.parseInt(components[0]);
            year -= 1900;
            return year / 10;
        }
    }

    public static class Reducer4
            extends Reducer<DoubleWritable, Text, Text, Text> {

        private static final String LAST_DECADE_PMI_RESULTS_FILENAME = "lastDecadePMIResults.txt";
        private long counter;
        private File allPairsInLastDecade;
        private BufferedWriter br;
        private AmazonS3 s3;
        private boolean reducerHandlesLastDecade;
        private long outputSize;

        @Override
        public void setup(Context context) throws IOException {
            counter = 0l;
            allPairsInLastDecade = new File(LAST_DECADE_PMI_RESULTS_FILENAME);
            br = new BufferedWriter(new FileWriter(allPairsInLastDecade));
            s3 = new AmazonS3Client();
            Region usEast1 = Region.getRegion(Regions.US_EAST_1);
            s3.setRegion(usEast1);
            reducerHandlesLastDecade = false;
            outputSize = Long.parseLong(context.getConfiguration().get("OUTPUT_SIZE"));
        }

        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            if (!reducerHandlesLastDecade && Integer.parseInt(values.iterator().next().toString().split("[$]")[0]) >= 2000)
                reducerHandlesLastDecade = true;
            for (Text value : values) {
                if (counter++ < outputSize)
                    context.write(value, new Text(key.toString()));
                if (reducerHandlesLastDecade)
                    br.write(value.toString() + "\t" +  key.toString() + "\n");
            }
        }

        @Override
        public void cleanup(Context context) throws IOException {
            br.flush();
            br.close();
            if (reducerHandlesLastDecade) {
                System.out.print("Uploading the last decade's results file to S3... ");
                s3.putObject(new PutObjectRequest("dsps162assignment2benasaf/results/", LAST_DECADE_PMI_RESULTS_FILENAME, allPairsInLastDecade));
                System.out.println("Done.");
            }
        }
    }

    public static class Comparator4 extends WritableComparator {

        @Override
        public int compare(byte[] bytes1, int s1, int l1, byte[] bytes2, int s2, int l2) {
            double d1 = readDouble(bytes1, s1);
            double d2 = readDouble(bytes2, s2);
            // invert the comparison outcome in order to sort the output in descending order by PMI
            if (d1 == d2)
                return 0;
            else return (d2 - d1 > 0) ? 1 : -1;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3)
            throw new IOException("Phase 4: supply 3 arguments");
        System.out.println("The output for each decade would contain only the top " + args[2] + " PMI values.");
        Configuration conf = new Configuration();
        conf.set("OUTPUT_SIZE", args[2]);
        Job job = Job.getInstance(conf, "Phase 4");
        job.setJarByClass(Phase4.class);
        job.setMapperClass(Mapper4.class);
        job.setPartitionerClass(Partitioner4.class);
        job.setReducerClass(Reducer4.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(Comparator4.class);
        job.setNumReduceTasks(12);
        System.out.println("Phase 4 - input path: " + args[0] + ", output path: " + args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (job.waitForCompletion(true))
            System.out.println("Phase 4: job completed successfully");
        else
            System.out.println("Phase 4: job completed unsuccessfully");
        Counter counter = job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS");
        System.out.println("Num of pairs sent to reducers in phase 4: " + counter.getValue());
        AmazonS3 s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);
        try {
            System.out.print("Uploading Phase 4 description file to S3... ");
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
