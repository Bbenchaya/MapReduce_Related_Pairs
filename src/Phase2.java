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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.File;
import java.io.FileWriter;
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

    public static void main(String[] args) throws Exception {
        if (args.length != 2)
            throw new IOException("Phase 2: supply 2 arguments");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Phase 2");
        job.setJarByClass(Phase2.class);
        job.setMapperClass(Phase2.Mapper2.class);
        job.setPartitionerClass(Phase2.Partitioner2.class);
        job.setReducerClass(Phase2.Reducer2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(WritableLongPair.class);
        job.setNumReduceTasks(26);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        Counter counter = job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS");
        System.out.println("Num of pairs sent to reducers in phase 2: " + counter.getValue());
        AmazonS3 s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);
        try {
            System.out.print("Uploading Phase 2 description file to S3... ");
            File file = new File("Phase2Results.txt");
            FileWriter fw = new FileWriter(file);
            fw.write(Long.toString(counter.getValue()) + "\n");
            fw.flush();
            fw.close();
            s3.putObject(new PutObjectRequest("dsps162assignment2benasaf/results/", "Phase2Results.txt", file));
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
