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
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

public class Phase4 {

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

        private long counter;
        private long outputSize;

        public void setup(Context context) {
            counter = 0l;
            outputSize = Long.parseLong(context.getConfiguration().get("OUTPUT_SIZE"));
        }

        public void reduce(DoubleWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (Text value : values)
                if (counter++ < outputSize)
                    context.write(value, new Text(key.toString()));
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
        if (args.length != 2)
            throw new IOException("Phase 4: supply 2 arguments");
        AmazonS3 s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);

        System.out.print("Downloading corpus description file from S3... ");
        S3Object object = s3.getObject(new GetObjectRequest("dsps162assignment2benasaf/results/", "numOfWordsInCorpus.txt"));
        System.out.println("Done.");
        Scanner sc = new Scanner(new InputStreamReader(object.getObjectContent()));
        String line = sc.nextLine();
        sc.close();
        System.out.println("Number of words in corpus: " + line);

        Configuration conf = new Configuration();
        conf.set("OUTPUT_SIZE", line);
        Job job = Job.getInstance(conf, "Phase 4");
        job.setJarByClass(Phase3.class);
        job.setMapperClass(Mapper4.class);
        job.setPartitionerClass(Partitioner4.class);
        job.setReducerClass(Reducer4.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(Comparator4.class);
        job.setNumReduceTasks(12);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        Counter counter = job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS");
        System.out.println("Num of pairs sent to reducers in phase 3: " + counter.getValue());


        try {
            System.out.print("Uploading Phase 4 description file to S3... ");
            File file = new File("Phase4Results.txt");
            FileWriter fw = new FileWriter(file);
            fw.write(Long.toString(counter.getValue()));
            fw.flush();
            fw.close();
            s3.putObject(new PutObjectRequest("dsps162assignment2benasaf/results/", "Phase4Results.txt", file));
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
