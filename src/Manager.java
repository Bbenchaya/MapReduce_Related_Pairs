import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Manager {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 3) {
            System.err.println("Usage: Manager <in> <out>");
            System.exit(1);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Phase1.class);
        job.setMapperClass(Phase1.Mapper1.class);
        job.setPartitionerClass(Phase1.Partitioner1.class);
//        job.setCombinerClass(Phase1.Combiner1.class);
        job.setReducerClass(Phase1.Reducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setNumReduceTasks(12);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        boolean result = job.waitForCompletion(true);
//        Counters counters = job.getCounters();
//
//        for (CounterGroup group : counters) {
//            System.out.println("* Counter Group: " + group.getDisplayName() + " (" + group.getName() + ")");
//            System.out.println("  number of counters in this group: " + group.size());
//            for (Counter counter : group) {
//                System.out.println("  - " + counter.getDisplayName() + ": " + counter.getName() + ": "+counter.getValue());
//            }
//        }
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.exit(result ? 0 : 1);
    }
}