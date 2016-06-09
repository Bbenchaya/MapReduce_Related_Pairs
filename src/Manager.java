import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.terasort.TeraInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Manager {

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        if (args.length != 3) {
            System.err.println("Usage: Manager <in> <out>");
            System.exit(1);
        }
        Job job1 = Job.getInstance(conf1, "Phase 1");
        job1.setJarByClass(Phase1.class);
        job1.setMapperClass(Phase1.Mapper1.class);
        job1.setPartitionerClass(Phase1.Partitioner1.class);
//        job1.setCombinerClass(Phase1.Combiner1.class);
        job1.setReducerClass(Phase1.Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setNumReduceTasks(12);
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        Path output1 = new Path(args[2]);
        FileOutputFormat.setOutputPath(job1, output1);
        boolean result = job1.waitForCompletion(true);
        Counter counter = job1.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS");
        System.out.println("Num of pairs sent to reducers in phase 1: " + counter.getValue());
//        Counters counters = job1.getCounters();
//
//        for (CounterGroup group : counters) {
//            System.out.println("* Counter Group: " + group.getDisplayName() + " (" + group.getName() + ")");
//            System.out.println("  number of counters in this group: " + group.size());
//            for (Counter counter : group) {
//                System.out.println("  - " + counter.getDisplayName() + ": " + counter.getName() + ": "+counter.getValue());
//            }
//        }

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Phase 2");
        job2.setJarByClass(Phase2.class);
        job2.setMapperClass(Phase2.Mapper2.class);
        job2.setPartitionerClass(Phase2.Partitioner2.class);
//        job2.setCombinerClass(Phase2.Combiner2.class);
        job2.setReducerClass(Phase2.Reducer2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(WritableLongPair.class);
        job2.setNumReduceTasks(26);
//        job2.setGroupingComparatorClass(Phase2.Comparator2.class);
        FileInputFormat.addInputPath(job2, output1);
        Path output2 = new Path(args[2] + "2");
        FileOutputFormat.setOutputPath(job2, output2);
        result = job2.waitForCompletion(true);
        counter = job2.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS");
        System.out.println("Num of pairs sent to reducers in phase 2: " + counter.getValue());


//        System.exit(job1.waitForCompletion(true) ? 0 : 1);

    }
}