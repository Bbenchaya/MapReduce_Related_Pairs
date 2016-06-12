import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Manager {

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: Manager <in> <out>");
            System.exit(1);
        }
        Job job1 = Job.getInstance(conf1, "Phase 1");
        job1.setJarByClass(Phase1.class);
        job1.setMapperClass(Phase1.Mapper1.class);
//        job1.setPartitionerClass(Phase1.Partitioner1.class);
//        job1.setCombinerClass(Phase1.Combiner1.class);
        job1.setReducerClass(Phase1.Reducer1.class);
        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
//        job1.setNumReduceTasks(12);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path output1 = new Path(args[1]);
        FileOutputFormat.setOutputPath(job1, output1);
        boolean result = job1.waitForCompletion(true);
        Counter counter = job1.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS");
        System.out.println("Num of pairs sent to reducers in phase 1: " + counter.getValue());
        long numOfWordsInCorpus = job1.getCounters().findCounter("Phase1$Mapper1$CountersEnum", "NUM_OF_WORDS_IN_CORPUS").getValue();
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
        job2.setReducerClass(Phase2.Reducer2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(WritableLongPair.class);
        job2.setNumReduceTasks(26);
        FileInputFormat.addInputPath(job2, output1);
        Path output2 = new Path(args[1] + "2");
        FileOutputFormat.setOutputPath(job2, output2);
        result = job2.waitForCompletion(true);
        counter = job2.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS");
        System.out.println("Num of pairs sent to reducers in phase 2: " + counter.getValue());

        Configuration conf3 = new Configuration();
        conf3.set("NUM_OF_WORDS_IN_CORPUS", new Long(numOfWordsInCorpus).toString());
        Job job3 = Job.getInstance(conf3, "Phase 3");
        job3.setJarByClass(Phase3.class);
        job3.setMapperClass(Phase3.Mapper3.class);
        job3.setPartitionerClass(Phase3.Partitioner3.class);
        job3.setReducerClass(Phase3.Reducer3.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(WritableLongPair.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);
        job3.setOutputKeyClass(DoubleWritable.class);
        job3.setOutputValueClass(Text.class);
        job3.setNumReduceTasks(26);
        FileInputFormat.addInputPath(job3, output2);
        Path output3 = new Path(args[1] + "3");
        FileOutputFormat.setOutputPath(job3, output3);
        result = job3.waitForCompletion(true);
        counter = job3.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS");
        System.out.println("Num of pairs sent to reducers in phase 3: " + counter.getValue());

        Configuration conf4 = new Configuration();
        Job job4 = Job.getInstance(conf4, "Phase 4");
        job4.setJarByClass(Phase3.class);
        job4.setMapperClass(Phase4.Mapper4.class);
        job4.setPartitionerClass(Phase4.Partitioner4.class);
        job4.setReducerClass(Phase4.Reducer4.class);
        job4.setInputFormatClass(SequenceFileInputFormat.class);
        job4.setMapOutputKeyClass(DoubleWritable.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setSortComparatorClass(Phase4.Comparator4.class);
        job4.setNumReduceTasks(12);
        FileInputFormat.addInputPath(job4, output3);
        Path output4 = new Path(args[1] + "4");
        FileOutputFormat.setOutputPath(job4, output4);
        result = job4.waitForCompletion(true);
        counter = job4.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS");
        System.out.println("Num of pairs sent to reducers in phase 4: " + counter.getValue());

//        System.exit(job1.waitForCompletion(true) ? 0 : 1);

    }
}