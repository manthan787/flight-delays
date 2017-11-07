package jobs;

import mappers.ActivityMapper;
import mappers.ActivitySwapMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import reducers.ActivitySwapReducer;
import writables.IntWritableComparator;
import java.io.IOException;
/***
 * @author Manthan Thakar
 * This class consists of 1 Job that can be used to calculate either,
 * Top 5 active Airlines
 * Top 5 active Airports
 * depending on the input parameter.
 * This job is run twice, first for Top 5 Airlines and next for Top 5 Airports.
 */
public class ActivityJobDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new ActivityJobDriver(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        if(args.length != 3) {
            System.err.println("Arguments expected are: <input_path> <output_path> <idType>");
            System.exit(1);
        }
        FileSystem hdfs = FileSystem.get(getConf());
        getConf().set("idType", args[2]);
        Path inputPath = new Path(args[0]);
        Path activityJobOut = new Path("activity-job");
        Path outputPath = new Path(args[1]);
        if(hdfs.exists(activityJobOut)) {
            hdfs.delete(activityJobOut, true);
        }
        Job activityJob = createActivityJob(inputPath, activityJobOut);
        activityJob.setInputFormatClass(SequenceFileInputFormat.class);
        activityJob.waitForCompletion(true);
        activityJob.waitForCompletion(true);
        if(activityJob.isSuccessful()) {
            Job swapJob = createSwapJob(activityJobOut, outputPath);
            swapJob.waitForCompletion(true);
            return 0;
        }
        return 1;
    }

    protected Job createActivityJob(Path inputPath, Path outputPath) throws IOException {
        Job j = Job.getInstance(getConf(), "Activity Job");
        j.setJarByClass(ActivityJobDriver.class);
        j.setMapperClass(ActivityMapper.class);
        j.setCombinerClass(IntSumReducer.class);
        j.setReducerClass(IntSumReducer.class);
        j.setOutputKeyClass(IntWritable.class);
        j.setOutputValueClass(IntWritable.class);
        j.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(j, inputPath);
        FileInputFormat.setInputDirRecursive(j, true);
        FileOutputFormat.setOutputPath(j, outputPath);
        return j;
    }

    protected Job createSwapJob(Path inputPath, Path outputPath) throws IOException {
        Job j = Job.getInstance(getConf(), "Activity Swap Job");
        j.setJarByClass(ActivityJobDriver.class);
        j.setMapperClass(ActivitySwapMapper.class);
        j.setReducerClass(ActivitySwapReducer.class);
        j.setSortComparatorClass(IntWritableComparator.class);
        j.setOutputKeyClass(IntWritable.class);
        j.setOutputValueClass(IntWritable.class);
        j.setInputFormatClass(SequenceFileInputFormat.class);
        j.setNumReduceTasks(1);
        FileInputFormat.addInputPath(j, inputPath);
        FileInputFormat.setInputDirRecursive(j, true);
        FileOutputFormat.setOutputPath(j, outputPath);
        return j;
    }
}
