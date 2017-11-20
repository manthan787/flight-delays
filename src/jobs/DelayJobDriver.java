package jobs;

import mappers.DelayMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import reducers.DelayReducer;
import writables.DelayWritable;

import java.io.IOException;
/***
 * @author: Manthan Thakar
 * Job Description:
 * This job is responsible for cleaning data and emitting valid flight records
 * with delays
 */
public class DelayJobDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        ToolRunner.run(conf, new DelayJobDriver(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        try {
            Job job = Job.getInstance(getConf(), "Clean Data");
            job.setJarByClass(DelayJobDriver.class);
            job.setMapperClass(DelayMapper.class);
            job.setCombinerClass(DelayReducer.class);
            job.setReducerClass(DelayReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DelayWritable.class);
            Path outputPath = new Path(args[1]);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileInputFormat.setInputDirRecursive(job, true);
            FileOutputFormat.setOutputPath(job, outputPath);
            job.waitForCompletion(true);
            if(job.isSuccessful()) return 0;
            return 1;
        } catch (IOException e) {
            e.printStackTrace();
            return 1;
        }
    }
}