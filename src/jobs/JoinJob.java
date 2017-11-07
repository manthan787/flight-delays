package jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import reducers.JoinReducer;

import java.io.IOException;

public class JoinJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        ToolRunner.run(conf, new DelayDriver(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job j = createJob(args);
        j.waitForCompletion(true);
        return 0;
    }

    public Job createJob(String[] args) throws IOException {
        Job j = Job.getInstance(getConf(),  "join");
        j.setJarByClass(JoinJob.class);
        j.setReducerClass(JoinReducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);
        j.setInputFormatClass(SequenceFileInputFormat.class);

        // Add Input path for clean data sequence file
        MultipleInputs.addInputPath(j, new Path(args[0]),
                SequenceFileInputFormat.class, JoinMapper.class);

        // Add Input Path for tagging user query
        MultipleInputs.addInputPath(j, new Path("query"),
                SequenceFileInputFormat.class, JoinQueryMapper.class);
        FileOutputFormat.setOutputPath(j, new Path("join-out"));
        return j;
    }
}
