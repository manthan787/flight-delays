package jobs;

import mappers.AirlineDelayMapper;
import mappers.AirportDelayMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import reducers.DelayReducer;

import java.io.IOException;
/***
 * @author: Manthan Thakar
 * 
 * This driver class consists of 2 jobs:
 * Job 1 calculates the Mean Flight Delay.
 * Job 2 calculates the Mean Airport Delay.
 * The input is [Offset, CleanDataWritable] and the output is [Airport/Flight (Key), Mean Delay
 * per Airport/Flight (Value)].
 * Each job has a distinct Mapper class however the reducer class is common for both jobs.
 */
public class DelayDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        ToolRunner.run(conf, new DelayDriver(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job airlineDelay = createJob("airline", args);
        airlineDelay.waitForCompletion(true);
        if(airlineDelay.isSuccessful()) {
            Job airportDelay = createJob("airport", args);
            airportDelay.waitForCompletion(true);
        }
        return 0;
    }

    public Job createJob(String idType, String[] args) throws IOException {
        Job j = Job.getInstance(getConf(),  idType + " Delay");
        j.setJarByClass(DelayDriver.class);
        if(idType == "airline")
            j.setMapperClass(AirlineDelayMapper.class);
        else
            j.setMapperClass(AirportDelayMapper.class);
        j.setReducerClass(DelayReducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);
        j.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(j, new Path(args[0]));
        FileOutputFormat.setOutputPath(j, new Path(args[1] + "-" + idType));
        return j;
    }
}