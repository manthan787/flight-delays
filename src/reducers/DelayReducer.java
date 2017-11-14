package reducers;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import writables.DelayWritable;

import java.io.IOException;

public class DelayReducer extends Reducer<Text, DelayWritable, Text, DelayWritable> {

    public void reduce(Text key, Iterable<DelayWritable> values, Context context) throws IOException, InterruptedException {
        double totalDelay = 0f;
        long count = 0;
        for(DelayWritable d : values) {
            totalDelay += d.getDelay().get();
            count += d.getCount().get();
        }
        DelayWritable outValue = new DelayWritable();
        outValue.setDelay(new DoubleWritable(totalDelay));
        outValue.setCount(new LongWritable(count));
        context.write(key, outValue);
    }
}