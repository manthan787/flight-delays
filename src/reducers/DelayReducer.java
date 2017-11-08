package reducers;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DelayReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float totalDelay = 0f;
        long count = 0;
        for(FloatWritable d : values) {
            totalDelay += d.get();
            count += 1;
        }
        context.write(key, new FloatWritable(totalDelay / count));
    }
}