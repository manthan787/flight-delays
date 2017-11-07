package reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * @author Manthan Thakar
 */
public class ActivitySwapReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    public void reduce(IntWritable freq, Iterable<IntWritable> value, Context context)
            throws IOException, InterruptedException {
        for(IntWritable v : value) {
            context.write(freq, v);
        }
    }
}
