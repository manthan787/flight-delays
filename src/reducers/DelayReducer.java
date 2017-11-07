package reducers;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author: Vineet Trivedi
 * Description:
 * Input: [CustomKey(Key), Delays(Values)]
 * Output: [CustomKey(Key), MeanDelay(Value)]
 * Computes the mean delay from the list of delays obtained against a custom key.
 * Writes the custom key and the mean delay to the output.
 */

public class DelayReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        float sum = 0f;
        int count = 1;
        for (FloatWritable value : values) {
            sum += value.get();
            count++;
        }
        context.write(key, new FloatWritable(sum / count));
    }
}
