package mappers;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import writables.CleanDataWritable;

/**
 * @author Vineet Trivedi
 * Description:
 * Input: [Offset(Key), CleanDataWritable(Value)]
 * Output: [CustomKey(Key), Delay(Value)]
 * Forms a custom key from Airport, month and year.
 * Writes the custom key and delay(value) to output.
 */

public class AirportDelayMapper extends Mapper<LongWritable, CleanDataWritable, Text, FloatWritable> {

    @Override
    protected void map(LongWritable key, CleanDataWritable value, Context context)
            throws IOException, InterruptedException {
        IntWritable airportID = value.getAirportID();
        IntWritable month = value.getMonth();
        IntWritable year = value.getYear();
        FloatWritable delay = value.getDelay();
        String outKey = airportID.toString() + "," + month.toString() + "," + year.toString();
        if (delay.compareTo(new FloatWritable(0)) != 0) {
            context.write(new Text(outKey), delay);
        }
    }
}
