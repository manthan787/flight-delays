package writables;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/***
 * @author: Manthan Thakar & Vineet Trivedi
 * Desctiption: This class contains the information required to compute Airport Delay and Flight delay (AirportId, AirlineId, Month, Year, Delay).
 * It is used in order to prevent the Jobs which compute Airport and Flight delay from iterating over the data again.
 */

public class DelayWritable  implements WritableComparable<DelayWritable> {

    DoubleWritable delay;
    LongWritable count;

    public DelayWritable() {
        this.delay = new DoubleWritable();
        this.count = new LongWritable();
    }

    @Override
    public int compareTo(DelayWritable o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        delay.write(dataOutput);
        count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        delay.readFields(dataInput);
        count.readFields(dataInput);
    }

    @Override
    public String toString() {
        return delay +  "," + count;
    }

    public DoubleWritable getDelay() {
        return delay;
    }

    public void setDelay(DoubleWritable delay) {
        this.delay = delay;
    }

    public LongWritable getCount() {
        return count;
    }

    public void setCount(LongWritable count) {
        this.count = count;
    }
}