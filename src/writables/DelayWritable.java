package writables;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/***
 * @author: Manthan Thakar
 * A custom writable to emit flight delays and counts of flights
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