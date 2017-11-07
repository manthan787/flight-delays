package writables;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * This custom comparator is used to sort IntWritable values in descending order
 * @author Manthan Thakar
 */
public class IntWritableComparator extends WritableComparator {

    public IntWritableComparator() {
        super(IntWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return -1 * ((IntWritable) a).compareTo((IntWritable) b);
    }
}