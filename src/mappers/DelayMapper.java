package mappers;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import utils.CSVRecord;
import utils.Parser;
import writables.DelayWritable;

import java.io.IOException;

/***
 * @author Manthan Thakar
 */
public class DelayMapper extends Mapper<LongWritable, Text, Text, DelayWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(! value.toString().startsWith("YEAR") ) {
            CSVRecord record = new CSVRecord(value.toString());
            Parser p = new Parser(record);
            if (p.sanityCheck()) {
                Text outKey = new Text(p.getAirline() + "," + p.getAirport() + "," + p.getMonth() + "," + p.getYear());
                DelayWritable outValue = new DelayWritable();
                outValue.setDelay(new DoubleWritable(p.getArrDelNew()));
                outValue.setCount(new LongWritable(1));
                context.write(outKey, outValue);
            }
        }
    }
}