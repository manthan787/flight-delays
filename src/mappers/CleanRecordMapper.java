package mappers;

import exceptions.InvalidRecordException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.CSVRecord;
import utils.Parser;
import writables.CleanDataWritable;
import java.io.IOException;

/***
 * @author Manthan Thakar & Vineet Trivedi
 * Description:
 * Input: [Offset (Key), Record(Value)]
 * Output: [Offset (Key), CleanDataWritable(Value)]
 * It passes the record to Parser. If it passes the sanity checks performed by the parser
 * the information required by the remaining jobs (Delay, Month, Year, AirportId, AirlineId)
 * is contained in a custom Writable called CleanDataWritable.
 * The mapper then writes the Offset and the CleanDataWritable to the output.
 */

public class CleanRecordMapper extends Mapper<LongWritable, Text, LongWritable, CleanDataWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(! value.toString().startsWith("YEAR") ) {
            CSVRecord record = new CSVRecord(value.toString());
            Parser p = new Parser(record);
            if (p.sanityCheck()) {
                context.write(key, getCleanData(p));
            }
        }
    }

    public CleanDataWritable getCleanData(Parser p) {
        CleanDataWritable d = new CleanDataWritable();
        d.setMonth(new IntWritable(p.getMonth()));
        if(p.isCancelled()) d.setDelay(new FloatWritable(4f));
        else d.setDelay(new FloatWritable( p.getArrDelNew() / p.getCrsElapsedTime()));
        d.setYear(new IntWritable(p.getYear()));
        d.setAirlineID(new IntWritable(p.getAirlineID()));
        d.setAirportID(new IntWritable(p.getAirportID()));
        return d;
    }
}