package mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import writables.CleanDataWritable;
import java.io.IOException;

/**
 * @author Manthan Thakar
 * This is a common mapper for finding Most Active Airports and Airlines.
 * Depending on the value passed to `idType` it emits counts for given
 * id (i.e. airportID and airlineID).
 */
public class ActivityMapper extends Mapper<LongWritable, CleanDataWritable, IntWritable, IntWritable> {

    // This is one of airlineID and airplaneID
    String idType;

    private IntWritable ONE = new IntWritable(1);

    /**
     * Gets the config value "idType" which helps us determine what ID to use for tracking
     * activity
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        idType = context.getConfiguration().get("idType");
    }

    /**
     * Gets line offset as key and cleaned data as value and writes the counts for
     * "idType" to `context`
     * @param key line offset
     * @param value cleaned data with required fields
     * @param context
     */
    public void map(LongWritable key, CleanDataWritable value, Context context)
            throws IOException, InterruptedException {
        context.write(getID(value), ONE);
    }

    /**
     * Returns appropriate ID field from CleanDataWritable based on the given `idType`
     * @param data cleaned data to read ID from
     * @return airlineID field from data, iff `idType` equals airlineID,
     *         airportID field from data, iff  `idType` equals airportID
     * @throws UnsupportedOperationException if `idType` is not valid
     */
    public IntWritable getID(CleanDataWritable data) {
        if(idType.equals("airlineID")) return data.getAirlineID();
        else if(idType.equals("airportID")) return data.getAirportID();
        else throw new UnsupportedOperationException("The given ID type isn't supported");
    }
}
