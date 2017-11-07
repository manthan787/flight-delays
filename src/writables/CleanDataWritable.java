package writables;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/***
 * @author: Manthan Thakar & Vineet Trivedi
 * Desctiption: This class contains the information required to compute Airport Delay and Flight delay (AirportId, AirlineId, Month, Year, Delay).
 * It is used in order to prevent the Jobs which compute Airport and Flight delay from iterating over the data again.
 */

public class CleanDataWritable  implements WritableComparable<CleanDataWritable> {

    IntWritable airlineID;
    IntWritable airportID;
    IntWritable month;
    IntWritable year;
    FloatWritable delay;

    public CleanDataWritable() {
        this.airlineID = new IntWritable();
        this.airportID = new IntWritable();
        this.month = new IntWritable();
        this.year = new IntWritable();
        this.delay = new FloatWritable();
    }

    public IntWritable getYear() {
        return year;
    }

    public void setYear(IntWritable year) {
        this.year = year;
    }

    public IntWritable getAirlineID() {
        return airlineID;
    }

    public void setAirlineID(IntWritable airlineID) {
        this.airlineID = airlineID;
    }

    public IntWritable getAirportID() {
        return airportID;
    }

    public void setAirportID(IntWritable airportID) {
        this.airportID = airportID;
    }

    public IntWritable getMonth() {
        return month;
    }

    public void setMonth(IntWritable month) {
        this.month = month;
    }

    public FloatWritable getDelay() {
        return delay;
    }

    public void setDelay(FloatWritable delay) {
        this.delay = delay;
    }


    @Override
    public int compareTo(CleanDataWritable o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        airlineID.write(dataOutput);
        airportID.write(dataOutput);
        month.write(dataOutput);
        year.write(dataOutput);
        delay.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        airlineID.readFields(dataInput);
        airportID.readFields(dataInput);
        month.readFields(dataInput);
        year.readFields(dataInput);
        delay.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "CleanDataWritable{" +
                "airlineID=" + airlineID +
                ", airportID=" + airportID +
                ", month=" + month +
                ", year=" + year +
                ", delay=" + delay +
                '}';
    }
}
