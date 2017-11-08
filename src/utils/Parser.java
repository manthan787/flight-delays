package utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/***
 * @author: Manthan Thakar
 * Description: Gets input as a record(String) and performs sanity checks against that record.
 */

public class Parser {
    private CSVRecord r;
    private int year;
    private int month;
    private String airline;
    private String airport;
    private boolean cancelled;
    private float arrDelNew;
    private int crsElapsedTime;
    private static final  int EXPECTED_FIELD_COUNT = 110;
    DateFormat df = new SimpleDateFormat("hhmm");

    public Parser(CSVRecord r) {
        this.r = r;
    }

    public float getArrDelNew() {
        return arrDelNew;
    }

    public int getCrsElapsedTime() {
        return crsElapsedTime;
    }

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public String getAirline() {
        return airline;
    }

    public void setAirline(String airline) {
        this.airline = airline;
    }

    public String getAirport() {
        return airport;
    }

    public void setAirport(String airport) {
        this.airport = airport;
    }

    public boolean sanityCheck() {
        try {
            // Check if number of fields is as expected
            if (r.getFieldCount() != EXPECTED_FIELD_COUNT) return false;
            if (! isYear(Integer.parseInt(r.get(0)))) return false;

            // check if the cancelled field has valid (binary) values
            int cancelled = Integer.parseInt(r.get(47));
            if (cancelled < 0 || cancelled > 1) return false;

            // Check if the month field is valid month number
            if (! isMonth(Integer.parseInt(r.get(2)))) return false;

            // Check if airlineID is positive
            if (! isPositive(Integer.parseInt(r.get(7)))) return false;

            // Check if airportID is positive
            if (! isPositive(Integer.parseInt(r.get(20)))) return false;

            // Check elapsed times
            if (!(Integer.parseInt(r.get(51)) > 0)) return false;
            if (!(Integer.parseInt(r.get(50)) > 0)) return false;

            // Check Delays
            Float.parseFloat(r.get(42));
            Float.parseFloat(r.get(43));
            Float.parseFloat(r.get(44));

            // AirportID,  AirportSeqID, CityMarketID, StateFips, Wac should be larger than 0
            if ((Integer.parseInt(r.get(20)) < 0)
                    || (Integer.parseInt(r.get(21)) < 0)
                    || (Integer.parseInt(r.get(22)) < 0)
                    || (Integer.parseInt(r.get(26)) < 0)
                    || (Integer.parseInt(r.get(28)) < 0))
                return false;

            // Origin, Destination,  CityName, State, StateName should not be empty
            if(this.locationNamesEmpty()) return false;

            // Test data for sanity tests
            if(! sanityTests()) return false;
            // set important fields so that they can be accessed from the parser object
            setRequiredFields();
        } catch(NumberFormatException e) {
            return false;
        }
        return true;
    }

    private boolean locationNamesEmpty() {
        // Return true if any of Origin, Destination,  CityName, State, StateName
        // are empty
        return isEmpty(r.get(14)) || isEmpty(r.get(15)) || isEmpty(r.get(18))
                || isEmpty(r.get(16)) || isEmpty(r.get(23)) || isEmpty(r.get(24))
                || isEmpty(r.get(25)) || isEmpty(r.get(27)) || isEmpty(r.get(8));
    }

    private boolean isEmpty(String s) {
        return (s.isEmpty() || !s.matches("[a-zA-Z, ]+"));
    }

    private void setRequiredFields() {
        this.year = Integer.parseInt(r.get(0));
        this.month = Integer.parseInt(r.get(2));
        this.airport = r.get(23);
        this.airline = r.get(8);
    }

    private boolean sanityTests() {
        try {
            Date crsArrTime = df.parse(r.get(40));
            Date arrTime = df.parse(r.get(41));
            Date crsDeptTime = df.parse(r.get(29));
            crsElapsedTime = Integer.parseInt(r.get(50));
            cancelled = (Integer.parseInt(r.get(47))) == 1;
            int actualElapsedTime = Integer.parseInt(r.get(51));
            Date depTime = df.parse(r.get(30));
            // various other checks mentioned in the assignment
            long timezone = TimeUnit.MILLISECONDS.toMinutes(crsArrTime.getTime() - crsDeptTime.getTime()) - crsElapsedTime;
            float arrDel = Float.parseFloat(r.get(42));
            arrDelNew = Float.parseFloat(r.get(43));
            float arrDel15 = Float.parseFloat(r.get(44));
            if (timezone % 60 != 0) return false;
            if (!cancelled) {
                if((TimeUnit.MILLISECONDS.toMinutes(arrTime.getTime() - depTime.getTime()) - actualElapsedTime - timezone) != 0) return false;
                if (arrDel > 0)
                    if (arrDel !=  arrDelNew) return false;
                if (arrDel < 0)
                    if (arrDelNew != 0) return false;
                if (arrDel >= 15)
                    if (arrDel15 != 1) return false;
            }
        } catch (ParseException e) {
            return false;
        }
        return true;
    }

    private boolean isMonth(int month) {
        return (month <= 12 && month > 0);
    }

    private boolean isYear(int year) {
        return (year > 1800 && year < 2018);
    }

    private boolean isPositive(int num) {
        return num > 0;
    }
}