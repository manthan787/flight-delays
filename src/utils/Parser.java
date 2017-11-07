package utils;

/***
 * @author: Manthan Thakar & Vineet Trivedi
 * Description: Gets input as a record(String) and performs sanity checks against that record.
 */

public class Parser {
    private CSVRecord r;
    private int year;
    private int month;
    private int airlineID;
    private int airportID;
    private boolean cancelled;
    private float arrDelNew;
    private int crsElapsedTime;
    private static final  int EXPECTED_FIELD_COUNT = 110;

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

    public int getAirlineID() {
        return airlineID;
    }

    public int getAirportID() {
        return airportID;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public boolean sanityCheck() {
        try {
            // Check if number of fields is as expected
            if (r.getFieldCount() != EXPECTED_FIELD_COUNT) return false;
            if (! isYear(Integer.parseInt(r.get(0)))) return false;//YEAR

            // check if the cancelled field has valid (binary) values
            int cancelled = Integer.parseInt(r.get(47));
            if (cancelled < 0 || cancelled > 1) return false;

            // Check if the month field is valid month number
            if (! isMonth(Integer.parseInt(r.get(2)))) return false;

            // Check if airlineID is positive
            if (! isPositive(Integer.parseInt(r.get(7)))) return false;

            // Check if airportID is positive
            if (! isPositive(Integer.parseInt(r.get(20)))) return false;

            // Check Arrival Times
            if (! isPositive(Integer.parseInt(r.get(40)))) return false;
            if (! isPositive(Integer.parseInt(r.get(41)))) return false;

            // Check departure times
            if (! isPositive(Integer.parseInt(r.get(29)))) return false;
            if (! isPositive(Integer.parseInt(r.get(30)))) return false;

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
        if (isEmpty(r.get(14)) || isEmpty(r.get(15)) || isEmpty(r.get(18))
            || isEmpty(r.get(16))|| isEmpty(r.get(23))|| isEmpty(r.get(24))
            || isEmpty(r.get(25))|| isEmpty(r.get(27)))
            return true;
        return false;
    }

    private boolean isEmpty(String s) {
        return (s.isEmpty() || !s.matches("[a-zA-Z, ]+"));
    }

    private void setRequiredFields() {
        this.year = Integer.parseInt(r.get(0));
        this.month = Integer.parseInt(r.get(2));
        this.airportID = Integer.parseInt(r.get(20));
        this.airlineID = Integer.parseInt(r.get(7));
    }

    public boolean sanityTests() {
        int crsArrTime = Integer.parseInt(r.get(40));
        int arrTime = Integer.parseInt(r.get(41));
        int crsDeptTime = Integer.parseInt(r.get(29));
        crsElapsedTime = Integer.parseInt(r.get(50));
        cancelled = ((Integer.parseInt(r.get(47))) == 1) ? true : false;
        int actualElapsedTime = Integer.parseInt(r.get(51));
        int depTime = Integer.parseInt(r.get(30));
        // various other checks mentioned in the assignment
        int timezone = crsArrTime - crsDeptTime - crsElapsedTime;
        float arrDel = Float.parseFloat(r.get(42));
        arrDelNew = Float.parseFloat(r.get(43));
        float arrDel15 = Float.parseFloat(r.get(44));
        if (timezone % 60 != 0) return false;
        if (!cancelled) {
            if((arrTime - depTime - actualElapsedTime - timezone) != 0) return false;
            if (arrDel > 0)
                if (arrDel !=  arrDelNew) return false;
            if (arrDel < 0)
                if (arrDelNew != 0) return false;
            if (arrDel >= 15)
                if (arrDel15 != 1) return false;
        }
        return true;
    }

    public boolean isMonth(int month) {
        return (month <= 12 && month > 0);
    }

    public boolean isYear(int year) {
        return (year > 1800 && year < 2018);
    }

    public boolean isPositive(int num) {
        return num > 0;
    }
}