package com.cisco.josouthe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

public class Utility {
    private static final Logger logger = LogManager.getFormatterLogger();
    private static SimpleDateFormat simpleDateFormatWithT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    private static Pattern patternDateStringWithT = Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\..*");
    private static SimpleDateFormat simpleDateFormatWithoutT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z z");
    private static Pattern patternDateStringWithoutT = Pattern.compile("\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\s[+-]\\d{4}\\s[A-Z]{3}");
    private static Pattern patternDecimalNumber = Pattern.compile("\\d+\\.\\d+");
    private static long dayInMilliseconds = 1000*60*60*24;

    public static long getDateFromString( String date ) throws ParseException {
        if( date == null || "".equals(date) )
            throw new ParseException("We can't parse an empty date string",-1);
        SimpleDateFormat simpleDateFormat = null;
        if(patternDateStringWithT.matcher(date).matches() ) {
            simpleDateFormat = simpleDateFormatWithT;
        } else if( patternDateStringWithoutT.matcher(date).matches() ) {
            simpleDateFormat = simpleDateFormatWithoutT;
        }
        if( simpleDateFormat == null )
            throw new ParseException("We don't have a parser defined for this date format: "+ date, -1);
        return simpleDateFormat.parse(date).getTime();
    }

    public static long now() { return System.currentTimeMillis(); }

    public static long getDaysUntil(String valid_until) {
        try {
            long expirationTime = getDateFromString(valid_until)-now();
            return expirationTime/dayInMilliseconds;
        } catch (ParseException e) {
            logger.warn("Parse Exception on date '%s' message: %s",valid_until, e.getMessage());
            return -1;
        }
    }

    public static boolean isDecimalNumber( String candidate ) {
        return patternDecimalNumber.matcher(candidate).matches();
    }
    public static long decimalToLong( String decimal ) {
        Double number = Double.parseDouble(decimal);
        number *= 100;
        return number.longValue();
    }

}
