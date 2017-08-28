
/*
 * net/balusc/util/DateUtil.java
 *
 * Copyright (C) 2007 BalusC
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if
 * not, write to the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */





/**
 * Useful Date utilities.
 *
 * @author BalusC
 * @see CalendarUtil
 * @link http://balusc.blogspot.com/2007/09/dateutil.html
 */
public final class DateUtil {

    // Init ---------------------------------------------------------------------------------------
	
	
	/*
	static {
		
		// courtesy http://txt2re.com
		//String txt="Wed Sep 11 00:00:00 EDT 2013";

	    //re1="((?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|Tues|Thur|Thurs|Sun|Mon|Tue|Wed|Thu|Fri|Sat))";
	    re1="((?:sun|mon|tue|wed|thu|fri|sat))";// Day Of Week 1
	    re2="(\\s+)";	// White Space 1
	    //re3="((?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Sept|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?))";	// Month 1
	    re3="((?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|sept|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?))";	// Month 1
	    re4="(\\s+)";	// White Space 2
	    re5="((?:(?:[0-2]?\\d{1})|(?:[3][01]{1})))(?![\\d])";	// Day 1
	    re6="(\\s+)";	// White Space 3
	    re7="((?:(?:[0-1][0-9])|(?:[2][0-3])|(?:[0-9])):(?:[0-5][0-9])(?::[0-5][0-9])?(?:\\s?(?:am|AM|pm|PM))?)";	// HourMinuteSec 1
	    re8="(\\s+)";	// White Space 4
	    re9="((?:[a-z][a-z]+))";	// Word 1
	    re10="(\\s+)";	// White Space 5
	    re11="((?:(?:[1]{1}\\d{1}\\d{1}\\d{1})|(?:[2]{1}\\d{3})))(?![\\d])";	// Year 1
	    
	    re1="((Mon)|(Tues)|(Wednes)|(Thurs)|(Fri)|(Satur)|(Sun))day";// Day Of Week 1

	    //Pattern p = Pattern.compile(re1+re2+re3+re4+re5+re6+re7+re8+re9+re10+re11,Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
	    // Matcher m = p.matcher(txt);

	}
	
	static String re1, re2, re3, re4, re5, re6, re7, re8, re9, re10, re11;
	*/

    private static final Map<String, String> DATE_FORMAT_REGEXPS = new HashMap<String, String>() {
    	/**
		 * 
		 */
		private static final long serialVersionUID = -2952238921890267819L;

	{
		//put(re1, "whatever");
		//put(re1+re2+re3+re4+re5+re6+re7+re8+re9+re10+re11, "EEE MMM dd HH:mm:ss zzz yyyy");
        //put("^\\d{8}$", "yyyyMMdd");
        //put("^\\d{8}", "yyyyMMdd");
		put("^[12]\\d{7}$", "yyyyMMdd");
		put("^[12]\\d{7}", "yyyyMMdd");
        put("^(1[0-2]|0[1-9])(3[01]|[12][0-9]|0[1-9])[0-9]{4}$", "MMddyyyy");
        put("^(3[01]|[12][0-9]|0[1-9])(1[0-2]|0[1-9])[0-9]{4}$", "ddMMyyyy");
        put("^\\d{1,2}-\\d{1,2}-\\d{4}$", "dd-MM-yyyy");
        put("^\\d{1,2}-[a-z]{3}-\\d{2}$", "dd-MMM-yy");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}$", "yyyy-MM-dd");
        put("^(3[01]|[12][0-9]|0[1-9])/(1[0-2]|0[1-9])/[0-9]{4}$", "dd/MM/yyyy");
        put("^\\d{1,2}/\\d{1,2}/\\d{4}$", "MM/dd/yyyy");
        put("^\\d{1,2}/\\d{1,2}/\\d{2}$", "MM/dd/yy");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}$", "yyyy/MM/dd");
        put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}$", "dd MMM yyyy");
        put("^\\d{1,2}[a-z]{3}\\d{4}$", "ddMMMyyyy");
        put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}$", "dd MMMM yyyy");
        put("^\\d{12}$", "yyyyMMddHHmm");
        put("^\\d{8}\\s\\d{4}$", "yyyyMMdd HHmm");
        put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}$", "dd-MM-yyyy HH:mm");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}$", "yyyy-MM-dd HH:mm");
        put("^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}$", "MM/dd/yyyy HH:mm");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}$", "yyyy/MM/dd HH:mm");
        put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", "dd MMM yyyy HH:mm");
        put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", "dd MMMM yyyy HH:mm");
        put("^\\d{14}$", "yyyyMMddHHmmss");
        put("^\\d{8}\\s\\d{6}$", "yyyyMMdd HHmmss");
        put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd-MM-yyyy HH:mm:ss");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss");
        put("^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "MM/dd/yyyy HH:mm:ss");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}$", "yyyy/MM/dd HH:mm:ss");
        put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd MMM yyyy HH:mm:ss");
        put("^\\d{1,2}[a-z]{3}\\d{4}:\\d{1,2}:\\d{2}:\\d{2}.\\d{3}$", "ddMMMyyyy:HH:mm:ss.SSS");
        put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd MMMM yyyy HH:mm:ss");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}.\\d{3}$", "yyyy/MM/dd HH:mm:ss.SSS");
        //put("^\\d{1,2}[a-z]{3}\\d{4}\\d{1,2}:\\d{2}:\\d{2}$", "ddMMM yyyy HH:mm:ss"); //01JAN19:00:00:00
    }};

    private DateUtil() {
        // Utility class, hide the constructor.
    }

    // Converters ---------------------------------------------------------------------------------

    /**
     * Convert the given date to a Calendar object. The TimeZone will be derived from the local
     * operating system's timezone.
     * @param date The date to be converted to Calendar.
     * @return The Calendar object set to the given date and using the local timezone.
     */
    public static Calendar toCalendar(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.setTime(date);
        return calendar;
    }

    /**
     * Convert the given date to a Calendar object with the given timezone.
     * @param date The date to be converted to Calendar.
     * @param timeZone The timezone to be set in the Calendar.
     * @return The Calendar object set to the given date and timezone.
     */
    public static Calendar toCalendar(Date date, TimeZone timeZone) {
        Calendar calendar = toCalendar(date);
        calendar.setTimeZone(timeZone);
        return calendar;
    }

    /**
     * Parse the given date string to date object and return a date instance based on the given
     * date string. This makes use of the {@link DateUtil#determineDateFormat(String)} to determine
     * the SimpleDateFormat pattern to be used for parsing.
     * @param dateString The date string to be parsed to date object.
     * @return The parsed date object.
     * @throws ParseException If the date format pattern of the given date string is unknown, or if
     * the given date string or its actual date is invalid based on the date format pattern.
     */
    public static Date parse(String dateString) throws ParseException {
        String dateFormat = determineDateFormat(dateString);
        if (dateFormat == null) {
            throw new ParseException("Unknown date format.", 0);
        }
        return parse(dateString, dateFormat);
    }

    /**
     * Validate the actual date of the given date string based on the given date format pattern and
     * return a date instance based on the given date string.
     * @param dateString The date string.
     * @param dateFormat The date format pattern which should respect the SimpleDateFormat rules.
     * @return The parsed date object.
     * @throws ParseException If the given date string or its actual date is invalid based on the
     * given date format pattern.
     * @see SimpleDateFormat
     */
    public static Date parse(String dateString, String dateFormat) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        simpleDateFormat.setLenient(false); // Don't automatically convert invalid date.
        return simpleDateFormat.parse(dateString);
    }

    // Validators ---------------------------------------------------------------------------------

    /**
     * Checks whether the actual date of the given date string is valid. This makes use of the
     * {@link DateUtil#determineDateFormat(String)} to determine the SimpleDateFormat pattern to be
     * used for parsing.
     * @param dateString The date string.
     * @return True if the actual date of the given date string is valid.
     */
    public static boolean isValidDate(String dateString) {
        try {
            parse(dateString);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    /**
     * Checks whether the actual date of the given date string is valid based on the given date
     * format pattern.
     * @param dateString The date string.
     * @param dateFormat The date format pattern which should respect the SimpleDateFormat rules.
     * @return True if the actual date of the given date string is valid based on the given date
     * format pattern.
     * @see SimpleDateFormat
     */
    public static boolean isValidDate(String dateString, String dateFormat) {
        try {
            parse(dateString, dateFormat);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    // Checkers -----------------------------------------------------------------------------------

    /**
     * Determine SimpleDateFormat pattern matching with the given date string. Returns null if
     * format is unknown. You can simply extend DateUtil with more formats if needed.
     * @param dateString The date string to determine the SimpleDateFormat pattern for.
     * @return The matching SimpleDateFormat pattern, or null if format is unknown.
     * @see SimpleDateFormat
     */
    public static String determineDateFormat(String dateString) {
        for (String regexp : DATE_FORMAT_REGEXPS.keySet()) {
            if (dateString.toLowerCase().matches(regexp)) {
                return DATE_FORMAT_REGEXPS.get(regexp);
            }
        }
        log.error("Could not determine date format for string: " + dateString);
        return null; // Unknown format.
    }
    
    
    
    private static Calendar Jan1_1960 = Calendar.getInstance();
    static {
    	Jan1_1960.set(Calendar.YEAR, 1960);
    	Jan1_1960.set(Calendar.MONTH, 1);
    	Jan1_1960.set(Calendar.DAY_OF_MONTH, 1);
    }
    
    public static Date getDateFromSAS9 (String s) {
    	try {
    		Integer iAdd = Integer.parseInt(s);
    		Calendar c = (Calendar) Jan1_1960.clone();
    		c.add(Calendar.DATE, iAdd);
    		return c.getTime();
    	}
    	catch (NumberFormatException e) {
    		return null;
    	}
    }
    
    
    /**
     * convert a known field
     * get to know an unknown field
     * @throws ParseException 
     */
    public static Date doParse (String sName, String sValue) throws ParseException {
    	Date d = null;
    	if (sValue == null  || sValue.trim().isEmpty())
    		{}
    	else {
    		if(!sdfList.containsKey(sName)) {
    			sdfList.put(sName, new SimpleDateFormat(DateUtil.determineDateFormat(sValue)));
    		}
    		if (sdfList.get(sName) == null)
    			log.error("Received unrecognizable date format for field " + sName + "'" + sValue + "'");
    		else
    			d = sdfList.get(sName).parse(sValue);
    	}
    	return d;
    }
    
    private static HashMap<String, SimpleDateFormat> sdfList = new HashMap<String, SimpleDateFormat>(); 
    
    public static void resetFieldFormatLookup ()  {
    	sdfList = new HashMap<String, SimpleDateFormat>();
    }
    
    
    public static String generateRunDate () {
    	// define the control run date
    	Date date = Calendar.getInstance().getTime();
    	DateFormat formatter = new SimpleDateFormat("yyyy_MM_dd_hhmm");
    	return formatter.format(date);
    }
    
    /**
     * Get the minutes difference
     */
    public static long minutesDiff( Date startDate, Date endDate )
    {
    	long duration  = endDate.getTime() - startDate.getTime();
    	return TimeUnit.MILLISECONDS.toMinutes(duration);
    }

    
    
    private static org.apache.log4j.Logger log = Logger.getLogger(DateUtil.class);
    
    public static void main(String[] args) throws ParseException
    {
    	String txt="9999-12-31";
    	System.out.println(determineDateFormat(txt));
    	System.out.println(doParse("checkDate", txt));
    }
    
 
}
