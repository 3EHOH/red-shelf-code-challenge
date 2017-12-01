


public class SpreadsheetHelper {
	
	
	public static ArrayList<Date> getMonthsForRange (Date start, Date end)
	{
		ArrayList<Date> dList = new ArrayList<Date>();
		if (start.before(end))
		{
			Calendar cX = Calendar.getInstance();
			cX.setTime(end);
			Calendar cS = Calendar.getInstance();
			cS.setTime(start);
			cS.set(Calendar.DAY_OF_MONTH,
					cS.getActualMinimum(Calendar.DAY_OF_MONTH));
			cS.set(Calendar.HOUR_OF_DAY, 0);
			cS.set(Calendar.MINUTE, 0);
			cS.set(Calendar.SECOND, 0);
			cS.set(Calendar.MILLISECOND, 0);
		    while (cS.before(cX))
		    {
		    	dList.add(cS.getTime());
		    	cS.add(Calendar.MONDAY, 1);
		    }
		    dList.add(cS.getTime());
		}
		return dList;
	}
	
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		ArrayList<Date> x = SpreadsheetHelper.getMonthsForRange(new Date(111, 0, 15), new Date());
		for (Date d : x) {
			log.info(d);
		}
	}
	
	 /**
     * cell styles used for formatting XSSF work sheets
     */
    public static Map<String, XSSFCellStyle> createStyles(XSSFWorkbook wb) {
    	
        Map<String, XSSFCellStyle> styles = new HashMap<String, XSSFCellStyle>();

        XSSFCellStyle style;
        short df;
        XSSFFont titleFont = wb.createFont();
        titleFont.setFontHeightInPoints((short)14);
        titleFont.setColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        style = wb.createCellStyle();
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setVerticalAlignment(VerticalAlignment.CENTER);
        style.setFont(titleFont);
        style.setFillBackgroundColor(new XSSFColor(new java.awt.Color(200, 251, 89)));
        styles.put("title", style);

        XSSFFont monthFont = wb.createFont();
        monthFont.setFontHeightInPoints((short)12);
        monthFont.setColor(new XSSFColor(new java.awt.Color(255, 255, 255)));
        monthFont.setBold(true);
        style = wb.createCellStyle();
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setVerticalAlignment(VerticalAlignment.CENTER);
        style.setFillForegroundColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setFont(monthFont);
        styles.put("month", style);

        XSSFFont lblFont = wb.createFont();
        lblFont.setFontHeightInPoints((short)12);
        lblFont.setBold(true);
        
        style = wb.createCellStyle();
        style.setAlignment(HorizontalAlignment.LEFT);
        style.setVerticalAlignment(VerticalAlignment.TOP);
        style.setFillForegroundColor(new XSSFColor(new java.awt.Color(228, 232, 243)));
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setBorderTop(BorderStyle.MEDIUM);
        style.setTopBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        style.setBorderLeft(BorderStyle.THIN);
        style.setLeftBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        style.setBorderRight(BorderStyle.THIN);
        style.setRightBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        style.setBorderBottom(BorderStyle.THIN);
        style.setBottomBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        style.setFont(lblFont);
        styles.put("labelformat_left", style);
      
        style = wb.createCellStyle();
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setVerticalAlignment(VerticalAlignment.TOP);
        style.setFillForegroundColor(new XSSFColor(new java.awt.Color(228, 232, 243)));
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setBorderTop(BorderStyle.MEDIUM);
        style.setTopBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        style.setBorderLeft(BorderStyle.THIN);
        style.setLeftBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        style.setBorderRight(BorderStyle.THIN);
        style.setRightBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        style.setBorderBottom(BorderStyle.THIN);
        style.setBottomBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        style.setFont(lblFont);
        styles.put("labelformat_center", style);

        style = wb.createCellStyle();
        df = wb.createDataFormat().getFormat("0");
        style.setDataFormat(df);
        style.setAlignment(HorizontalAlignment.RIGHT);
        style.setVerticalAlignment(VerticalAlignment.TOP);
        style.setFillForegroundColor(new XSSFColor(new java.awt.Color(255, 255, 255)));
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setBorderRight(BorderStyle.THIN);
        style.setRightBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        style.setBorderBottom(BorderStyle.THIN);
        style.setBottomBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        styles.put("integer_normal", style);
        
        XSSFFont lblSingleUnderline = wb.createFont();
        lblSingleUnderline.setFontHeightInPoints((short)11);
        lblSingleUnderline.setUnderline(FontUnderline.SINGLE_ACCOUNTING);
        
        style = (XSSFCellStyle) style.clone();
        style.setFont(lblSingleUnderline);
        styles.put("integer_subtotal", style);

        style = wb.createCellStyle();
        df = wb.createDataFormat().getFormat("#,##0.00");
        style.setDataFormat(df);
        style.setAlignment(HorizontalAlignment.RIGHT);
        style.setVerticalAlignment(VerticalAlignment.TOP);
        style.setFillForegroundColor(new XSSFColor(new java.awt.Color(255, 255, 255)));
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setBorderRight(BorderStyle.THIN);
        style.setRightBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        style.setBorderBottom(BorderStyle.THIN);
        style.setBottomBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        styles.put("currency_normal", style);
        
        style = (XSSFCellStyle) style.clone();
        style.setFont(lblSingleUnderline);
        styles.put("currency_subtotal", style);

        style = wb.createCellStyle();
        style.setBorderLeft(BorderStyle.THIN);
        style.setFillForegroundColor(new XSSFColor(new java.awt.Color(234, 234, 234)));
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setBorderBottom(BorderStyle.THIN);
        style.setBottomBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        styles.put("grey_left", style);

        style = wb.createCellStyle();
        style.setFillForegroundColor(new XSSFColor(new java.awt.Color(234, 234, 234)));
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setBorderRight(BorderStyle.THIN);
        style.setRightBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        style.setBorderBottom(BorderStyle.THIN);
        style.setBottomBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        styles.put("grey_right", style);
        
        style = wb.createCellStyle();
        df = wb.createDataFormat().getFormat("YYYY-mm-dd");
        style.setDataFormat(df);
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setBorderLeft(BorderStyle.THIN);
        style.setLeftBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        style.setBorderRight(BorderStyle.THIN);
        style.setRightBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        style.setBorderBottom(BorderStyle.THIN);
        style.setBottomBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        styles.put("normal_date", style);
        
        style = wb.createCellStyle();
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setBorderLeft(BorderStyle.THIN);
        style.setLeftBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        style.setBorderRight(BorderStyle.THIN);
        style.setRightBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        style.setBorderBottom(BorderStyle.THIN);
        style.setBottomBorderColor(new XSSFColor(new java.awt.Color(39, 51, 89)));
        styles.put("normalformat", style);

        return styles;
    }

    private static org.apache.log4j.Logger log = Logger.getLogger(SpreadsheetHelper.class);

}
