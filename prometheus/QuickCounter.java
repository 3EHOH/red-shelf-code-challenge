


public class QuickCounter {
	
	
	String filePath = "c:/temp/QuickCounter_CHC.xls";
	XSSFWorkbook wb;
	Map<String, XSSFCellStyle> styles;
	Sheet  totalSheet;
	
	static String today;
	
	
	public static void main(String[] args) {
		
		loadParameters(args);
		if((sFile = parameters.get("input")) == null)
			throw new IllegalStateException("Need input= parameter");
		
		Date date = Calendar.getInstance().getTime();
		DateFormat formatter = new SimpleDateFormat("yyyy_MM_dd_hhmm");
		today = formatter.format(date);

		QuickCounter q = new QuickCounter();
		q.process();
		
	}
	
	
	private void process () {
		
		FileReader inputFile = null;

		try {
			inputFile = new FileReader(sFile);
		} catch (FileNotFoundException e) {
			System.out.println("Failure occurred trying to read " + sFile);
		}
		
		BufferedReader br = new BufferedReader(inputFile); 
		String line;
		int count = 0;
		
		// read the lines and print
		try {
			while ((line = br.readLine()) != null )  {
				doCounts(line);
				count++;
			}
			br.close();

		} catch (IOException e) {
			System.out.println("An error occurred while reading " + sFile);
		}  
		
		log.info("Read " + count + " records");
		log.info("Found " + claimList.size() + " unique claim Ids");
		
		for(Entry<String, countCats> entry : counts.entrySet()){
		    log.info("Bill Type: " + entry.getKey() + " " + entry.getValue());
		}
		
		
		wb = new XSSFWorkbook();		// warning - I've seen debugger take a classNotFound exception loading XSSFWorkbook, but then find the class later
		styles = SpreadsheetHelper.createStyles(wb);
		totalSheet = wb.createSheet("Summary");
		int ts_row = 0;
		
		
		// Overall count section of total sheet
		ts_row = 0;
		row = totalSheet.createRow(ts_row);
		addTextCell(0, row, parameters.get("runname"), "title"); 
		addTextCell(1, row, today, "title"); 
		
		// Title row of total sheet
		ts_row++;
		row = totalSheet.createRow(ts_row);
		addTextCell(0, row, "Bill Type", "labelformat_left");  
		addTextCell(1, row, "Total Records", "labelformat_center");
		addTextCell(2, row, "Unique Claims", "labelformat_center");
		addTextCell(3, row, "Duplicate Claims", "labelformat_center");
		addTextCell(4, row, "Unique Line Items", "labelformat_center");
		addTextCell(5, row, "Duplicate Line Items", "labelformat_center");
		
		for(Entry<String, countCats> entry : counts.entrySet()) {
			ts_row++;
			row = totalSheet.createRow(ts_row);
			addTextCell(0, row, entry.getKey(), "labelformat_left");  
			addNumberCell(1, row, entry.getValue().countFound, "labelformat_center");
			addNumberCell(2, row, entry.getValue().countClaimUnique, "labelformat_center");
			addNumberCell(3, row, entry.getValue().countClaimDupes, "labelformat_center");
			addNumberCell(4, row, entry.getValue().countLineUnique, "labelformat_center");
			addNumberCell(5, row, entry.getValue().countLineDupes, "labelformat_center");
		}
				
		FileOutputStream fileOut;
		try {
			fileOut = new FileOutputStream(filePath);
			wb.write(fileOut);
			fileOut.close();
		} catch (FileNotFoundException e) {
			log.error(e);
			e.printStackTrace();
		} catch (IOException e) {
			log.error(e);
			e.printStackTrace();
		}
		
		
	}

	
	private void doCounts(String line) {
		
		in = line.split(sDelimiter);
		
		countCats c;
		
		if ( counts.containsKey(in[25])) {
			c = counts.get(in[25]);
			c.countFound++;
		}
		else {
			c = new countCats();
			c.countFound++;
			counts.put(in[25], c);
		}
		
		
		// unique claim id won't exist
		if ( claimList.containsKey(in[6]+in[2]) ) {
			c.countClaimDupes++;
		}
		else {
			claimList.put(in[6]+in[2], in[6]+in[2]);
			c.countClaimUnique++;
		}
		
		// unique claim id won't exist
		if ( claimLineList.containsKey(in[6]+in[2]+in[3]) ) {
			c.countLineDupes++;
		}
		else {
			claimLineList.put(in[6]+in[2]+in[3], in[6]+in[2]+in[3]);
			c.countLineUnique++;
		}
		
	}
	
	static Integer iC;
	static String[] in;
	
	static HashMap<String, countCats> counts = new HashMap<String, countCats>();
	static HashMap<String, String> claimList = new HashMap<String, String>();
	static HashMap<String, String> claimLineList = new HashMap<String, String>();

	static String sFile;
	
	static private void loadParameters (String[] args) {
		// load any default parameters from the default parameter array
		for (int i = 0; i < parameterDefaults.length; i++) {
			parameters.put(parameterDefaults[i][0], parameterDefaults[i][1]);
		}
		// overlay or add any incoming parameters
		for (int i = 0; i < args.length; i++) {
			parameters.put(args[i].substring(0, args[i].indexOf('=')), args[i].substring(args[i].indexOf('=')+1)) ;
		}	
	}
	
	static HashMap<String, String> parameters = new HashMap<String, String>();
	static String [][] parameterDefaults = {
			{"count", "99999999"}
	};
	
	public static final String sDelimiter = "\\*|\\||;";
	
	private static org.apache.log4j.Logger log = Logger.getLogger(QuickCounter.class);

	Row row;
    Cell cell;
    CellStyle style;

	private void addTextCell(int col, Row row, String text, String style) {
		cell = row.createCell(col);
        cell.setCellValue(text);
        cell.setCellStyle(styles.get(style));
	}
	

	private void addNumberCell(int col, Row row, double value, String style) {
		cell = row.createCell(col);
        cell.setCellValue(value);
        cell.setCellStyle(styles.get(style));
	}
	
	
	
	class countCats {
		int countFound = 0;
		int countClaimUnique = 0;
		int countClaimDupes = 0;
		int countLineUnique = 0;
		int countLineDupes = 0;
		
		public String toString() {
			return "Found: " + countFound + " Unique Claims: " + countClaimUnique + " Duplicate Claims " + countClaimDupes +
					" Unique Lines: " + countLineUnique + " Duplicate Lines: " + countLineDupes;
		}
	}
	
	

}
