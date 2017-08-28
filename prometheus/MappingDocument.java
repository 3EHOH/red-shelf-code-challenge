



public class MappingDocument {
	
	
	String filePath;
	String today;
	
	XSSFWorkbook wb;
	Map<String, XSSFCellStyle> styles;
	
	Row row;
    Cell cell;
    CellStyle style;
    
    Sheet  svcClaimSheet;
    Sheet  rxClaimSheet; 
    Sheet  memberSheet;
	Sheet  enrollmentSheet;
	Sheet  providerSheet;
	
	
    
	/**
	 * main process for input mapping document generation
	 * @throws IOException
	 */
	private void process () throws IOException {

		log.info("Starting Input Mapping Document Generation");
		
		
		Date date = Calendar.getInstance().getTime();
		DateFormat formatter = new SimpleDateFormat("yyyy_MM_dd_hhmm");
		today = formatter.format(date);
		
		filePath = parameters.get("workdirectory") == null ? "c:/temp/" : parameters.get("workdirectory"); 
		filePath = filePath + "MappingTemplate"  + today + ".xlsx"; 
		wb = new XSSFWorkbook();		// warning - I've seen debugger take a classNotFound exception loading XSSFWorkbook, but then find the class later
		styles = SpreadsheetHelper.createStyles(wb);
		
		
		svcClaimSheet = wb.createSheet("Service Claims");
		rxClaimSheet = wb.createSheet("Rx Claims");
		memberSheet = wb.createSheet("Members");
		enrollmentSheet = wb.createSheet("Enrollments");
		providerSheet = wb.createSheet("Providers");
		 
		// claim details sheet
		doSheetHeader(svcClaimSheet);
		writeMappingRows(new ClaimServLine(), svcClaimSheet);
		
		// Rx claim details sheet
		doSheetHeader(rxClaimSheet);
		writeMappingRows(new ClaimRx(), rxClaimSheet);
		
		// plan member details sheet
		doSheetHeader(memberSheet);
		writeMappingRows(new PlanMember(), memberSheet);
		
		// enrollment details sheet
		doSheetHeader(enrollmentSheet);
		writeMappingRows(new Enrollment(), enrollmentSheet);
		
		// provider details sheet
		doSheetHeader(providerSheet);
		writeMappingRows(new Provider(), providerSheet);



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
		
		log.info("Completed Input Document Generation");
		
	}
	
	
	
	private void doSheetHeader(Sheet sh) {
		
		sh.setColumnWidth(0, 30*256);
		sh.setColumnWidth(1, 30*256);
		sh.setColumnWidth(2, 40*256);
		sh.setColumnWidth(3, 20*256);
		sh.setColumnWidth(4, 20*256);
		sh.setColumnWidth(5, 60*256);
		
		row = sh.createRow(0);
		addTextCell(0, row, "Internal Field Name", "labelformat_left");
		addTextCell(1, row, "Source File", "labelformat_center");
		addTextCell(2, row, "Source Field", "labelformat_center");
		addTextCell(3, row, "Reference", "labelformat_center");
		addTextCell(4, row, "Source Usage", "labelformat_center");
		addTextCell(5, row, "Notes", "labelformat_center");
		
	}
	
	
	private void writeMappingRows(Object o, Sheet sh) {
		
		Field[] f =  o.getClass().getDeclaredFields();
		
		int row_idx = 1;
			
		for (int i=0; i < f.length; i++) {
			try {
				
				if (f[i].getName().equals("serialVersionUID"))		// don't bother with any serialization UID's
					continue;
				if (f[i].getName().equals("errMgr"))				// or the error managers
					continue;
				
				row_idx++;
				row = sh.createRow(row_idx);
				addTextCell(0, row, f[i].getName(), "labelformat_left");
				
			} 
			catch (IllegalArgumentException e) {
				// do nothing
			}
		}
		
				
	}
	

	private void addTextCell(int col, Row row, String text, String style) {
		cell = row.createCell(col);
        cell.setCellValue(text);
        cell.setCellStyle(styles.get(style));
	}

	
	

	public static void main(String[] args) throws IOException {
		
		MappingDocument instance = new MappingDocument();
		
		// get parameters (if any)
		instance.loadParameters(args);

		// process
		instance.process();

	}
	
	HashMap<String, String> parameters = RunParameters.parameters;
	String [][] parameterDefaults = {
			{"workdirectory", "C:\\Users\\" + System.getProperty("user.name") + "\\Dropbox (HCI3 Team)\\HCI3 Shared Folder\\"
					+ "Prometheus Operations\\Finalized Data Templates\\Excel Templates\\5. Mapping Templates\\"}
	};
	
	/**
	 * load default parameters and 
	 * put any run arguments in the hash map as well
	 * arguments should take the form keyword=value (e.g., studybegin=20140101)
	 * @param args
	 */
	private void loadParameters (String[] args) {
		// load any default parameters from the default parameter array
		for (int i = 0; i < parameterDefaults.length; i++) {
			parameters.put(parameterDefaults[i][0], parameterDefaults[i][1]);
		}
		// overlay or add any incoming parameters
		for (int i = 0; i < args.length; i++) {
			parameters.put(args[i].substring(0, args[i].indexOf('=')), args[i].substring(args[i].indexOf('=')+1)) ;
		}
		
		
	}
	
	
	DateFormat df1 = new SimpleDateFormat("yyyyMMdd");
	DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
	
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(MappingDocument.class);



}
