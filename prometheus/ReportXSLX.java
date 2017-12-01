



public class ReportXSLX extends ReportBase implements ReportInterface  {
	
	String filePath = "c:/temp/mysample.xls";
	XSSFWorkbook wb;
	Map<String, XSSFCellStyle> styles;

	Sheet  totalSheet;
	Sheet  svcClaimErrorSheet;
	Sheet  enrollmentErrorSheet;
	Sheet  memberErrorSheet;
	Sheet  providerErrorSheet;
	Sheet  rxClaimErrorSheet; 
	
	int maxErrorRows = 1000;
	int errorMsgWidth = 120;
	
	int ts_row = 0;
	int pcd_row = 0;
	int claim_row = 0;
	int enrollment_row = 0;
	int member_row = 0;
	int provider_row = 0;
	int rx_row = 0;
	
	Row row;
    Cell cell;
    CellStyle style;
    
	
	/**
	 * construct this class with invoker's parameters and start a workbook
	 * @param parameters
	 */
	public ReportXSLX(HashMap<String, String> parameters) {
		super(parameters);
		//filePath = parameters.get("workdirectory") == null ? "c:/temp/" : parameters.get("workdirectory"); 
		filePath = FileNamer.getFileName( parameters.get("runname") ) + ".xlsx"; 
		wb = new XSSFWorkbook();		// warning - I've seen debugger take a classNotFound exception loading XSSFWorkbook, but then find the class later
		styles = SpreadsheetHelper.createStyles(wb);
	}
	
	

	@Override
	public void createValidationReports(AllDataInterface di) {
		
		log.info("Starting Preparation of Validation Reports");
		
		initialize(di);
		processServiceClaims();
		

		totalSheet = wb.createSheet("Summary"); 
		svcClaimErrorSheet = wb.createSheet("Service Claim Errors");
		enrollmentErrorSheet = wb.createSheet("Enrollment Errors");
		memberErrorSheet = wb.createSheet("Member Errors");
		providerErrorSheet = wb.createSheet("Provider Errors");
		rxClaimErrorSheet = wb.createSheet("Rx Claim Errors"); 
		
		
		// initialize total sheet
		totalSheet.setColumnWidth(0, 30*256);
		totalSheet.setColumnWidth(1, 22*256);
		totalSheet.setColumnWidth(2, 22*256);
		totalSheet.setColumnWidth(3, 22*256);

		
		// Overall count section of total sheet
		ts_row = 0;
		row = totalSheet.createRow(ts_row);
		addTextCell(0, row, parameters.get("runname"), "title"); 
		addTextCell(1, row, today, "title"); 
		
		// Title row of total sheet
		ts_row++;
		row = totalSheet.createRow(ts_row);
		addTextCell(0, row, "Input Record Counts", "labelformat_left");  
		addTextCell(1, row, "Record Total", "labelformat_center");
		addTextCell(2, row, "Records Accepted", "labelformat_center");
		addTextCell(3, row, "Records Rejected", "labelformat_center");
		
		
		// total sheet input detail start
		// claims
		ts_row++;
		row = totalSheet.createRow(ts_row);
		addTextCell(0, row, "Claim Input Records", "labelformat_left");
		addNumberCell(1, row, di.getAllServiceClaimCounts().getRecordsRead(), "integer_normal");
		addNumberCell(2, row, di.getAllServiceClaimCounts().getRecordsAccepted(), "integer_normal");
		addNumberCell(3, row, di.getAllServiceClaimCounts().getRecordsRejected(), "integer_normal");
		// enrollments
		ts_row++;
		row = totalSheet.createRow(ts_row);
		addTextCell(0, row, "Enrollment Input Records", "labelformat_left");
		addNumberCell(1, row, di.getAllEnrollmentCounts().getRecordsRead(), "integer_normal");
		addNumberCell(2, row, di.getAllEnrollmentCounts().getRecordsAccepted(), "integer_normal");
		addNumberCell(3, row, di.getAllEnrollmentCounts().getRecordsRejected(), "integer_normal");
		// plan members
		ts_row++;
		row = totalSheet.createRow(ts_row);
		addTextCell(0, row, "Plan Member Input Records", "labelformat_left");
		addNumberCell(1, row, di.getAllPlanMemberCounts().getRecordsRead(), "integer_normal");
		addNumberCell(2, row, di.getAllPlanMemberCounts().getRecordsAccepted(), "integer_normal");
		addNumberCell(3, row, di.getAllPlanMemberCounts().getRecordsRejected(), "integer_normal");
		// providers
		ts_row++;
		row = totalSheet.createRow(ts_row);
		addTextCell(0, row, "Provider Input Records", "labelformat_left");
		addNumberCell(1, row, di.getAllProviderCounts().getRecordsRead(), "integer_normal");
		addNumberCell(2, row, di.getAllProviderCounts().getRecordsAccepted(), "integer_normal");
		addNumberCell(3, row, di.getAllProviderCounts().getRecordsRejected(), "integer_normal");
		// Rx
		ts_row++;
		row = totalSheet.createRow(ts_row);
		addTextCell(0, row, "Rx Claims Input Records", "labelformat_left");
		addNumberCell(1, row, di.getAllRxClaimCounts().getRecordsRead(), "integer_normal");
		addNumberCell(2, row, di.getAllRxClaimCounts().getRecordsAccepted(), "integer_normal");
		addNumberCell(3, row, di.getAllRxClaimCounts().getRecordsRejected(), "integer_normal");
		
		
		// claim error details sheet
		svcClaimErrorSheet.setColumnWidth(0, errorMsgWidth*256);
		claim_row = 0;
		for (String s : di.getAllServiceClaimErrors()) {
			row = svcClaimErrorSheet.createRow(claim_row);
			addTextCell(0, row, s, "normalformat");
			claim_row++;
			if (claim_row > maxErrorRows) {
				addTextCell(0, row, "Stopped at maximum error rows", "normalformat");
				break;
			}
		}
		
		// enrollment error details sheet
		enrollment_row = 0;
		enrollmentErrorSheet.setColumnWidth(0, errorMsgWidth*256);
		for (String s : di.getAllEnrollmentErrors()) {
			row = enrollmentErrorSheet.createRow(enrollment_row);
			addTextCell(0, row, s, "normalformat");
			enrollment_row++;
			if (enrollment_row > maxErrorRows) {
				addTextCell(0, row, "Stopped at maximum error rows", "normalformat");
				break;
			}
		}
		
		// plan member error details sheet
		member_row = 0;
		memberErrorSheet.setColumnWidth(0, errorMsgWidth*256);
		for (String s : di.getAllPlanMemberErrors()) {
			row = memberErrorSheet.createRow(member_row);
			addTextCell(0, row, s,"normalformat");
			member_row++;
			if (member_row > maxErrorRows) {
				addTextCell(0, row, "Stopped at maximum error rows", "normalformat");
				break;
			}
		}
		
		// provider error details sheet
		provider_row = 0;
		providerErrorSheet.setColumnWidth(0, errorMsgWidth*256);
		for (String s : di.getAllProviderErrors()) {
			row = providerErrorSheet.createRow(provider_row);
			addTextCell(0, row, s, "normalformat");
			provider_row++;
			if (provider_row > maxErrorRows) {
				addTextCell(0, row, "Stopped at maximum error rows", "normalformat");
				break;
			}
		}

		// Rx error details sheet
		rx_row = 0;
		rxClaimErrorSheet.setColumnWidth(0, errorMsgWidth*256);
		for (String s : di.getAllRxClaimErrors()) {
			row = rxClaimErrorSheet.createRow(rx_row);
			addTextCell(0, row, s, "normalformat");
			rx_row++;
			if (rx_row > maxErrorRows) {
				addTextCell(0, row, "Stopped at maximum error rows", "normalformat");
				break;
			}
		}

		
		
		// write out QC totals
		Date[] d = DateRange.getFirstAndLastDatesFromServiceClaims(svcLines);
		ts_row++;
		row = totalSheet.createRow(ts_row);
		addTextCell(0, row, "Earliest Claim Begin Date", "labelformat_left");
		addDateCell(1, row, d[0], "normal_date");
		ts_row++;
		row = totalSheet.createRow(ts_row);
		addTextCell(0, row, "Latest Claim End Date", "labelformat_left");
		addDateCell(1, row, d[1], "normal_date");
		ts_row++;
		row = totalSheet.createRow(ts_row);
		addTextCell(0, row, "Total Members with Claims", "labelformat_left");
		addNumberCell(1, row, total_members_with_claims, "integer_normal");
		
		ts_row++;
		ts_row++;
		row = totalSheet.createRow(ts_row);
		addTextCell(0, row, "Claim Type", "labelformat_left");
		addTextCell(1, row, "Total Unique Claims", "labelformat_center");
		addTextCell(2, row, "% of All Service Claims", "labelformat_center");
		addTextCell(3, row, "Average Claim Amount", "labelformat_center");
		ts_row++;
		row = totalSheet.createRow(ts_row);
		addTextCell(0, row, "All Inpatient Claims", "labelformat_left");
		addNumberCell(1, row, total_ip_claim_count, "integer_normal");
		addNumberCell(2, row, total_ip_claim_count / total_svc_claim_count * 100, "integer_normal");
		addNumberCell(3, row, total_ip_claim_amt / total_ip_claim_count , "currency_normal");
		ts_row++;
		row = totalSheet.createRow(ts_row);
		addTextCell(0, row, "All Outpatient Claims", "labelformat_left");
		addNumberCell(1, row, total_op_claim_count, "integer_normal");
		addNumberCell(2, row, total_op_claim_count / total_svc_claim_count * 100, "integer_normal");
		addNumberCell(3, row, total_op_claim_amt / total_op_claim_count , "currency_normal");
		ts_row++;
		row = totalSheet.createRow(ts_row);
		addTextCell(0, row, "All Professional Claims", "labelformat_left");
		addNumberCell(1, row, total_pb_claim_count, "integer_normal");
		addNumberCell(2, row, total_pb_claim_count / total_svc_claim_count * 100, "integer_normal");
		addNumberCell(3, row, total_pb_claim_amt / total_pb_claim_count , "currency_normal");
		ts_row++;
		row = totalSheet.createRow(ts_row);
		addTextCell(0, row, "All Service Claims", "labelformat_left");
		addNumberCell(1, row, total_svc_claim_count, "integer_subtotal");
		addTextCell(2, row, " ", "labelformat_left");
		addNumberCell(3, row, total_svc_claim_amt / total_svc_claim_count , "currency_subtotal");
		ts_row++;
		row = totalSheet.createRow(ts_row);
		addTextCell(0, row, "All Pharmacy Claims", "labelformat_left");
		addNumberCell(1, row, total_rx_claim_count, "integer_normal");
		addTextCell(2, row, "--", "labelformat_center");
		addNumberCell(3, row, total_rx_claim_amt / total_rx_claim_count , "currency_normal");
		
		// column by column input statistics
		ts_row++;
		doColumnarTotals(di.getServiceClaimStatistics(), di.getAllServiceClaimCounts().getRecordsRead(), "Claim Input Column ");
		doColumnarTotals(di.getRxClaimStatistics(), di.getAllRxClaimCounts().getRecordsRead(), "Rx Claim Input Column ");
		doColumnarTotals(di.getEnrollmentStatistics(), di.getAllEnrollmentCounts().getRecordsRead(), "Enrollment Column ");
		doColumnarTotals(di.getMemberStatistics(), di.getAllPlanMemberCounts().getRecordsRead(), "Member Input Column ");
		doColumnarTotals(di.getProviderStatistics(), di.getAllProviderCounts().getRecordsRead(), "Provider Input Column ");
        
		
        log.info("Completed Preparation of Validation Reports");
		
	}
	
	


	private void doColumnarTotals (InputStatistics stats, int iTot, String label) {
		ts_row++;
		ts_row++;
		row = totalSheet.createRow(ts_row);
		addTextCell(0, row, label, "labelformat_left");
		addTextCell(1, row, "Expected ?", "labelformat_center");
		addTextCell(2, row, "Percent Filled", "labelformat_center");
		Set<String> keys = stats.column_stats.keySet();
		for (String sK : keys) {
			ts_row++;
			row = totalSheet.createRow(ts_row);
			InputStatistics.StatEntry e = stats.column_stats.get(sK);
			addTextCell(0, row, e.col_name, "labelformat_left");
			addTextCell(1, row, (e.bExpected ? "Yes" : "No"), "normalformat");
			addNumberCell(2, row, e.filled_count / iTot * 100, "currency_normal");
		}
	}
	

	
	Sheet  patCostDistSheet; 

	
	@Override
	public void createPatientCostDistribution (AllDataInterface di) {
		
		log.info("Starting Patient Cost Distribution Report");
		
		// be sure the data is loaded
		initialize(di);
		processServiceClaims();
		
		// initialize patient cost distribution sheet
		patCostDistSheet = wb.createSheet("Patient Cost Distribution");
		patCostDistSheet.setColumnWidth(0, 15*256);
		patCostDistSheet.setColumnWidth(1, 20*256);
		patCostDistSheet.setColumnWidth(2, 20*256);
		patCostDistSheet.setColumnWidth(3, 20*256);
		patCostDistSheet.setColumnWidth(4, 20*256);
		patCostDistSheet.setColumnWidth(5, 20*256);
		patCostDistSheet.setColumnWidth(6, 20*256);
		patCostDistSheet.setColumnWidth(7, 20*256);
		patCostDistSheet.setColumnWidth(8, 20*256);
		patCostDistSheet.setColumnWidth(9, 20*256);
		row = patCostDistSheet.createRow(pcd_row);
		addTextCell(0, row, "Member Id", "labelformat_center");  
		addTextCell(1, row, "Total Cost", "labelformat_center");  
		addTextCell(2, row, "Number of PB Services", "labelformat_center");  
		addTextCell(3, row, "Average PB cost", "labelformat_center");  
		addTextCell(4, row, "Number of OP Services", "labelformat_center");  
		addTextCell(5, row, "Average OP cost", "labelformat_center");  
		addTextCell(6, row, "Number of IP Services", "labelformat_center");  
		addTextCell(7, row, "Average IP cost", "labelformat_center");  
		addTextCell(8, row, "Number of Rx Claims", "labelformat_center");  
		addTextCell(9, row, "Average Rx Cost", "labelformat_center");  
		pcd_row++;
		
		
		for (pcdSubTotal sub : pcdSubs) {
			row = patCostDistSheet.createRow(pcd_row);
			addTextCell(0, row, sub.member_id, "normalformat");
			addNumberCell(1, row, sub.ip_total_amt + sub.op_total_amt + sub.pb_total_amt + sub.rx_total_amt, "currency_normal");
			addNumberCell(2, row, sub.pb_total_cnt, "integer_normal");
			addNumberCell(3, row, Calculations.average(sub.pb_total_amt, sub.pb_total_cnt), "currency_normal");
			addNumberCell(4, row, sub.op_total_cnt, "integer_normal");
			addNumberCell(5, row, Calculations.average(sub.op_total_amt, sub.op_total_cnt), "currency_normal");
			addNumberCell(6, row, sub.ip_total_cnt, "integer_normal");
			addNumberCell(7, row, Calculations.average(sub.ip_total_amt, sub.ip_total_cnt), "currency_normal");
			addNumberCell(8, row, sub.rx_total_cnt, "integer_normal");
			addNumberCell(9, row, Calculations.average(sub.rx_total_amt, sub.rx_total_cnt), "currency_normal");
			pcd_row++;
			if (pcd_row == rowMax) {
				addTextCell(10, row, "Stopped at max rows", "mormalformat");
				break;
			}
		}
		
		log.info("Completed Patient Cost Distribution Report");
		
	}
	
	static final int rowMax = 1048575;

	
	Sheet  genDataSummarySheet; 
	int gds_row = 0;

	
	@Override
	public void createGeneralDataSummary(AllDataInterface di) {
		
		log.info("Starting General Data Summary");
		
		// be sure the data is loaded
		initialize(di);
		ArrayList<ClaimServLine> ipClaims = new ArrayList<ClaimServLine>();
		ArrayList<ClaimServLine> opClaims = new ArrayList<ClaimServLine>();
		ArrayList<ClaimServLine> pbClaims = new ArrayList<ClaimServLine>();
		processServiceClaims();
		
		for (ClaimServLine clm : svcLines) {
			try {
				switch (clm.getClaim_line_type_code()) {
				case "IP": 
					ipClaims.add(clm);
					break;
				case "OP": 
					opClaims.add(clm);
					break;
				case "PB": 
					pbClaims.add(clm);
					break;
				}
			}
			catch (NullPointerException e) {
				log.error("Encountered null claim type for claim: " + svcLines.indexOf(clm) + "|" + clm.getClaim_id());
			}
		}
		
		Date[] dIp, dOp, dPb, dRx;
		
		// initialize patient cost distribution sheet
		genDataSummarySheet = wb.createSheet("General Data Summary");
		genDataSummarySheet.setColumnWidth(0, 25*256);
		genDataSummarySheet.setColumnWidth(1, 25*256);
		genDataSummarySheet.setColumnWidth(2, 25*256);
		genDataSummarySheet.setColumnWidth(3, 25*256);
		genDataSummarySheet.setColumnWidth(4, 25*256);
		genDataSummarySheet.setColumnWidth(5, 25*256);
		genDataSummarySheet.setColumnWidth(6, 25*256);
		genDataSummarySheet.setColumnWidth(7, 20*256);
		genDataSummarySheet.setColumnWidth(8, 20*256);
		genDataSummarySheet.setColumnWidth(9, 20*256);
		row = genDataSummarySheet.createRow(gds_row);
		addTextCell(0, row, "Claim Type", "labelformat_center");  
		addTextCell(1, row, "Claim Date Range Start", "labelformat_center"); 
		addTextCell(2, row, "Claim Date Range End", "labelformat_center");
		addTextCell(3, row, "No of Unique Patients", "labelformat_center");  
		addTextCell(4, row, "No of Claim Lines", "labelformat_center");  
		addTextCell(5, row, "Total Allowed Amt", "labelformat_center");  
		addTextCell(6, row, "Avg Claim Allowed Amount", "labelformat_center");  
		addTextCell(7, row, "Avg Patient Allowed per Year", "labelformat_center");  
		addTextCell(8, row, "Benchmark Lower CI", "labelformat_center");  
		addTextCell(9, row, "Benchmark Upper CI", "labelformat_center");   
		gds_row++;
		
		// Professional Claims Row
		row = genDataSummarySheet.createRow(gds_row);
		dPb = DateRange.getFirstAndLastDatesFromServiceClaims(ipClaims);
		addTextCell(0, row, "Professional Claims", "labelformat_left");
		addDateCell(1, row, dPb[0], "normal_date");
		addDateCell(2, row, dPb[1], "normal_date");
		addNumberCell(3, row, total_members_with_pb, "integer_normal");
		addNumberCell(4, row, pbClaims.size(), "integer_normal");
		addNumberCell(5, row, total_pb_claim_amt, "currency_normal");
		addNumberCell(6, row, total_pb_claim_amt / total_pb_claim_count, "currency_normal");
		gds_row++;
		
		// Outpatient Claims Row
		row = genDataSummarySheet.createRow(gds_row);
		dOp = DateRange.getFirstAndLastDatesFromServiceClaims(opClaims);
		addTextCell(0, row, "Outpatient Facility", "labelformat_left");
		addDateCell(1, row, dOp[0], "normal_date");
		addDateCell(2, row, dOp[1], "normal_date");
		addNumberCell(3, row, total_members_with_op, "integer_normal");
		addNumberCell(4, row, opClaims.size(), "integer_normal");
		addNumberCell(5, row, total_op_claim_amt, "currency_normal");
		addNumberCell(6, row, total_op_claim_amt / total_op_claim_count, "currency_normal");
		gds_row++;
		
		// Inpatient Claims Row
		row = genDataSummarySheet.createRow(gds_row);
		dIp = DateRange.getFirstAndLastDatesFromServiceClaims(ipClaims);
		addTextCell(0, row, "Stay Claims", "labelformat_left");
		addDateCell(1, row, dIp[0], "normal_date");
		addDateCell(2, row, dIp[1], "normal_date");
		addNumberCell(3, row, total_members_with_ip, "integer_normal");
		addNumberCell(4, row, ipClaims.size(), "integer_normal");
		addNumberCell(5, row, total_ip_claim_amt, "currency_normal");
		addNumberCell(6, row, total_ip_claim_amt / total_ip_claim_count, "currency_normal");
		gds_row++;
		
		// Prescription Claims Row
		row = genDataSummarySheet.createRow(gds_row);
		dRx = DateRange.getFirstAndLastDatesFromRxClaims(rxClaims.values());
		addTextCell(0, row, "Rx Claims", "labelformat_left");
		addDateCell(1, row, dRx[0], "normal_date");
		addDateCell(2, row, dRx[1], "normal_date");
		addNumberCell(3, row, total_members_with_rx, "integer_normal");
		addNumberCell(4, row, rxClaims.size(), "integer_normal");
		addNumberCell(5, row, total_rx_claim_amt, "currency_normal");
		addNumberCell(6, row, total_rx_claim_amt / total_rx_claim_count, "currency_normal");
		gds_row++;
		
		// OP/IP/RX CLAIMS
		row = genDataSummarySheet.createRow(gds_row);
		Date dB = dIp[0] == null ? new Date() : dIp[0];
		if (dRx[0] != null && dRx[0].before(dB))
			dB = dRx[0];
		if (dOp[0] != null && dOp[0].before(dB))
			dB = dOp[0];
		addTextCell(0, row, "OP/IP/RX Claims", "labelformat_left");
		addDateCell(1, row, dB, "normal_date");
		dB = dIp[1] == null ? new Date() : dIp[1];
		if (dRx[1] != null && dRx[1].after(dB))
			dB = dRx[1];
		if (dOp[1] != null && dOp[1].after(dB))
			dB = dOp[1];
		addDateCell(2, row, dB, "normal_date");
		ArrayList<Object>tempAL = new ArrayList<>();
		tempAL.addAll(opClaims);
		tempAL.addAll(ipClaims);
		tempAL.addAll(rxClaims.values());
		addNumberCell(3, row, getUniquePatientCount(tempAL), "integer_normal");
		addNumberCell(4, row, rxClaims.size() + opClaims.size() + ipClaims.size(), "integer_normal");
		addNumberCell(5, row, total_rx_claim_amt + total_ip_claim_amt + total_op_claim_amt, "currency_normal");
		addNumberCell(6, row, (total_rx_claim_amt + total_ip_claim_amt + total_op_claim_amt) / (total_rx_claim_count + total_ip_claim_count + total_op_claim_count), "currency_normal");
		gds_row++;
				
		
		log.info("Completed General Data Summary");
		
	}
	
	private double getUniquePatientCount(ArrayList<Object> stuff)
	{
		HashMap<String, String>uniques = new HashMap<String, String>();
		for(Object o : stuff)
		{
			if (o instanceof ClaimServLine) {
				uniques.put(((ClaimServLine)o).getMember_id(), "");
			}
			else if (o instanceof ClaimRx) {
				uniques.put(((ClaimRx)o).getMember_id(), "");
			}
		}
		return uniques.size();
	}
	
	
	
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
	
	private void addDateCell(int col, Row row, Date value, String style) {
		//String s = "OK";
		try {
			//s = "1" + row.toString() + ":" + col + ":" + value + ":" + style;
			cell = row.createCell(col);
			//s = s.concat("2" + s.substring(1));
			cell.setCellValue(value);
			//s = s.concat("3" + s.substring(1));
			cell.setCellStyle(styles.get(style));
		}
		catch (NullPointerException e)
		{
			//log.error("NP on: " + s);
			addTextCell(col,row,"n/a",style);
		}
	}
	
	

	@Override
	public void finalize(AllDataInterface di) {

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
	

	
	private static org.apache.log4j.Logger log = Logger.getLogger(ReportXSLX.class);




}
