




/*import com.mongodb.BasicDBObject;


public class BillTypeReport {
	
	
	HashMap<String, String> parameters;
	
	
	Mongo mongoClient;
	DB db;
	DBCursor cursor;
	BasicDBObject query;
	DBCollection coll;
	DBCollection collRx;
	
	String schemaName=null;
	String collName=null;
	String jobUid=null;
	String collNameRx=null;
	
	MongoInputCollectionSet micky;
	Report rpt = new Report();
	AggregationOutput output = null;
	int billTypeSize=0;
	
	boolean debugMode = false;
	String verifyMsg = null;
	
	boolean hasBillType = false;
	boolean hasClaimType = false;
	boolean hasChargeAmt = false;
	boolean hasAllowedAmt = false;
	boolean hasNpi = false;
	boolean hasProviderId = false;
	boolean hasDrg = false;
	boolean hasRxChargeAmt = false;
	boolean hasRxAllowedAmt = false;

	
	static final String BILLTYPEFIELD ="type_of_bill";
	static final String CLAIMTYPEFIELD ="claim_line_type_code";
	static final String ALLOWEDAMTFIELD="allowed_amt";
	static final String CHARGEAMTFIELD="charge_amt";
	static final String NPIFIELD="provider_npi";
	static final String PROVIDERIDFIELD="provider_id";
	static final String DRGFIELD="ms_drg_code";
	/**
	 * static parameter constructor, just for testing
	 */
	public BillTypeReport() {	}
	
	/**
	 * constructor using parameters pulled from control database
	 * @param parameters
	 */
	public BillTypeReport(HashMap<String, String> parameters) {
		this.parameters = parameters;
	}
	
	
	private void process () {
		
		initialize();
		if(debugMode){
			System.out.println("init done!");
		}
		
		verifyMsg = dataVerification();
		if(debugMode){
			System.out.println("dataVerification done! verifyMsg="+verifyMsg);
		}
		
		if(verifyMsg == null){
			if(hasBillType){
				generateReport(null);
			}else{
				if(hasClaimType){
					generateReport(null);
					generateReport("RX");
				}
			}
			if(debugMode){
				System.out.println("generateReport done!");
			}
		}
		
		storeReport();
		if(debugMode){
			System.out.println("storeReport done!");
		}
		
	}
	
	
	private String retrieveBTDesc(String billType){
		String btDesc = billType;
		 String [][] btDef = { //cp frm common.util.BillTypeManager
			{"11x",	"IP",	"Hospital - Inpatient",	"ST"},
			{"12x",	"PB",	"Hospital - Inpatient (Part B only)", ""},	
			{"13x",	"OP",	"Hospital - Outpatient", ""},	
			{"14x",	"OP",	"Hospital - Other/Home Health", ""},	
			{"15x",	"IP",	"Hospital - Nursing Facility Level I",	"LT"},
			{"16x",	"IP",	"Hospital - Nursing Facility Level II",	"LT"},
			{"17x",	"IP",	"Hospital - Intermediate Care - Level III Nursing Facility", "LT"},
			{"18x",	"IP",	"Hospital - Swing Beds",	"LT"},
			{"20x", "PB",	"SNF",	"SNF"},
			{"21x",	"IP",	"SNF - Inpatient",	"SNF"},
			{"22x",	"PB",	"SNF - Inpatient (Part B)", ""},	
			{"23x",	"OP",	"SNF - Outpatient", ""},	
			{"24x",	"OP",	"SNF - Other/Home Health", ""},	
			{"25x",	"IP",	"SNF - Nursing Facility Level I",	"SNF"},
			{"26x",	"IP",	"SNF - Nursing Facility Level II",	"SNF"},
			{"27x",	"IP",	"SNF - Intermediate Care - Level III Nursing Facility",	"SNF"},
			{"28x",	"IP",	"SNF - Swing Beds",	"SNF"},
			{"31x",	"IP",	"Home Health - Inpatient",	"LT"},
			{"32x",	"PB",	"Home Health - Inpatient (Part B)", ""},	
			{"33x",	"OP",	"Home Health - Outpatient",	""},
			{"34x",	"OP",	"Home Health - Other/Home Health",	""},
			{"35x",	"IP",	"Home Health - Nursing Facility Level I",	"LT"},
			{"36x",	"IP",	"Home Health - Nursing Facility Level II",	"LT"},
			{"37x",	"IP",	"Home Health - Intermediate Care - Level III Nursing Facility",	"LT"},
			{"38x",	"IP",	"Home Health - Swing Beds",	"LT"},
			{"41x", "IP",	"Christian Science Hospital - Inpatient",	"ST"},
			{"42x",	"PB",	"Christian Science Hospital - Inpatient (Part B)", ""},	
			{"43x",	"OP",	"Christian Science Hospital - Outpatient", ""},	
			{"44x",	"OP",	"Christian Science Hospital - Other/Home Health", ""},	
			{"45x",	"IP",	"Christian Science Hospital - Nursing Facility Level I",	"LT"},
			{"46x",	"IP",	"Christian Science Hospital - Nursing Facility Level II",	"LT"},
			{"47x",	"IP",	"Christian Science Hospital - Intermediate Care - Level III Nursing Facility",	"LT"},
			{"48x",	"IP",	"Christian Science Hospital - Swing Beds",	"LT"},
			{"51x",	"IP",	"Christian Science Extended Care - Inpatient",	"LT"},
			{"52x",	"PB",	"Christian Science Extended Care - Inpatient (Part B)", ""},	
			{"53x",	"OP",	"Christian Science Extended Care - Outpatient", ""},		
			{"54x",	"OP",	"Christian Science Extended Care - Other/Home Health", ""},	
			{"55x", "IP",	"Christian Science Extended Care - Nursing Facility Level I",	"LT"},
			{"56x",	"IP",	"Christian Science Extended Care - Nursing Facility Level II",	"LT"},
			{"57x",	"IP",	"Christian Science Extended Care - Intermediate Care - Level III Nursing Facility",	"LT"},
			{"58x",	"IP",	"Christian Science Extended Care - Swing Beds",	"LT"},
			{"61x",	"IP",	"Intermediate Care - Inpatient",	"LT"},
			{"62x",	"PB",	"Intermediate Care - Inpatient (Part B)", ""},	
			{"63x",	"OP",	"Intermediate Care - Outpatient", ""},	
			{"64x",	"OP",	"Intermediate Care - Other/Home Health", ""},	
			{"65x",	"IP",	"Intermediate Care - Nursing Facility Level I",	"LT"},
			{"66x",	"IP",	"Intermediate Care - Nursing Facility Level II",	"LT"},
			{"67x",	"IP",	"Intermediate Care - Intermediate Care - Level III Nursing Facility",	"LT"},
			{"68x",	"IP",	"Intermediate Care - Swing Beds",	"LT"},
			{"71x",	"OP",	"Clinic - Rural Health",	""},	
			{"72x",	"OP",	"Clinic - Hospital Based or Independent Renal Dialysis Center",	""},	
			{"73x",	"OP",	"Clinic - Free Standing Outpatient Rehab Facility",	""},	
			{"74x", "OP",	"Clinic - Outpatient Rehab Facility", ""},
			{"75x",	"OP",	"Clinic - Comprehensive Outpatient Rehab Facility",	""},
			{"76x",	"OP",	"Clinic - Community Mental Health Center",	""},
			{"77x", "OP",	"Clinic - Federally Qualified Health Center", ""},
			{"78x", "OP",	"Licensed Free Standing Emergency Medical Facility", ""},	
			{"79x",	"OP",	"Clinic - Other",	""},
			{"81x",	"IP",	"Special Facility - Hospice, Non-hospital based",	"LT"},
			{"82x",	"IP",	"Special Facility - Hospice, Hospital based",	"LT"},
			{"83x",	"OP",	"Special Facility - Ambulatory Surgery Center",	""},	
			{"84x",	"OP",	"Special Facility - Free Standing Birthing Center",	""},	
			{"85x", "OP",	"Special Facility - Critical Access Hospital", ""},
			{"86x", "OP",	"Special Facility - Residential Facility", ""},
			{"87x", "PB",	"Special Facility - Unknown Type", ""},
			{"88x", "PB",	"Special Facility - Unknown Type", ""},
			{"89x",	"OP",	"Special Facility - Other",	""},
			{"90x",	"PB",	"Unknown",	""},
			{"91x",	"PB",	"Unknown",	""},
			{"92x",	"PB",	"Unknown",	""},
			{"93x",	"PB",	"Unknown",	""},
			{"94x",	"PB",	"Unknown",	""},
			{"95x",	"PB",	"Unknown",	""},
			{"96x",	"PB",	"Unknown",	""},
			{"97x",	"PB",	"Unknown",	""},
			{"98x",	"PB",	"Unknown",	""},
			{"99x",	"PB",	"Unknown",	""}
		};
		
		int btLen = btDef.length;
		if(billType !=null && !billType.equals("") && billType.length()>2){
			for (int i=0; i < btLen; i++) {
				if(btDef[i][0].substring(0, 2).equalsIgnoreCase(billType.substring(0,2))){
					btDesc =billType+"-"+btDef[i][1]+"-"+btDef[i][2];
					break;
				}
						
			}
		}
		
		return btDesc;

	}

	
	private boolean fieldExist(String fieldName, boolean isRx) {
	boolean fieldExist = false;

	
	BasicDBObject query = new BasicDBObject(); 
	BasicDBObject fields=null;
	DBCursor curs = null;
	int cnt = 0;
	String reqField=null;
	int nums=10;
	if(!fieldExist){
		fields = new BasicDBObject(fieldName,true).append("_id",false);;
		if(!isRx){
			curs = coll.find(query, fields);
		}else{
			curs = collRx.find(query, fields);
		}
		cnt = 0;
		reqField = null;
		while(curs.hasNext()) {
			   DBObject o = curs.next();
			   reqField =(String)o.get(fieldName);
			   if(reqField !=null){
				   fieldExist = true;
				   break;
			   }
			   cnt++;
			   if(cnt>nums){
				   break;
			   }
		}
	}
	return fieldExist;
	}

	private String dataVerification() {

		String errorMsg = null;
		
		hasBillType = false;
		hasBillType = fieldExist(BILLTYPEFIELD, false);
		
		hasClaimType = false;
		if(!hasBillType){//bilType not found, then check cliam_line_type_code
			hasClaimType = fieldExist(CLAIMTYPEFIELD, false);
		}
		
		if(!hasBillType && !hasClaimType){
			errorMsg ="Both "+BILLTYPEFIELD+" and "+CLAIMTYPEFIELD+" are missing, can not run the report!";
			return errorMsg;
		}

		//check charge_amt field
		hasChargeAmt = fieldExist(CHARGEAMTFIELD, false);
		//if not found charge_amt, check allowed_amt
		hasAllowedAmt = false;
		if(!hasChargeAmt){
			hasAllowedAmt=fieldExist(ALLOWEDAMTFIELD, false);
		}

		//check charge_amt field RX
		hasRxChargeAmt = fieldExist(CHARGEAMTFIELD, true);
		//if not found charge_amt, check allowed_amt
		hasRxAllowedAmt=fieldExist(ALLOWEDAMTFIELD, true);
		
		
		hasNpi = fieldExist(NPIFIELD, false);
		hasProviderId = fieldExist(PROVIDERIDFIELD, false);
		hasDrg = fieldExist(DRGFIELD, false);
		
		if(!hasChargeAmt && !hasAllowedAmt){
			errorMsg="Both Charge Amt and Allowed Amt are missing, can not run the report!";
			return errorMsg;
		}
		if(debugMode){
			System.out.println("BType="+hasBillType+" CType="+hasClaimType+" Charge="+hasChargeAmt+" Allow="+hasAllowedAmt+" Npi="+hasNpi+" pId="+hasProviderId+" drg="+hasDrg);
		}
		return errorMsg;
	}
		
	
	
	
	
	
	@SuppressWarnings("deprecation")
	private void generateReport(String claimType) {

		if(debugMode){
			System.out.println("generateReport colNM="+collName);
		}
		
		rpt.medicalClaimsTotalRecords = coll.getCount();
		if(debugMode){
			System.out.println("getMedicalClaimValues collection recCnt="+rpt.medicalClaimsTotalRecords);
		}
		
		boolean isRx = false;
		
		//build qury
		DBObject groupFields = null;
		if(claimType == null){
			if(hasBillType){
				groupFields = new BasicDBObject( "_id", "$type_of_bill");
			}else{
				groupFields = new BasicDBObject( "_id", "$claim_line_type_code");
			}
		}else{
			groupFields = new BasicDBObject( "_id", "$claim_line_type_code");
			coll = collRx;
			isRx = true;
			
		}
		
	    groupFields.put("count", new BasicDBObject( "$sum", 1));
	    if(hasNpi){
	    	groupFields.put(
	            "countNPI", new BasicDBObject(
	                "$sum", new BasicDBObject(
	                    "$cond", new Object[]{
	                        new BasicDBObject(
	                            "$eq", new Object[]{ "$provider_npi", ""}),0,1}
	                )));
	    }
	    if(hasDrg){
	    	groupFields.put(
	            "countDRG", new BasicDBObject(
	                "$sum", new BasicDBObject(
	                    "$cond", new Object[]{
	                        new BasicDBObject(
	                            "$eq", new Object[]{ "$ms_drg_code", ""}),0,1}
	                )));
	    }
	    if(hasProviderId){
	    	groupFields.put(
	            "countProvID", new BasicDBObject(
	                "$sum", new BasicDBObject(
	                    "$cond", new Object[]{
	                        new BasicDBObject(
	                            "$eq", new Object[]{ "provider_id", ""}),0,1}
	                )));
	    }
	    
		DBObject group = new BasicDBObject("$group", groupFields );


		DBObject sortFields = new BasicDBObject("_id", 1);
	    DBObject sort = new BasicDBObject("$sort", sortFields );
	    if(debugMode){
	    	System.out.println("start aggregate count group="+group);
	    }

		if(claimType == null){
			output = coll.aggregate(group, sort);
		}else{
			output = collRx.aggregate(group, sort);
		}
	    if(debugMode){
	    	System.out.println("after aggregate count");
	    }

	    int idx = billTypeSize;
	    for (DBObject obj : output.results()) {
            String billType = (String)obj.get("_id");
            
    		rpt.recTotal[idx] =Long.parseLong(obj.get("count").toString());
    		rpt.recTotalPerc[idx] = (float)rpt.recTotal[idx] / rpt.medicalClaimsTotalRecords *100;
    		
    		if(hasNpi){
    			rpt.recNPITotal[idx] =Long.parseLong(obj.get("countNPI").toString());
    		}
    		if(hasDrg){
    			rpt.recDRGTotal[idx] =Long.parseLong(obj.get("countDRG").toString());
    		}
    		if(hasProviderId){
    			rpt.recPIDTotal[idx] =Long.parseLong(obj.get("countProvID").toString());
    		}

/*old    		rpt.recNPITotalPerc[idx] =(float)rpt.recNPITotal[idx] / rpt.medicalClaimsTotalRecords *100;
    		rpt.recDRGTotalPerc[idx] =(float)rpt.recDRGTotal[idx] / rpt.medicalClaimsTotalRecords *100;
    		rpt.recPIDTotalPerc[idx] =(float)rpt.recPIDTotal[idx] / rpt.medicalClaimsTotalRecords *100;*/

    		rpt.recNPITotalPerc[idx] =(float)rpt.recNPITotal[idx] / rpt.recTotal[idx] *100;
    		rpt.recDRGTotalPerc[idx] =(float)rpt.recDRGTotal[idx] / rpt.recTotal[idx] *100;
    		rpt.recPIDTotalPerc[idx] =(float)rpt.recPIDTotal[idx] / rpt.recTotal[idx] *100;
    		
    		
    		
    		rpt.medicalClaimsTotalChargeAmt =new BigDecimal("0.00");
    		calculateTotalCost(billType, isRx);
    		rpt.medicalClaimsTotalCosts[idx]=rpt.medicalClaimsTotalChargeAmt;
             if(billType == null || billType.equals("") || billType.equals(" ")){
                	billType="Null/blank/unknown";
             }
     		 rpt.billTypes[idx]=billType;
     		 
     		 if(hasBillType){
     			 rpt.billTypesDesc[idx]=retrieveBTDesc(billType);
     		 } else{
     			 rpt.billTypesDesc[idx]=billType;
     		 }
     		 idx++;
//     		 if(idx>5){     			 break;     		 }
            	
	    };	    
	    billTypeSize = idx;    

	    if(debugMode){
	    	System.out.println("Complete aggregate count billTypeSize="+billTypeSize+" maxSize="+rpt.maxSize);
	    }
	    
//		log.info(getStatsAsHTML());
		
		
	}

	
	
	
	private void calculateTotalCost(String billType, boolean isRx) {

		String amtField = null;
		String amtValue = "";
		double amtValD = 0;

		String groupBy=null;
		if(hasChargeAmt){
			amtField = "charge_amt";
		}else{
			amtField = "allowed_amt";
		}
		if(isRx){
			if(hasRxAllowedAmt){
				amtField = "allowed_amt";
			}else{
				amtField = "charge_amt";
			}
		}
		
		if(hasBillType){
			groupBy=BILLTYPEFIELD;
		}else{
			groupBy=CLAIMTYPEFIELD;
		}
		if(debugMode){
			System.out.println("billType="+billType+" groupBy="+groupBy+" amt="+amtField);
		}
		//build qury
		BasicDBObject query = new BasicDBObject();	
		List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
		obj.add(new BasicDBObject(groupBy, billType));
		obj.add(new BasicDBObject(amtField, new BasicDBObject("$ne", "0.0")));
		query.put("$and", obj);
		
		BasicDBObject fields = new BasicDBObject();
		fields.put(groupBy,1);
		fields.put(amtField,1);
     
		DBCursor curs = null;
		if("RX".equalsIgnoreCase(billType)) {
			curs = collRx.find(query, fields);
		}else{
			curs = coll.find(query, fields);
		}
		rpt.medicalClaimsTotalChargeAmt =new BigDecimal("0.00");
		while(curs.hasNext()) {
 		   DBObject costObj = curs.next();
 		   if(costObj !=null && costObj.get(amtField) != null){
 			   amtValue = (String) costObj.get(amtField);
 			   if(amtValue != null){
 				   amtValD = Double.parseDouble(amtValue.replace("$", ""));
 		 		   rpt.medicalClaimsTotalChargeAmt = rpt.medicalClaimsTotalChargeAmt.add(BigDecimal.valueOf(amtValD));
 			   }
 		   }
        }

		if(debugMode){
			System.out.println("Complete calculateTotalCost for "+billType+" amtValDTotal="+rpt.medicalClaimsTotalChargeAmt);
		}
	
	}
	
	
	
	private void storeReport() {
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
        cList.add(ProcessReport.class);
		
		String env = parameters.get("env") == null ?  "prd" :  parameters.get("env");
        
        HibernateHelper h = new HibernateHelper(cList, env, schemaName);
        SessionFactory factory = h.getFactory(env, schemaName);

        Session session = factory.openSession();
        session.setCacheMode(CacheMode.IGNORE);
        session.setFlushMode(FlushMode.COMMIT);
        Transaction tx = null;
        
        
        try{
			tx = session.beginTransaction();
			
			String hql = "FROM ProcessReport WHERE jobuid = :jobuid AND stepname = '" + ProcessJobStep.STEP_POSTMAP_REPORT + "' and reportName='"+rpt.reportName+"'";
					
			Query query = session.createQuery(hql).setLockOptions(new LockOptions(LockMode.PESSIMISTIC_WRITE));
//			query.setParameter("jobuid", parameters.get("jobuid"));
			query.setParameter("jobuid", jobUid);

			@SuppressWarnings("unchecked")
			List<ProcessReport> results = query.list();
			ProcessReport r;
			if (results.isEmpty()) {
				r = new ProcessReport();
//				r.setJobUid(Long.parseLong(parameters.get("jobuid")));
				r.setJobUid(Long.parseLong(jobUid));
				r.setStepName(ProcessJobStep.STEP_POSTMAP_REPORT);
				r.setReportName(rpt.reportName);
			} else
				r = results.get(0);
			r.setReport(new SerialBlob(getStatsAsHTML().getBytes()));
			
			session.save(r);
			
			tx.commit();
			
		}catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		} catch (SerialException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			session.close(); 

			try {
				h.close("prd", schemaName);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
        
	}

	/**
	 * gets the statistics for the raw input file and formats them into a html page
	 * @return
	 */
	private String getStatsAsHTML ()  {
		
		StringBuffer sb = new StringBuffer();
		
		sb.append("<table  class=\"sortable\" border=\"1\" cellspacing=\"1\" cellpadding=\"5\" width=\"900px\" >");
		sb.append("<tr  bgcolor=\"#99CCFF\">");
		sb.append("<th width=\"230px\">Type of Bill</th>");
		sb.append("<th width=\"100px\">Total Number of Records</th>");
		sb.append("<th width=\"100px\">% of Records with BillType</th>");
		sb.append("<th width=\"100px\">% of Records with NPI Populated</th>");
		sb.append("<th width=\"100px\">% of records with DRG</th>");
		sb.append("<th width=\"100px\">% of Records with Provider ID Populated</th>");
		sb.append("<th width=\"170px\">Total Cost</th>");
		sb.append("</tr>");

		if(verifyMsg != null){
			sb.append("<tr><td colspan='7'>"+verifyMsg+"</td</tr></table>");
			return sb.toString();
			
		}
		for(int idx =0; idx<billTypeSize; idx++){
    		sb.append("<tr>");
	    	sb.append("<td align=\"right\">" + rpt.billTypesDesc[idx] + "</td>"); 	
	    	sb.append("<td align=\"right\">" + rpt.recTotal[idx] + "</td>");
	    	sb.append("<td align=\"right\">" + String.format("%.10f",rpt.recTotalPerc[idx]) + "%</td>"); 				
	    	sb.append("<td align=\"right\">" + String.format("%.10f",rpt.recNPITotalPerc[idx]) + "%</td>"); 				
	    	sb.append("<td align=\"right\">" + String.format("%.10f",rpt.recDRGTotalPerc[idx]) + "%</td>"); 				
	    	sb.append("<td align=\"right\">" + String.format("%.10f",rpt.recPIDTotalPerc[idx]) + "%</td>"); 				
	    	sb.append("<td align=\"right\">" +  NumberFormat.getCurrencyInstance().format(rpt.medicalClaimsTotalCosts[idx]) + "</td>"); 				
	    	sb.append("</tr>");	 								
        }        
		
		sb.append("</table>");
		
		
		return sb.toString();
		
	}

	
	
	
	private void initialize ()  {

		micky = new MongoInputCollectionSet(parameters);
		db = micky.getDb();
		collName=micky.getsClaimColl();
		collNameRx = micky.getsRxColl();
		
		jobUid=parameters.get("jobuid");
        schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");
		
	
		//test settings
       /* collName ="CHC_CHC_V5_3_6_2014_6_2015ClaimServLine20150721";//test
		collNameRx ="CHC_CHC_V5_3_6_2014_6_2015ClaimRx20150721";//test
		schemaName="CHC_CHC_V5_3_6_2014_6_201520150721";//test
		jobUid="1056";*/
        
        
		coll = db.getCollection(collName);
		collRx = db.getCollection(collNameRx);
		if(debugMode){
			System.out.println("collName="+collName);
		}
		
		
	}
	
	class Report {
		
		long medicalClaimsTotalRecords = 0l;
		BigDecimal medicalClaimsTotalAllowedAmt = new BigDecimal("0.00");
		long pharmacyClaimsTotalRecords = 0l;
		BigDecimal pharmacyClaimsTotalAllowedAmt = new BigDecimal("0.00");
		
		BigDecimal medicalClaimsTotalChargeAmt = new BigDecimal("0.00");
		
		int maxSize = 150;
		String billTypes[] =new String[maxSize];
		String billTypesDesc[] =new String[maxSize];
		long recTotal[] =new long[maxSize];
		float recTotalPerc[] =new float[maxSize];
		long recNPITotal[] =new long[maxSize];
		long recDRGTotal[] =new long[maxSize];
		long recPIDTotal[] =new long[maxSize];
		float recNPITotalPerc[] =new float[maxSize];
		float recDRGTotalPerc[] =new float[maxSize];
		float recPIDTotalPerc[] =new float[maxSize];
		String reportName ="BillTypeReport";
		
		BigDecimal[] medicalClaimsTotalCosts = new BigDecimal[maxSize];
		
		
	}
	//Arrays.fill(medicalClaimsTotalCosts, BigDecimal.ZERO);
	
	public static void main(String[] args) {
		
		log.info("Starting Post Mapping Control Report");

		BillTypeReport instance = new BillTypeReport();
		
		// get parameters and make them available to all
		RunParameters rp = new RunParameters();
		instance.parameters = rp.loadParameters(args, parameterDefaults);
				
		String s = instance.parameters.get("runname") + "_" + instance.parameters.get("rundate");
		instance.parameters.put("jobname", s);
		
				
		RunParameters.parameters.put("configfolder", instance.parameters.get("configfolder"));
		
		instance.process();
		
		log.info("Complete BillType Report");
	}
	

	static String [][] parameterDefaults = {
		{"configfolder", "C:/workspace/ECR_Analytics/trunk/EpisodeConstruction/src/"},
		{"rundate", "20150611"},
		{"runname", "CHC"},
		{"jobuid", "1043"} //hardcode for test
	};
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(BillTypeReport.class);

}
