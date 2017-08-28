




public abstract class ReportBase {
	

	String today;


	int maxErrorRows = 1000;
	
	
	double total_svc_claim_amt = 0;
	double total_svc_claim_count = 0;
	double total_rx_claim_amt = 0;
	double total_rx_claim_count = 0;
	double total_ip_claim_amt = 0;
	double total_ip_claim_count = 0;
	double total_op_claim_amt = 0;
	double total_op_claim_count = 0;
	double total_pb_claim_amt = 0;
	double total_pb_claim_count = 0;
	double total_members_with_claims = 0;
	double total_members_with_ip = 0;
	double total_members_with_op = 0;
	double total_members_with_pb = 0;
	double total_members_with_rx = 0;

	
	InputStatistics stats;
	
	List<ClaimServLine> svcLines;
	Map<String, List<ClaimRx>> rxClaims = new HashMap<String, List<ClaimRx>>();
	Map<String, List<Enrollment>> enrollments;
	Map<String, PlanMember> members;
	Map<String, Provider> providers;
	
	
	PlanMember planMember = null;
	
	
	AllDataInterface di;
	ErrorManager errMgr;
	
	/**
	 * construct this class with invoker's parameters and start a workbook
	 * @param parameters
	 */
	public ReportBase(HashMap<String, String> parameters) {
		this.parameters = parameters;
		Date date = Calendar.getInstance().getTime();
		DateFormat formatter = new SimpleDateFormat("yyyy_MM_dd_hhmm");
		today = formatter.format(date);
	}
	
	HashMap<String, String> parameters = new HashMap<String, String>();
	

	

	void initialize(AllDataInterface di) {
		this.di = di;
		errMgr = new ErrorManager(di);
		// zero out all counts and totals
		total_svc_claim_amt = 0;
		total_svc_claim_count = 0;
		total_rx_claim_amt = 0;
		total_rx_claim_count = 0;
		total_ip_claim_amt = 0;
		total_ip_claim_count = 0;
		total_op_claim_amt = 0;
		total_op_claim_count = 0;
		total_pb_claim_amt = 0;
		total_pb_claim_count = 0;
		total_members_with_claims = 0;
		total_members_with_ip = 0;
		total_members_with_op = 0;
		total_members_with_pb = 0;
		total_members_with_rx = 0;
		pcdSubs = new LinkedList<pcdSubTotal>();
		// prime all of the collections
		planMember =  new PlanMember();
		log.info("Obtaining Claim Data");
		svcLines = di.getAllServiceClaims();
		log.info("Claim Data Obtained");
		log.info("Obtaining Enrollment Data");
		enrollments = di.getAllEnrollments();
		log.info("Enrollment Data Obtained");
		log.info("Obtaining Plan Member Data");
		members = di.getAllPlanMembers();
		log.info("Plan Member Data Obtained");
		log.info("Obtaining Provider Data");
		providers = di.getAllProviders();
		log.info("Provider Data Obtained");
		log.info("Obtaining Rx Claims Data");
		assignRxClaims(di.getAllRxClaims());
		log.info(" Rx Claims Data Obtained");
	}


	
	void assignRxClaims(List<ClaimRx> allRxClaims) {
		String sMemberId = null;
		List<ClaimRx> rxList = new ArrayList<ClaimRx>();
		for (ClaimRx clm : allRxClaims) {
			if (clm.getMember_id().equals(sMemberId))
			{
				rxList.add(clm);
			}
			else
			{
				sMemberId = clm.getMember_id();
				rxList = new ArrayList<ClaimRx>();
				rxClaims.put(sMemberId, rxList);
				rxList.add(clm);
			}
		}
	}
	

	LinkedList<pcdSubTotal>pcdSubs;

	
	/**
	 * pass the ordered service Lines looking for each plan member
	 * we could come through here a number of times for reports,
	 * so this is not a great template for a one-pass claim loop
	 */
	void processServiceClaims () {
		boolean bAddMode = true;						// do we need to add
		for (ClaimServLine clm : svcLines) { 
						
			// build a list of claims within study period for each plan member
			if (clm.getMember_id() == null)
				continue;
			if (clm.getMember_id().equals(planMember.getMember_id()))
			{
				if (bAddMode)
					planMember.addClaimServiceLine(clm);
			}
			else
			{
				if (planMember.getMember_id() != null)				// need a null check for first pass
				{
					// do everything for the prior plan member
					processEachPlanMember() ;
				}
				planMember = members.get(clm.getMember_id());
				
				// a null here indicates that a claim has been found for a member id not in the member list
				// we should probably abort
				if (planMember == null)
				{
					planMember = new PlanMember();
					planMember.setMember_id(clm.getMember_id());
					//log.error("Found member id " + clm.getMember_id() + " in the claim file, but not in the member file");
					//di.getAllProviderErrors().add("Found member id " + clm.getMember_id() + " in the claim file, but not in the member file");
					errMgr.issueError("svc102", null, clm);
				}
				// join the enrollment - if not joined already by prior cycle through here
				if (planMember.getEnrollment() == null)
					planMember.setEnrollment(enrollments.get(clm.getMember_id()));
				// get any Rx claims - if not already loaded (yeah, this stinks, it will look again if there aren't any Rx claims)
				if (planMember.getRxClaims().isEmpty())
					planMember.setRxClaims( rxClaims.get(clm.getMember_id()) == null ? new ArrayList<ClaimRx>() : rxClaims.get(clm.getMember_id()) );
				// check if any svc claims have been posted (i.e., we've been through here before)
				if (planMember.getServiceLines().isEmpty()) {
					planMember.addClaimServiceLine(clm);
					bAddMode = true;
				}
				else
					bAddMode = false;
			}
		}
		// do everything for the last plan member
		processEachPlanMember() ;
	}

	
	
	/**
	 * pass each plan member's service Lines looking for counts, etc.
	 */
	private void processEachPlanMember()  {
		//log.info("Processing member " + planMember.getMember_id());
		total_members_with_claims++;
		boolean bHadIP = false, bHadOP = false, bHadPB = false, bHadRx = false;
		HashMap<String, ArrayList<String>> claim_ids = new HashMap<String, ArrayList<String>>();
		ArrayList<String> line_ids;
		pcdSubTotal sub = new pcdSubTotal(planMember.getMember_id());
		int debugLoc = 0;
		for (ClaimServLine c : planMember.getServiceLines()) {
			debugLoc++;
			if(!claim_ids.containsKey(c.getClaim_id())) {
				line_ids = new ArrayList<String>();
				claim_ids.put(c.getClaim_id(), line_ids);
				total_svc_claim_count++;
				if (c.getClaim_line_type_code() == null)
					c.setClaim_line_type_code("XX");
				try {
				switch (c.getClaim_line_type_code()) {
				case "IP": 
					total_ip_claim_count++;
					bHadIP = true;
					break;
				case "OP": 
					total_op_claim_count++;
					bHadOP = true;
					break;
				case "PB": 
					total_pb_claim_count++;
					bHadPB = true;
					break;
				}
				}
				catch (NullPointerException e) {
					log.error("Null at: " + debugLoc);
					e.printStackTrace();
				}
			}
			else {
				line_ids = claim_ids.get(c.getClaim_id());
				if (line_ids.contains(c.getClaim_line_id())) {
					errMgr.issueError("svc103", null, c);
					//log.error("Found member id " + c.getMember_id() + " with duplicate claim " + c.getClaim_id() + "|" + c.getClaim_line_id());
					//di.getAllServiceClaimErrors().add("Found member id " + c.getMember_id() + " with duplicate claim " + c.getClaim_id() + "|" + c.getClaim_line_id());
				}
				else
					line_ids.add(c.getClaim_line_id());
			}
			try {
			switch (c.getClaim_line_type_code()) {
			case "IP": 
				sub.ip_total_cnt++;
				total_ip_claim_amt = total_ip_claim_amt + c.getAllowed_amt();
				sub.ip_total_amt = sub.ip_total_amt + c.getAllowed_amt();
				break;
			case "OP": 
				sub.op_total_cnt++;
				total_op_claim_amt = total_op_claim_amt + c.getAllowed_amt();
				sub.op_total_amt = sub.op_total_amt + c.getAllowed_amt();
				break;
			case "PB": 
				sub.pb_total_cnt++;
				total_pb_claim_amt = total_pb_claim_amt + c.getAllowed_amt();
				sub.pb_total_amt = sub.pb_total_amt + c.getAllowed_amt();
				break;
			}
			}
			catch (NullPointerException e) {
				log.error("Null at: " + debugLoc);
				e.printStackTrace();
			}
			
			total_svc_claim_amt = total_svc_claim_amt + c.getAllowed_amt();
		}
		for (ClaimRx c : planMember.getRxClaims()) {
			sub.rx_total_amt = sub.rx_total_amt + c.getAllowed_amt();
			total_rx_claim_amt = total_rx_claim_amt + c.getAllowed_amt();
			total_rx_claim_count++;
			sub.rx_total_cnt++;
			bHadRx = true;
		}
		pcdSubs.add(sub);
		if (bHadIP)
			total_members_with_ip++;
		if (bHadOP)
			total_members_with_op++;
		if (bHadPB)
			total_members_with_pb++;
		if (bHadRx)
			total_members_with_rx++;
		//log.info("This member was born in " + planMember.getBirth_year() + " and has " + planMember.getServiceLines().size() + " claim service lines"
		//		+ " totalling $" + claim_total);
		
	}
	

	
	class pcdSubTotal {
		String member_id;
		double ip_total_amt = 0L;
		double ip_total_cnt = 0L;
		double op_total_amt = 0L;
		double op_total_cnt = 0L;
		double pb_total_amt = 0L;
		double pb_total_cnt = 0L;
		double rx_total_amt = 0L;
		double rx_total_cnt = 0L;
		
		pcdSubTotal (String member_id) {
			this.member_id = member_id;
		}
	}

	
	private static org.apache.log4j.Logger log = Logger.getLogger(ReportBase.class);



}
