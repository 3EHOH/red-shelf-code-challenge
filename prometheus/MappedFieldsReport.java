





public class MappedFieldsReport {
	
	
	HashMap<String, String> parameters;
	
	Report rpt = new Report();
    
	String schemaName=null;
	String jobUid=null;
	String env=null;
	
    long claimRecCnt=0;
    int idx = 0;
    
	String medFieldList[]= {"master_claim_id","member_id", "claim_id","claim_line_id","provider_npi","provider_id",
			"physician_id","facility_id","allowed_amt","facility_type_code","begin_date","end_date","place_of_svc_code",
			"claim_line_type_code","quantity","standard_payment_amt","charge_amt","paid_amt",
			"prepaid_amt","copay_amt","coinsurance_amt","deductible_amt","insurance_product","admission_date",
			"admission_src_code","admit_type_code","discharge_status_code","discharge_date","type_of_bill","rev_count",
			"drg_version","ms_drg_code","apr_drg_code","readmission","office_visit","ed_visit","ed_visit_id",
			"core_service","pas", "rev_code/code"	};
	
	String rxFieldList[]= {"master_claim_id",
	"member_id",
	"claim_id",
	"allowed_amt",
	"line_counter",
	"charge_amt",
	"paid_amt",
	"prepaid_amt",
	"copay_amt",
	"coinsurance_amt",
	"deductible_amt",
	"drug_nomen",
	"drug_code",
	"drug_name",
	"builder_match_code",
	"days_supply_amt",
	"quantityDispensed",
	"rx_fill_date",
	"prescribing_provider_id",
	"prescribing_provider_npi",
	"prescribing_provider_dea",
	"pharmacy_zip_code",
	"insurance_product",
	"genericDrugIndicator",
	"national_pharmacy_Id",
	"orig_adj_rev",
	"plan_id" 		};

    
	String memFieldList[]= {"member_id",
	"gender_code",
	"race_code",
	"zip_code",
	"birth_year",
	"age",
	"enforced_provider_id",
	"primary_care_provider_id",
	"primary_care_provider_npi",
	"pcp_effective_date",
	"date_of_death",
	"insurance_type",
	"insurance_carrier",
	"dual_eligible",
	"months_eligible_total",
	"cost_of_care",
	"unassigned_cost",
	"assigned_cost",
	"ed_visits",
	"ed_cost",
	"ip_stay_count",
	"ip_stay_cost",
	"bed_days",
	"alos",
	"claim_lines",
	"claim_lines_t",
	"claim_lines_tc",
	"claim_lines_c",
	"ip_readmit_count",
	"ip_readmit_cost",
	"designated_pcp",
	"plan_id",
	"rf_count",
	"st_count" };

		
	String pvFieldList[]= {"provider_id",
	"npi",
	"dea_no",
	"group_name",
	"practice_name",
	"provider_name",
	"system_name",
	"tax_id",
	"medicare_id",
	"zipcode",
	"pilot_name",
	"aco_name",
	"provider_type",
	"provider_attribution_code",
	"provider_aco",
	"provider_health_system",
	"provider_medical_group",
	"facility_id",
	"member_count",
	"episode_count",
	"severity_score",
	"performance_score" };

	
	String enFieldList[]= {"member_id",
	"begin_date",
	"end_date",
	"insurance_product",
	"coverage_type",
	"isGap" };
	
	
	//index fields only
	String medFieldListIdx[]= {"master_claim_id","member_id", "claim_line_type_code","begin_date"		};
	String rxFieldListIdx[]= {"master_claim_id","member_id"	};
	String memFieldListIdx[]= {"member_id"	};
	String enFieldListIdx[]= {"member_id",	"begin_date"     };
	String pvFieldListIdx[]= {"provider_id"    };
		
	
    HibernateHelper h;
	SessionFactory factory;
	
	boolean debugMode = false;
	
	/**
	 * static parameter constructor, just for testing
	 */
	public MappedFieldsReport() {	}
	
	/**
	 * constructor using parameters pulled from control database
	 * @param parameters
	 */
	public MappedFieldsReport(HashMap<String, String> parameters) {
		this.parameters = parameters;
	}
	
	
	private void process () {
		
		initialize();
		if(debugMode){
			System.out.println("init done!");
		}
		generateReport();
		if(debugMode){
			System.out.println("generateReport done!");
		}
		
		storeReport();
		if(debugMode){
			System.out.println("storeReport done!");
		}
		
	}
	
	
	
	private void generateReport() {
		List<Class<?>> cList = new ArrayList<Class<?>>();
        cList.add(ClaimsCombinedTbl.class);
        cList.add(PlanMember.class);
        cList.add(Enrollment.class);
        cList.add(Provider.class);
        cList.add(ClaimRx.class);
        cList.add(MedCode.class);
        cList.add(ClaimLineTbl.class);
		
        HibernateHelper h = new HibernateHelper(cList, env, schemaName);

        String connUrl=null;
        String rptQry=null;
		Connection conn=null;
		ResultSet rs=null;
    
        try{
            
       		if(debugMode){
       			System.out.println("Env="+env+ " DbDial="+h.getCurrentConfig().getConnParms().getDialect()+" schema="+schemaName+" db="+h.getCurrentConfig().getConnParms().getDatabase());
       		}
        	
            if("vertica".equalsIgnoreCase(h.getCurrentConfig().getConnParms().getDialect())){
            	connUrl="jdbc:vertica://" + h.getCurrentConfig().getConnParms().getDbUrl()+"/" + h.getCurrentConfig().getConnParms().getDatabase();
            }else{
          	    connUrl="jdbc:mysql://" + h.getCurrentConfig().getConnParms().getDbUrl()+"/ecr";
            }
       		if(debugMode){
       			System.out.println("connUrl="+connUrl);
       		}
                
    		Properties connProp = new Properties();
            connProp.put("user", h.getCurrentConfig().getConnParms().getDbUser());
            connProp.put("password", h.getCurrentConfig().getConnParms().getDbPw());
            conn = DriverManager.getConnection(connUrl, connProp);
            
            rptQry="select count(*) med_total from "+schemaName+".claims_combined c where c.claim_line_type_code in ('IP','OP','PB') ";            
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
		    	rpt.totalRecMed= rs.getLong("med_total");
            }
			
	    	if(rpt.totalRecMed>25000000){
	    		medFieldList=medFieldListIdx;
	    		rxFieldList=rxFieldListIdx;
	    		memFieldList=memFieldListIdx;
	    		enFieldList=enFieldListIdx;
	    		pvFieldList=pvFieldListIdx;
	    		
	    	}
			
			for(int i = 0; i<medFieldList.length; i++){
				if("rev_code/code".equalsIgnoreCase(medFieldList[i])){
					
					rptQry ="select count(*) total from "+schemaName+".claims_combined c where c.claim_line_type_code='IP' and c.id = (select min(u_c_id) from code cd where cd.nomen='REV' and cd.u_c_id=c.id)";
		       	    rs = conn.createStatement().executeQuery(rptQry);
		            while(rs.next()){
		            	rpt.recCountMed[i] = rs.getLong("total");
		            }
					
					rptQry ="select count(*) from "+schemaName+".claims_combined c where c.claim_line_type_code in ('PB','OP') and c.id=(select min(u_c_id) from code cd where cd.nomen='PX' and cd.principal=1 and cd.u_c_id=c.id)";
		       	    rs = conn.createStatement().executeQuery(rptQry);
		            while(rs.next()){
		            	rpt.recCountMed[i] =rpt.recCountMed[i]+rs.getLong("total");
		            }
					
		    		rpt.recPercMed[i] =(float)rpt.recCountMed[i] / rpt.totalRecMed *100;
		    		
				}else if("allowed_amt".equalsIgnoreCase(medFieldList[i]) ||"real_allowed_amt".equalsIgnoreCase(medFieldList[i]) ||"proxy_allowed_amt".equalsIgnoreCase(medFieldList[i]) ||"standard_payment_amt".equalsIgnoreCase(medFieldList[i])
						||"charge_amt".equalsIgnoreCase(medFieldList[i]) ||"paid_amt".equalsIgnoreCase(medFieldList[i]) ||"prepaid_amt".equalsIgnoreCase(medFieldList[i])
						||"copay_amt".equalsIgnoreCase(medFieldList[i]) ||"coinsurance_amt".equalsIgnoreCase(medFieldList[i]) ||"deductible_amt".equalsIgnoreCase(medFieldList[i])
						||"quantity".equalsIgnoreCase(medFieldList[i]) ||"assigned".equalsIgnoreCase(medFieldList[i])||"assigned_count".equalsIgnoreCase(medFieldList[i]) ||"rev_count".equalsIgnoreCase(medFieldList[i])
						||"readmission".equalsIgnoreCase(medFieldList[i])||"office_visit".equalsIgnoreCase(medFieldList[i])||"trigger".equalsIgnoreCase(medFieldList[i])||"ed_visit".equalsIgnoreCase(medFieldList[i])
						||"core_service".equalsIgnoreCase(medFieldList[i])||"pas".equalsIgnoreCase(medFieldList[i])
						){
					
					rptQry = "select count(*) total from "+schemaName+".claims_combined c where claim_line_type_code in ('IP','OP','PB') and ("+medFieldList[i]+" is null or "+ medFieldList[i]+" = 0)";
		       	    rs = conn.createStatement().executeQuery(rptQry);
		            while(rs.next()){
		            	rpt.recCountMed[i]= rpt.totalRecMed-rs.getLong("total");
		            	break;
		            }
					rpt.recPercMed[i] =(float)rpt.recCountMed[i] / rpt.totalRecMed *100;
					
				}else if("begin_date".equalsIgnoreCase(medFieldList[i]) ||"end_date".equalsIgnoreCase(medFieldList[i]) ||"admission_date".equalsIgnoreCase(medFieldList[i]) ||"discharge_date".equalsIgnoreCase(medFieldList[i])
						){
					
					rptQry = "select count(*) total from "+schemaName+".claims_combined c where c.claim_line_type_code in ('IP','OP','PB') and ("+medFieldList[i]+" is null )";
		       	    rs = conn.createStatement().executeQuery(rptQry);
		            while(rs.next()){
		            	rpt.recCountMed[i]= rpt.totalRecMed-rs.getLong("total");
		            	break;
		            }
					rpt.recPercMed[i] =(float)rpt.recCountMed[i] / rpt.totalRecMed *100;
					
				}else{
					rptQry="select count(*) total from "+schemaName+".claims_combined c where c.claim_line_type_code in ('IP','OP','PB') and ("+medFieldList[i]+" is null or "+ medFieldList[i]+" = '')";
		       	    rs = conn.createStatement().executeQuery(rptQry);
		            while(rs.next()){
		            	rpt.recCountMed[i]= rpt.totalRecMed-rs.getLong("total");
		            	break;
		            }
					rpt.recPercMed[i] =(float)rpt.recCountMed[i] / rpt.totalRecMed *100;
				}
				
				if(debugMode){
					System.out.println("Med totalRec="+rpt.totalRecMed+" field="+medFieldList[i]+" populatedCnt="+rpt.recCountMed[i]);
				}
			}
			
            rptQry = "select count(*) total from "+schemaName+".claim_line_rx c  ";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
            	rpt.totalRecRX= rs.getLong("total");
            	break;
            }
			
			for(int i = 0; i<rxFieldList.length; i++){
				if("allowed_amt".equalsIgnoreCase(rxFieldList[i]) ||"real_allowed_amt".equalsIgnoreCase(rxFieldList[i])||"".equalsIgnoreCase(rxFieldList[i])||"proxy_allowed_amt".equalsIgnoreCase(rxFieldList[i])
				||"assigned".equalsIgnoreCase(rxFieldList[i]) ||"assigned_count".equalsIgnoreCase(rxFieldList[i]) ||"line_counter".equalsIgnoreCase(rxFieldList[i]) 
				||"charge_amt".equalsIgnoreCase(rxFieldList[i]) ||"paid_amt".equalsIgnoreCase(rxFieldList[i])||"prepaid_amt".equalsIgnoreCase(rxFieldList[i])	
				||"copay_amt".equalsIgnoreCase(rxFieldList[i])||"coinsurance_amt".equalsIgnoreCase(rxFieldList[i])||"deductible_amt".equalsIgnoreCase(rxFieldList[i])
				||"days_supply_amt".equalsIgnoreCase(rxFieldList[i])||"quantityDispensed".equalsIgnoreCase(rxFieldList[i])
				){
					rptQry="select count(*) total from "+schemaName+".claim_line_rx c where  "+rxFieldList[i]+" is null or "+rxFieldList[i]+" = 0 ";
				}else if("rx_fill_date".equalsIgnoreCase(rxFieldList[i])){
					rptQry="select count(*) total from "+schemaName+".claim_line_rx c where  "+rxFieldList[i]+" is null ";
				}else{
					rptQry="select count(*) total from "+schemaName+".claim_line_rx c where  "+rxFieldList[i]+" is null or "+rxFieldList[i]+" = '' ";
				}
	       	    rs = conn.createStatement().executeQuery(rptQry);
	            while(rs.next()){
	            	rpt.recCountRX[i]= rpt.totalRecRX-rs.getLong("total");
	            	break;
	            }
	    		rpt.recPercRX[i] =(float)rpt.recCountRX[i] / rpt.totalRecRX *100;
				
				if(debugMode){
					System.out.println("RX totalRec="+rpt.totalRecRX+" field="+rxFieldList[i]+" populatedCnt="+rpt.recCountRX[i]);
				}
			} 

            rptQry="select count(*) total from "+schemaName+".member ";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
            	rpt.totalRecMem= rs.getLong("total");
            }
			
			for(int i = 0; i<memFieldList.length; i++){
				if("birth_year".equalsIgnoreCase(memFieldList[i]) ||"age".equalsIgnoreCase(memFieldList[i])||"dual_eligible".equalsIgnoreCase(memFieldList[i])||"months_eligible_total".equalsIgnoreCase(memFieldList[i])
				||"cost_of_care".equalsIgnoreCase(memFieldList[i])||"unassigned_cost".equalsIgnoreCase(memFieldList[i])||"assigned_cost".equalsIgnoreCase(memFieldList[i])||"ed_visits".equalsIgnoreCase(memFieldList[i])						
				||"ed_cost".equalsIgnoreCase(memFieldList[i])||"ip_stay_count".equalsIgnoreCase(memFieldList[i])||"ip_stay_cost".equalsIgnoreCase(memFieldList[i])||"bed_days".equalsIgnoreCase(memFieldList[i])						
				||"alos".equalsIgnoreCase(memFieldList[i])||"claim_lines".equalsIgnoreCase(memFieldList[i])||"claim_lines_t".equalsIgnoreCase(memFieldList[i])||"claim_lines_tc".equalsIgnoreCase(memFieldList[i])						
				||"claim_lines_c".equalsIgnoreCase(memFieldList[i])||"ip_readmit_count".equalsIgnoreCase(memFieldList[i])||"ip_readmit_cost".equalsIgnoreCase(memFieldList[i])||"rf_count".equalsIgnoreCase(memFieldList[i])
				||"st_count".equalsIgnoreCase(memFieldList[i])
				){
					rptQry = "select count(*) total from "+schemaName+".member where "+memFieldList[i]+" is null or "+memFieldList[i]+" =0";
				}else if("pcp_effective_date".equalsIgnoreCase(memFieldList[i])||"date_of_death".equalsIgnoreCase(memFieldList[i])){
					rptQry = "select count(*) total from "+schemaName+".member where "+memFieldList[i]+" is null ";
				}else{
					rptQry = "select count(*) total from "+schemaName+".member where "+memFieldList[i]+" is null or "+memFieldList[i]+" =''";
				}
	       	    rs = conn.createStatement().executeQuery(rptQry);
	            while(rs.next()){
	            	rpt.recCountMem[i]= rpt.totalRecMem-rs.getLong("total");
	            	break;
	            }
	    		rpt.recPercMem[i] =(float)rpt.recCountMem[i] / rpt.totalRecMem *100;
				
				if(debugMode){
					System.out.println("Member totalRec="+rpt.totalRecMem+" field="+memFieldList[i]+" populatedCnt="+rpt.recCountMem[i]);
				}
			}

            rptQry = "select count(*) total from "+schemaName+".enrollment ";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
            	rpt.totalRecEn= rs.getLong("total");
            	break;
            }
			
			for(int i = 0; i<enFieldList.length; i++){
				if("age_at_enrollment".equalsIgnoreCase(enFieldList[i]) ||"isGap".equalsIgnoreCase(enFieldList[i])
				){
					rptQry ="select count(*) total from "+schemaName+".enrollment where "+enFieldList[i]+" is null or "+enFieldList[i]+"=0";
				}else if ("begin_date".equalsIgnoreCase(enFieldList[i]) ||"end_date".equalsIgnoreCase(enFieldList[i])){
					rptQry = "select count(*) total from "+schemaName+".enrollment where "+enFieldList[i]+" is null ";
				}else{
					rptQry = "select count(*) total from "+schemaName+".enrollment where "+enFieldList[i]+" is null or "+enFieldList[i]+"=''";
				}
	       	    rs = conn.createStatement().executeQuery(rptQry);
	            while(rs.next()){
	            	rpt.recCountEn[i]= rpt.totalRecEn-rs.getLong("total");
	            	break;
	            }
	    		rpt.recPercEn[i] =(float)rpt.recCountEn[i] / rpt.totalRecEn *100;
				
				if(debugMode){
					System.out.println("Enroll totalRec="+rpt.totalRecEn+" field="+enFieldList[i]+" populatedCnt="+rpt.recCountEn[i]);
				}
			}

			
            rptQry= "select count(*) total from "+schemaName+".provider ";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
            	rpt.totalRecPv= rs.getLong("total");
            	break;
            }
			
			for(int i = 0; i<pvFieldList.length; i++){
				if("member_count".equalsIgnoreCase(pvFieldList[i]) ||"episode_count".equalsIgnoreCase(pvFieldList[i])
				||"severity_score".equalsIgnoreCase(pvFieldList[i])||"performance_score".equalsIgnoreCase(pvFieldList[i])
				){
					rptQry = "select count(*) total from "+schemaName+".provider where "+pvFieldList[i]+" is null or "+pvFieldList[i]+" = 0";
				}else{
					rptQry = "select count(*) total from "+schemaName+".provider where "+pvFieldList[i]+" is null or "+pvFieldList[i]+" = ''";
				}
	       	    rs = conn.createStatement().executeQuery(rptQry);
	            while(rs.next()){
	            	rpt.recCountPv[i]= rpt.totalRecPv-rs.getLong("total");
	            	break;
	            }
	    		rpt.recPercPv[i] =(float)rpt.recCountPv[i] / rpt.totalRecPv *100;
				
				if(debugMode){
					System.out.println("Provider totalRec="+rpt.totalRecPv+" field="+pvFieldList[i]+" populatedCnt="+rpt.recCountPv[i]);
				}
			}
			
		}catch (HibernateException e) {
			e.printStackTrace(); 
		} catch (Exception e) {
			e.printStackTrace();
			
		}finally {

			try {
				 if(rs !=null){rs.close();}	
				 if(conn != null){conn.close();	}
				 h.close(env, schemaName);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	    
//		log.info(getStatsAsHTML());
		
	}


	private void generateReportHiber() {
		List<Class<?>> cList = new ArrayList<Class<?>>();
        cList.add(ClaimsCombinedTbl.class);
        cList.add(PlanMember.class);
        cList.add(Enrollment.class);
        cList.add(Provider.class);
        cList.add(ClaimRx.class);
        cList.add(MedCode.class);
        cList.add(ClaimLineTbl.class);
		
        HibernateHelper h = new HibernateHelper(cList, env, schemaName);
        SessionFactory factory = h.getFactory(env, schemaName);

        Session session = factory.openSession();
        session.setCacheMode(CacheMode.IGNORE);
        session.setFlushMode(FlushMode.COMMIT);
        Transaction tx = null;
        Query query=null;
        String qryCnt =null;
    
        try{
			tx = session.beginTransaction();

            ArrayList<String> claimTypeList = new ArrayList<String>();
            claimTypeList.add("IP");
            claimTypeList.add("OP");
            claimTypeList.add("PB");
			
			query = session.createQuery("select count(*) from ClaimLineTbl c where claim_line_type_code in (:claimTypeList)");
	    	query.setParameterList("claimTypeList",claimTypeList);
	    	rpt.totalRecMed= (Long)query.uniqueResult();
	    	if(debugMode){
				System.out.println("Med totalRec="+rpt.totalRecMed);
			}	    	
	    	if(rpt.totalRecMed>25000000){
	    		medFieldList=medFieldListIdx;
	    		rxFieldList=rxFieldListIdx;
	    		memFieldList=memFieldListIdx;
	    		enFieldList=enFieldListIdx;
	    		pvFieldList=pvFieldListIdx;
	    		
	    	}
			
			for(int i = 0; i<medFieldList.length; i++){
				if("rev_code/code".equalsIgnoreCase(medFieldList[i])){
					qryCnt ="select count(*) from ClaimLineTbl c where c.claim_line_type_code='IP' and c.id = ( select min(u_c_id) from MedCode cd where cd.nomen='REV' and cd.u_c_id=c.id)";
					query = session.createQuery(qryCnt);
					rpt.recCountMed[i] =(Long)query.uniqueResult();
					
					qryCnt ="select count(*) from ClaimLineTbl c where c.claim_line_type_code in ('PB','OP') and c.id=(select min(u_c_id) from MedCode cd where cd.nomen='PX' and cd.principal=1	and cd.u_c_id=c.id)";
					query = session.createQuery(qryCnt);				
					rpt.recCountMed[i] =rpt.recCountMed[i]+(Long)query.uniqueResult();
		    		rpt.recPercMed[i] =(float)rpt.recCountMed[i] / rpt.totalRecMed *100;
				}else if("allowed_amt".equalsIgnoreCase(medFieldList[i]) ||"real_allowed_amt".equalsIgnoreCase(medFieldList[i]) ||"proxy_allowed_amt".equalsIgnoreCase(medFieldList[i]) ||"standard_payment_amt".equalsIgnoreCase(medFieldList[i])
						||"charge_amt".equalsIgnoreCase(medFieldList[i]) ||"paid_amt".equalsIgnoreCase(medFieldList[i]) ||"prepaid_amt".equalsIgnoreCase(medFieldList[i])
						||"copay_amt".equalsIgnoreCase(medFieldList[i]) ||"coinsurance_amt".equalsIgnoreCase(medFieldList[i]) ||"deductible_amt".equalsIgnoreCase(medFieldList[i])
						||"quantity".equalsIgnoreCase(medFieldList[i]) ||"assigned".equalsIgnoreCase(medFieldList[i])||"assigned_count".equalsIgnoreCase(medFieldList[i]) ||"rev_count".equalsIgnoreCase(medFieldList[i])
						||"readmission".equalsIgnoreCase(medFieldList[i])||"office_visit".equalsIgnoreCase(medFieldList[i])||"trigger".equalsIgnoreCase(medFieldList[i])||"ed_visit".equalsIgnoreCase(medFieldList[i])
						||"core_service".equalsIgnoreCase(medFieldList[i])||"pas".equalsIgnoreCase(medFieldList[i])
						){
					query = session.createQuery("select count(*) from ClaimLineTbl c where claim_line_type_code in (:claimTypeList) and ("+medFieldList[i]+" is null or "+ medFieldList[i]+" = 0)");
					query.setParameterList("claimTypeList",claimTypeList);
					rpt.recCountMed[i]= rpt.totalRecMed-(Long)query.uniqueResult();
					rpt.recPercMed[i] =(float)rpt.recCountMed[i] / rpt.totalRecMed *100;
				}else if("begin_date".equalsIgnoreCase(medFieldList[i]) ||"end_date".equalsIgnoreCase(medFieldList[i]) ||"admission_date".equalsIgnoreCase(medFieldList[i]) ||"discharge_date".equalsIgnoreCase(medFieldList[i])
						){
					query = session.createQuery("select count(*) from ClaimLineTbl c where claim_line_type_code in (:claimTypeList) and ("+medFieldList[i]+" is null )");
					query.setParameterList("claimTypeList",claimTypeList);
					rpt.recCountMed[i]= rpt.totalRecMed-(Long)query.uniqueResult();
					rpt.recPercMed[i] =(float)rpt.recCountMed[i] / rpt.totalRecMed *100;
					
				}else{
					query = session.createQuery("select count(*) from ClaimLineTbl c where claim_line_type_code in (:claimTypeList) and ("+medFieldList[i]+" is null or "+ medFieldList[i]+" = '')");
					query.setParameterList("claimTypeList",claimTypeList);
					rpt.recCountMed[i]= rpt.totalRecMed-(Long)query.uniqueResult();
					rpt.recPercMed[i] =(float)rpt.recCountMed[i] / rpt.totalRecMed *100;
				}
				
				if(debugMode){
					System.out.println("Med totalRec="+rpt.totalRecMed+" field="+medFieldList[i]+" populatedCnt="+rpt.recCountMed[i]);
				}
			}
			
            query = session.createQuery("select count(*) from ClaimRx c  ");
	    	rpt.totalRecRX= (Long)query.uniqueResult();
			
			for(int i = 0; i<rxFieldList.length; i++){
				if("allowed_amt".equalsIgnoreCase(rxFieldList[i]) ||"real_allowed_amt".equalsIgnoreCase(rxFieldList[i])||"".equalsIgnoreCase(rxFieldList[i])||"proxy_allowed_amt".equalsIgnoreCase(rxFieldList[i])
				||"assigned".equalsIgnoreCase(rxFieldList[i]) ||"assigned_count".equalsIgnoreCase(rxFieldList[i]) ||"line_counter".equalsIgnoreCase(rxFieldList[i]) 
				||"charge_amt".equalsIgnoreCase(rxFieldList[i]) ||"paid_amt".equalsIgnoreCase(rxFieldList[i])||"prepaid_amt".equalsIgnoreCase(rxFieldList[i])	
				||"copay_amt".equalsIgnoreCase(rxFieldList[i])||"coinsurance_amt".equalsIgnoreCase(rxFieldList[i])||"deductible_amt".equalsIgnoreCase(rxFieldList[i])
				||"days_supply_amt".equalsIgnoreCase(rxFieldList[i])||"quantityDispensed".equalsIgnoreCase(rxFieldList[i])
				){
					query = session.createQuery("select count(*) from ClaimRx c where  "+rxFieldList[i]+" is null or "+rxFieldList[i]+" = 0 ");
				}else if("rx_fill_date".equalsIgnoreCase(rxFieldList[i])){
					query = session.createQuery("select count(*) from ClaimRx c where  "+rxFieldList[i]+" is null ");
				
				}else{
					query = session.createQuery("select count(*) from ClaimRx c where  "+rxFieldList[i]+" is null or "+rxFieldList[i]+" = '' ");
				}
				rpt.recCountRX[i]= rpt.totalRecRX-(Long)query.uniqueResult();
	    		rpt.recPercRX[i] =(float)rpt.recCountRX[i] / rpt.totalRecRX *100;
				
				if(debugMode){
					System.out.println("RX totalRec="+rpt.totalRecRX+" field="+rxFieldList[i]+" populatedCnt="+rpt.recCountRX[i]);
				}
			} 

            query = session.createQuery("select count(*) from PlanMember ");
	    	rpt.totalRecMem= (Long)query.uniqueResult();
			
			for(int i = 0; i<memFieldList.length; i++){
				if("birth_year".equalsIgnoreCase(memFieldList[i]) ||"age".equalsIgnoreCase(memFieldList[i])||"dual_eligible".equalsIgnoreCase(memFieldList[i])||"months_eligible_total".equalsIgnoreCase(memFieldList[i])
				||"cost_of_care".equalsIgnoreCase(memFieldList[i])||"unassigned_cost".equalsIgnoreCase(memFieldList[i])||"assigned_cost".equalsIgnoreCase(memFieldList[i])||"ed_visits".equalsIgnoreCase(memFieldList[i])						
				||"ed_cost".equalsIgnoreCase(memFieldList[i])||"ip_stay_count".equalsIgnoreCase(memFieldList[i])||"ip_stay_cost".equalsIgnoreCase(memFieldList[i])||"bed_days".equalsIgnoreCase(memFieldList[i])						
				||"alos".equalsIgnoreCase(memFieldList[i])||"claim_lines".equalsIgnoreCase(memFieldList[i])||"claim_lines_t".equalsIgnoreCase(memFieldList[i])||"claim_lines_tc".equalsIgnoreCase(memFieldList[i])						
				||"claim_lines_c".equalsIgnoreCase(memFieldList[i])||"ip_readmit_count".equalsIgnoreCase(memFieldList[i])||"ip_readmit_cost".equalsIgnoreCase(memFieldList[i])||"rf_count".equalsIgnoreCase(memFieldList[i])
				||"st_count".equalsIgnoreCase(memFieldList[i])
				){
					query = session.createQuery("select count(*) from PlanMember  where "+memFieldList[i]+" is null or "+memFieldList[i]+" =0");
				}else if("pcp_effective_date".equalsIgnoreCase(memFieldList[i])||"date_of_death".equalsIgnoreCase(memFieldList[i])){
					query = session.createQuery("select count(*) from PlanMember  where "+memFieldList[i]+" is null ");
				}else{
					query = session.createQuery("select count(*) from PlanMember  where "+memFieldList[i]+" is null or "+memFieldList[i]+" =''");
				}
				rpt.recCountMem[i]= rpt.totalRecMem-(Long)query.uniqueResult();
	    		rpt.recPercMem[i] =(float)rpt.recCountMem[i] / rpt.totalRecMem *100;
				
				if(debugMode){
					System.out.println("Member totalRec="+rpt.totalRecMem+" field="+memFieldList[i]+" populatedCnt="+rpt.recCountMem[i]);
				}
			}

            query = session.createQuery("select count(*) from Enrollment ");
	    	rpt.totalRecEn= (Long)query.uniqueResult();
			
			for(int i = 0; i<enFieldList.length; i++){
				if("age_at_enrollment".equalsIgnoreCase(enFieldList[i]) ||"isGap".equalsIgnoreCase(enFieldList[i])
				){
					query = session.createQuery("select count(*) from Enrollment  where "+enFieldList[i]+" is null or "+enFieldList[i]+"=0");
				}else if ("begin_date".equalsIgnoreCase(enFieldList[i]) ||"end_date".equalsIgnoreCase(enFieldList[i])){
					query = session.createQuery("select count(*) from Enrollment  where "+enFieldList[i]+" is null ");
				}else{
					query = session.createQuery("select count(*) from Enrollment  where "+enFieldList[i]+" is null or "+enFieldList[i]+"=''");
				}
				rpt.recCountEn[i]= rpt.totalRecEn-(Long)query.uniqueResult();
	    		rpt.recPercEn[i] =(float)rpt.recCountEn[i] / rpt.totalRecEn *100;
				
				if(debugMode){
					System.out.println("Enroll totalRec="+rpt.totalRecEn+" field="+enFieldList[i]+" populatedCnt="+rpt.recCountEn[i]);
				}
			}

			
            query = session.createQuery("select count(*) from Provider ");
	    	rpt.totalRecPv= (Long)query.uniqueResult();
			
			for(int i = 0; i<pvFieldList.length; i++){
				if("member_count".equalsIgnoreCase(pvFieldList[i]) ||"episode_count".equalsIgnoreCase(pvFieldList[i])
				||"severity_score".equalsIgnoreCase(pvFieldList[i])||"performance_score".equalsIgnoreCase(pvFieldList[i])
				){
					query = session.createQuery("select count(*) from Provider where "+pvFieldList[i]+" is null or "+pvFieldList[i]+" = 0");
				}else{
					query = session.createQuery("select count(*) from Provider where "+pvFieldList[i]+" is null or "+pvFieldList[i]+" = ''");
				}
				rpt.recCountPv[i]= rpt.totalRecPv-(Long)query.uniqueResult();
	    		rpt.recPercPv[i] =(float)rpt.recCountPv[i] / rpt.totalRecPv *100;
				
				if(debugMode){
					System.out.println("Provider totalRec="+rpt.totalRecPv+" field="+pvFieldList[i]+" populatedCnt="+rpt.recCountPv[i]);
				}
			}
			
			
		}catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		}finally {
			session.close(); 

			try {
				h.close(env, schemaName);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	    
//		log.info(getStatsAsHTML());
		
	}
	
	
	private void storeReportV(String connUrl, String user, String passWd){
		Properties connProp = new Properties();
        connProp.put("user", user);
        connProp.put("password", passWd);

        String procRptTab ="processReport";
		Connection conn=null;
		
		String insertQ="insert INTO "+schemaName+"."+procRptTab+ " (jobUid,stepName,reportName,report,uid ) VALUES(?,?,?,?,?)";
		String updateQ="update "+schemaName+"."+procRptTab+" set report=? where jobUid =? AND stepname = '" +
				ProcessJobStep.STEP_POSTNORMALIZATION_REPORT  + "' and reportName='"+rpt.reportName+"'";
		
		String nextUidQ=" select ZEROIFNULL (max(uid))+1 uid from  "+schemaName+"."+procRptTab;
		PreparedStatement pstmt=null;
		ResultSet rs=null;
		int uId=0;
		try {
            conn = DriverManager.getConnection(connUrl, connProp);
            conn.setAutoCommit (false); 
    
            rs = conn.createStatement().executeQuery(nextUidQ);
            while(rs.next()){
            	uId=rs.getInt("uid");
            }
            
            pstmt = conn.prepareStatement(updateQ);
            pstmt.setBytes(1,getStatsAsHTML().getBytes());
            pstmt.setString(2,jobUid );
            int rtnCode= pstmt.executeUpdate();
            if(rtnCode ==0){
            	pstmt = conn.prepareStatement(insertQ);
                pstmt.setString(1,jobUid );
                pstmt.setString(2, ProcessJobStep.STEP_POSTNORMALIZATION_REPORT);
                pstmt.setString(3,rpt.reportName);
                pstmt.setBytes(4,getStatsAsHTML().getBytes());
                pstmt.setInt(5,uId );
            }
            pstmt.execute();
            conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
			 if(rs !=null){rs.close();}	
			 if(conn != null){conn.close();	}
			} catch (Exception e) {
				e.printStackTrace();
			 
			}
		}
	}
	

	
	private void storeReport() {
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
        cList.add(ProcessReport.class);
		
        HibernateHelper h = new HibernateHelper(cList, env, schemaName);
        
        String connUrl=null;
        String user=null;
        String passWd=null;
        Transaction tx = null;
        Session session = null;
        
        try{
       		if(debugMode){
       			System.out.println("Db Dialect="+h.getCurrentConfig().getConnParms().getDialect()+env+" schema="+schemaName);
       		}
            	
            if("vertica".equalsIgnoreCase(h.getCurrentConfig().getConnParms().getDialect())){
            	connUrl="jdbc:vertica://" + h.getCurrentConfig().getConnParms().getDbUrl()+"/" + h.getCurrentConfig().getConnParms().getDatabase();
            	user=h.getCurrentConfig().getConnParms().getDbUser();
            	passWd=h.getCurrentConfig().getConnParms().getDbPw();
                storeReportV(connUrl, user, passWd);
            }else{
            	SessionFactory factory = h.getFactory(env, schemaName);
            	session = factory.openSession();
            	session.setCacheMode(CacheMode.IGNORE);
            	session.setFlushMode(FlushMode.COMMIT);
            
			tx = session.beginTransaction();
			
			String hql = "FROM ProcessReport WHERE jobuid = :jobuid AND stepname = '" + ProcessJobStep.STEP_POSTNORMALIZATION_REPORT  + "' and reportName='"+rpt.reportName+"'";
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
				r.setStepName(ProcessJobStep.STEP_POSTNORMALIZATION_REPORT);
				r.setReportName(rpt.reportName);
			} else
				r = results.get(0);
			r.setReport(new SerialBlob(getStatsAsHTML().getBytes()));
			
			session.save(r);
			
			tx.commit();
            }
			
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
			if(session != null)
			session.close(); 

			try {
				h.close(env, schemaName);
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
		
		sb.append("<br><b>Medical File</b>");
		sb.append("<table class=\"sortable\" border=\"1\" cellspacing=\"1\" cellpadding=\"5\" width=\"750px\" >");
		sb.append("<tr bgcolor=\"#99CCFF\">");
		sb.append("<th width=\"300px\">Mapped Data Field Name</th>");
		sb.append("<th width=\"150px\">Number of Records Populated</th>");
		sb.append("<th width=\"150px\">Total Records in File</th>");
		sb.append("<th width=\"150px\">% Populated</th>");
		sb.append("</tr>");

		for(int i = 0; i<medFieldList.length; i++){
    		sb.append("<tr>");
			sb.append("<div><td align=\"left\">" + medFieldList[i]+ "</td></div>"); 	
			sb.append("<div><td align=\"left\">" + rpt.recCountMed[i]+ "</td></div>"); 	
			sb.append("<div><td align=\"left\">" + rpt.totalRecMed+ "</td></div>"); 	
			sb.append("<div><td align=\"left\">" + String.format("%.10f",rpt.recPercMed[i]) + "%</td></div>"); 	
			sb.append("</tr>");
		}
		
		sb.append("</table>");
		
		sb.append("<br><b>RX File</b>");
		sb.append("<table class=\"sortable\" border=\"1\" cellspacing=\"1\" cellpadding=\"5\" width=\"750px\" >");
		sb.append("<tr bgcolor=\"#99CCFF\">");
		sb.append("<th width=\"300px\">Mapped Data Field Name</th>");
		sb.append("<th width=\"150px\">Number of Records Populated</th>");
		sb.append("<th width=\"150px\">Total Records in File</th>");
		sb.append("<th width=\"150px\">% Populated</th>");
		sb.append("</tr>");

		for(int i = 0; i<rxFieldList.length; i++){
    		sb.append("<tr>");
			sb.append("<div><td align=\"left\">" + rxFieldList[i]+ "</td></div>"); 	
			sb.append("<div><td align=\"left\">" + rpt.recCountRX[i]+ "</td></div>"); 	
			sb.append("<div><td align=\"left\">" + rpt.totalRecRX+ "</td></div>"); 	
			sb.append("<div><td align=\"left\">" + String.format("%.10f",rpt.recPercRX[i]) + "%</td></div>"); 	
			sb.append("</tr>");
		}
		
		sb.append("</table>");

		sb.append("<br><b>Enrollment File</b>");
		sb.append("<table class=\"sortable\" border=\"1\" cellspacing=\"1\" cellpadding=\"5\" width=\"750px\" >");
		sb.append("<tr bgcolor=\"#99CCFF\">");
		sb.append("<th width=\"300px\">Mapped Data Field Name</th>");
		sb.append("<th width=\"150px\">Number of Records Populated</th>");
		sb.append("<th width=\"150px\">Total Records in File</th>");
		sb.append("<th width=\"150px\">% Populated</th>");
		sb.append("</tr>");

		for(int i = 0; i<enFieldList.length; i++){
    		sb.append("<tr>");
			sb.append("<div><td align=\"left\">" + enFieldList[i]+ "</td></div>"); 	
			sb.append("<div><td align=\"left\">" + rpt.recCountEn[i]+ "</td></div>"); 	
			sb.append("<div><td align=\"left\">" + rpt.totalRecEn+ "</td></div>"); 	
			sb.append("<div><td align=\"left\">" + String.format("%.10f",rpt.recPercEn[i]) + "%</td></div>"); 	
			sb.append("</tr>");
		}
		
		sb.append("</table>");
		
		

		sb.append("<br><b>Member File</b>");
		sb.append("<table class=\"sortable\" border=\"1\" cellspacing=\"1\" cellpadding=\"5\" width=\"750px\" >");
		sb.append("<tr bgcolor=\"#99CCFF\">");
		sb.append("<th width=\"300px\">Mapped Data Field Name</th>");
		sb.append("<th width=\"150px\">Number of Records Populated</th>");
		sb.append("<th width=\"150px\">Total Records in File</th>");
		sb.append("<th width=\"150px\">% Populated</th>");
		sb.append("</tr>");

		for(int i = 0; i<memFieldList.length; i++){
    		sb.append("<tr>");
			sb.append("<div><td align=\"left\">" + memFieldList[i]+ "</td></div>"); 	
			sb.append("<div><td align=\"left\">" + rpt.recCountMem[i]+ "</td></div>"); 	
			sb.append("<div><td align=\"left\">" + rpt.totalRecMem+ "</td></div>"); 	
			sb.append("<div><td align=\"left\">" + String.format("%.10f",rpt.recPercMem[i]) + "%</td></div>"); 	
			sb.append("</tr>");
		}
		
		sb.append("</table>");

		sb.append("<br><b>Provider File</b>");
		sb.append("<table class=\"sortable\" border=\"1\" cellspacing=\"1\" cellpadding=\"5\" width=\"750px\" >");
		sb.append("<tr bgcolor=\"#99CCFF\">");
		sb.append("<th width=\"300px\">Mapped Data Field Name</th>");
		sb.append("<th width=\"150px\">Number of Records Populated</th>");
		sb.append("<th width=\"150px\">Total Records in File</th>");
		sb.append("<th width=\"150px\">% Populated</th>");
		sb.append("</tr>");

		for(int i = 0; i<pvFieldList.length; i++){
    		sb.append("<tr>");
			sb.append("<div><td align=\"left\">" + pvFieldList[i]+ "</td></div>"); 	
			sb.append("<div><td align=\"left\">" + rpt.recCountPv[i]+ "</td></div>"); 	
			sb.append("<div><td align=\"left\">" + rpt.totalRecPv+ "</td></div>"); 	
			sb.append("<div><td align=\"left\">" + String.format("%.10f",rpt.recPercPv[i]) + "%</td></div>"); 	
			sb.append("</tr>");
		}
		
		sb.append("</table>");
		
//    	log.info ("rptHtml="+sb.toString());
		
		return sb.toString();
		
	}

	
	
	
	private void initialize ()  {
        schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");
		env = parameters.get("env") == null ?  "prd" :  parameters.get("env");

		jobUid=parameters.get("jobuid");
		
		//vertica test 
		/*jobUid="1120";
		schemaName="Warren_Vertica_8_Bench20161129";
		env="prdv";

		jobUid="1120";
		schemaName="MD_APCD_Enroll_Fix_5_420160711";
		env="prdv2";*/
		
		
		//mysql test
		/*jobUid="1143";
		schemaName="CT_Medicaid_5400220161205";*/
		
		/*jobUid="1138";
		schemaName="Horizon_54002_20160921";*/		
		
		if(debugMode){
			System.out.println("schemaName="+schemaName+" jobId="+jobUid);
		}
		
	}
	
	class Report {
		
		long medicalClaimsTotalRecords = 0l;
		
		int maxSize = 100;
		
		long totalRecMed =0l;
		long recCountMed[] =new long[maxSize];
		float recPercMed[] =new float[maxSize];

		long totalRecRX =0l;
		long recCountRX[] =new long[maxSize];
		float recPercRX[] =new float[maxSize];

		long totalRecMem =0l;
		long recCountMem[] =new long[maxSize];
		float recPercMem[] =new float[maxSize];

		long totalRecEn =0l;
		long recCountEn[] =new long[maxSize];
		float recPercEn[] =new float[maxSize];
		
		long totalRecPv =0l;
		long recCountPv[] =new long[maxSize];
		float recPercPv[] =new float[maxSize];
		
		
		String reportName ="MappedFieldsReport";
		
	}
	//Arrays.fill(medicalClaimsTotalCosts, BigDecimal.ZERO);
	
	public static void main(String[] args) {
		
		log.info("Starting DataValidation Report");

		MappedFieldsReport instance = new MappedFieldsReport();
		
		// get parameters and make them available to all
		RunParameters rp = new RunParameters();
		instance.parameters = rp.loadParameters(args, parameterDefaults);
				
		String s = instance.parameters.get("runname") + "_" + instance.parameters.get("rundate");
		instance.parameters.put("jobname", s);
		
				
		RunParameters.parameters.put("configfolder", instance.parameters.get("configfolder"));
		
		instance.process();
		
		
		log.info("Complete DataValidation Report");
	}
	

	static String [][] parameterDefaults = {
		{"configfolder", "C:/workspace/ECR_Analytics/trunk/EpisodeConstruction/src/"},
		{"rundate", "20150611"},
		{"runname", "CHC"},
		{"jobuid", "1043"}
	};
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(MappedFieldsReport.class);

}
