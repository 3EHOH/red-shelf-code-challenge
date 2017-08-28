





public class EpisodeMetaDataFromSQL extends EpisodeMetaDataAbstract implements EpisodeMetaDataInterface {
	
	
	
	private List<Class<?>> cList = new ArrayList<Class<?>>();
	HibernateHelper h;
	SessionFactory factory;
	String schemaName;
	
	
	private ArrayList<CodeQueryResult> cqr;
	HashMap<String, HashMap<String, ArrayList<TriggerCodeMetaData>>> tcbe;
	
	
	@Override
	public List<EpisodeMetaData> getAllEpisodes() {
		return epList;
	}
	
	
	public List<String> getAllEMCodes() {
		return emCodeList;
	}
	
	public MetaDataHeader getMetaDataHeader() {
		return mdh;
	}
	
	/*
	 * Constructor
	 * 
	 * actually reads an episode builder database and prepares metadata objects
	 */
	public EpisodeMetaDataFromSQL (String resource) {
		
		initialize(resource);

		buildEpisodesFromTables();
		doAllEMCodes();
		
		//bDebug = true;
		
		outputCheck();
		
	} 
	
	

	
	/**
	 * build an episode definition from the episode tag group
	 * @param ep
	 */
	private void buildEpisodesFromTables () {
		
		
		Session session = factory.openSession();
		Transaction tx = null;
	
		try{

			tx = session.beginTransaction();
		
			String hql = "FROM EpisodeDefinition WHERE VERSION=(SELECT max(VERSION) FROM EpisodeDefinition)";
			Query query = session.createQuery(hql);

			@SuppressWarnings("unchecked")
			List<EpisodeDefinition> results = query.list();
			
			// header stuff
			mdh.setExport_date( sdf.format(new Date()) );
			mdh.setExport_version( ((EpisodeDefinition)results.get(0)).getVERSION() );
			mdh.setRelease_notes("Auto load from SQL");
			
			for (EpisodeDefinition _ED : results) {
				EpisodeMetaData emd = new EpisodeMetaData();
				epList.add(emd);
				emd.setEpisode_id(_ED.getEPISODE_ID());
				emd.setEpisode_acronym(_ED.getNAME());
				emd.setEpisode_modified_date(_ED.getMODIFIED_DATE());
				emd.setEpisode_name(_ED.getDESCRIPTION());
				emd.setEpisode_type(_ED.getTYPE());
				emd.setEpisode_version(_ED.getVERSION());
				if (_ED.getTYPE().equals("System Related Failure")) {
					emd.setPost_claim_assignment(1);
				} else {
					emd.setPost_claim_assignment(0);
				}
				emd.setLook_back(_ED.getBOUND_OFFSET());
				if (_ED.getEND_OF_STUDY() != null  &&  _ED.getEND_OF_STUDY() == 1)
					emd.setLook_ahead("EOS");
				else
					emd.setLook_ahead(Integer.toString(_ED.getBOUND_LENGTH()));
				emd.setCondition_min(_ED.getCONDITION_MIN());
			}

			tx.commit();

		
		} catch (HibernateException e) {
		
			if (tx != null) tx.rollback();
			e.printStackTrace();
		
		} finally {
			
			session.close(); 

		}
		
		
		// build out episodes from related tables
		for (EpisodeMetaData emd : epList) {
			log.info("Starting Episode " + emd.getEpisode_id() + " " + new Date());
			
			
			
			doTriggerConditions(emd);
			doNonRxCodes(emd);
			doAssociations(emd);
			doRxCodes(emd);
			
			log.info("Completed Episode " + emd.getEpisode_id() + " " + new Date());
		}
		

		
	}
	
	
	/**
	 * get all global E&M codes
	 */
	private void doAllEMCodes () {
		
		
		Session session = factory.openSession();
		Transaction tx = null;
	
		try{

			tx = session.beginTransaction();
		
			String sql = "SELECT c.VALUE, c.TYPE_ID, c.DESCRIPTION, c.GROUP_ID, g.NAME FROM code c " +
							"JOIN global_em_201503 e ON e.VALUE = c.VALUE " +
							"AND e.TYPE_ID = c.TYPE_ID " +
							"JOIN `group` g on c.GROUP_ID = g.ID";
			Query query = session.createSQLQuery(sql);

			@SuppressWarnings("unchecked")
			List<Object[]> list = query.list();
			for(Object[] arr : list){
				emCodeList.add((String) arr[0]);
	        }
			
			
			tx.commit();

			
		} catch (HibernateException e) {
		
			if (tx != null) tx.rollback();
			e.printStackTrace();
		
		} finally {
			
			session.close(); 

		}
	
		
	}
	
	
	
	/**
	 * handle all tr, cm, dx, px and sp codes
	 * @param emd
	 */
	private void doNonRxCodes(EpisodeMetaData emd) {
		
		// set repository for all code query results
		cqr = new ArrayList<CodeQueryResult>();
		
		// build this episode's look up maps
		tcbe = new HashMap<String, HashMap<String, ArrayList<TriggerCodeMetaData>>>();		
		emd.setTrigger_code_by_ep(tcbe);
		
		Session session = factory.openSession();
		Transaction tx = null;
	
		try{

			tx = session.beginTransaction();
		
			String sql = "SELECT DISTINCT e.FUNCTION_ID, e.CODE_VALUE, e.CODE_TYPE_ID, c.DESCRIPTION, c.GROUP_ID, g.NAME AS group_name, e.SUBTYPE_ID, " + 
							"e.COMPLICATION, e.CORE, e.SUFFICIENT, e.PAS, e.RX_FUNCTION, c.BETOS_CATEGORY, e.QUALIFYING_DIAGNOSIS " + 
							"FROM episode_to_code e  " + 
							"JOIN code c ON e.CODE_VALUE = c.VALUE  " + 
							"	AND e.CODE_TYPE_ID = c.TYPE_ID  " + 
							"	AND e.CODE_MULTUM_CATEGORY = c.MULTUM_CATEGORY  " + 
							"	AND e.EPISODE_ID=:epid  " + 
							"	AND e.CODE_TYPE_ID<>'RX' " + 
							"JOIN `group` g ON c.GROUP_ID = g.ID";
			Query query = session.createSQLQuery(sql);
			query.setParameter("epid", emd.getEpisode_id());

			@SuppressWarnings("unchecked")
			List<Object[]> list = query.list();
			for(Object[] arr : list){
	            cqr.add(codeQueryToObject(arr));
	        }
			
			
			// ICD-10 clone of above
			String sql2 = "SELECT DISTINCT e.FUNCTION_ID, e.CODE_VALUE, e.CODE_TYPE_ID, c.DESCRIPTION, c.GROUP_ID, g.NAME AS group_name, e.SUBTYPE_ID, " + 
					"e.COMPLICATION, e.CORE, e.SUFFICIENT, e.PAS, e.RX_FUNCTION, c.BETOS_CATEGORY, e.QUALIFYING_DIAGNOSIS " + 
					"FROM episode_to_code_icd10_5_3_006  e  " + 
					"JOIN code_icd10 c ON e.CODE_VALUE = c.VALUE  " + 
					"	AND e.CODE_TYPE_ID = c.TYPE_ID  " + 
					"	AND e.CODE_MULTUM_CATEGORY = c.MULTUM_CATEGORY  " + 
					"	AND e.EPISODE_ID=:epid  " + 
					"	AND e.CODE_TYPE_ID<>'RX' " + 
					"JOIN `group` g ON c.GROUP_ID = g.ID";
			Query query2 = session.createSQLQuery(sql2);
			query2.setParameter("epid", emd.getEpisode_id());

			
			@SuppressWarnings("unchecked")
			List<Object[]> list2 = query2.list();
			for(Object[] arr : list2){
				cqr.add(codeQueryToObject(arr));
			}


			tx.commit();

			
		} catch (HibernateException e) {
		
			if (tx != null) tx.rollback();
			e.printStackTrace();
		
		} finally {
			
			session.close(); 

		}
		
		
	
		for (CodeQueryResult _cqr : cqr) {
			
			switch (_cqr.getFunction_id()) {
			case "px":
				doPxCode(emd, _cqr);
				break;
			case "tr":
				doTriggerCode(emd, _cqr);
				break;
			case "dx":
				doDxCode(emd, _cqr);
				break;
			case "cm":
				doComplicationCode(emd, _cqr);
				break;
			case "sp":
				doSubTypeCode(emd, _cqr);
				break;	
			default:
				break;	
			}
		}
		
		
		
	}
	
	
	private CodeQueryResult codeQueryToObject (Object[] o) {
		
		CodeQueryResult _cqr = new CodeQueryResult();
		_cqr.setFunction_id((String) o[0]);
		_cqr.setCode_id((String) o[1]);
		_cqr.setType_id((String) o[2]);
		_cqr.setCode_name((String) o[3]);
		_cqr.setGroup_id((String) o[4]);
		_cqr.setGroup_name((String) o[5]);
		_cqr.setSub_type_group_id((String) o[6]);
		_cqr.setComplication_type((Integer) o[7]);
		_cqr.setCore((Boolean) o[8]);
		_cqr.setSufficient((Boolean) o[9]);
		_cqr.setPas((Boolean) o[10]);
		_cqr.setRx_assignment((String) o[11]);
		_cqr.setBetos_category((String) o[12]);
		_cqr.setQualifying_diagnosis((Boolean) o[13]);
		return _cqr;
		
	}
 
	
	/**
	 * handle each sub-type
	 * @param emd - the episode metadata being built
	 * @param _cqr - the query result row object
	 */
	private void doSubTypeCode(EpisodeMetaData emd, CodeQueryResult _cqr) {
				
		SubTypeCodeMetaData _stmd = new SubTypeCodeMetaData();
		emd.getSub_type_codes().add(_stmd);
		doCommonCodeMapping(_stmd, _cqr);
		
	}
	
	

	/**
	 * load all Rx codes for a given episode
	 * @param emd
	 */
	private void doRxCodes(EpisodeMetaData emd) {
		
		
		cqr = new ArrayList<CodeQueryResult>();
		
		Session session = factory.openSession();
		Transaction tx = null;
	
		try{

			tx = session.beginTransaction();
		
			String sql = "SELECT DISTINCT e.FUNCTION_ID, c.VALUE, e.CODE_TYPE_ID, c.DESCRIPTION, e.CODE_VALUE, 'rx group', e.SUBTYPE_ID, e.COMPLICATION, e.CORE AS core, " +
							"e.SUFFICIENT, e.PAS, e.RX_FUNCTION, c.BETOS_CATEGORY, e.QUALIFYING_DIAGNOSIS " +
							"FROM episode_to_code e " +
							"JOIN ndc_to_rxcui nr ON e.CODE_VALUE = nr.rxcui " +
							"JOIN code_ndc c ON nr.ndcClean = c.VALUE " +
							"AND e.CODE_TYPE_ID = c.TYPE_ID " + 
							"AND e.CODE_MULTUM_CATEGORY = c.MULTUM_CATEGORY " + 
							"AND e.EPISODE_ID = :epid " + 
							"AND e.CODE_TYPE_ID = 'RX'";
			Query query = session.createSQLQuery(sql);
			query.setParameter("epid", emd.getEpisode_id());

			@SuppressWarnings("unchecked")
			List<Object[]> list = query.list();
			for(Object[] arr : list){
	            cqr.add(codeQueryToObject(arr));
	        }
			
			
			tx.commit();

			
		} catch (HibernateException e) {
		
			if (tx != null) tx.rollback();
			e.printStackTrace();
		
		} finally {
			
			session.close(); 

		}
		

		for (CodeQueryResult _cqr : cqr) {
			RxCodeMetaData _rxmd = new RxCodeMetaData();
			emd.getRx_codes().put(_cqr.getCode_id(), _rxmd);
			doCommonCodeMapping(_rxmd, _cqr);
			
			_rxmd.setRx_assignment(_cqr.getRx_assignment());
			
		}
		
		
	}

	
	/**
	 * load all associations for the given episode
	 * @param emd
	 */
	private void doAssociations(EpisodeMetaData emd) {
		
		Session session = factory.openSession();
		Transaction tx = null;
	
		try{

			tx = session.beginTransaction();
		
			String hql = "FROM AssociationDefinition WHERE PRIMARY_EPISODE_ID = :epid";
			Query query = session.createQuery(hql);
			query.setParameter("epid", emd.getEpisode_id());

			@SuppressWarnings("unchecked")
			List<AssociationDefinition> results = query.list();

			for (AssociationDefinition _AD : results) {
				
				AssociationMetaData asmd = new AssociationMetaData();
				emd.getAssociations().add(asmd);
				asmd.setEpisode_id(_AD.getSECONDARY_EPISODE_ID());
				asmd.setEpisode_acronym(getEpAcronym(_AD.getSECONDARY_EPISODE_ID()));
				asmd.setSubsidiary_to_procedural(_AD.getSUBSIDIARY_TO_PROC());
				asmd.setAss_type(_AD.getASSOCIATION());
				asmd.setAss_level(Integer.parseInt(_AD.getLEVEL()));
				asmd.setAss_start_day(_AD.getSTART_DAY());
				asmd.setAss_end_day(_AD.getEND_DAY());
				
			}
			


			tx.commit();

		
		} catch (HibernateException e) {
		
			if (tx != null) tx.rollback();
			e.printStackTrace();
		
		} finally {
			
			session.close(); 

		}
		
	}
	

	/**
	 * map each complication code for an episode
	 * @param emd - the episode metadata being built
	 * @param _cqr - the query result row object
	 */
	private void doComplicationCode(EpisodeMetaData emd, CodeQueryResult _cqr) {
				
		ComplicationCodeMetaData tcmd = new ComplicationCodeMetaData();
		emd.getComplication_codes().add(tcmd);
		doCommonCodeMapping(tcmd, _cqr);
		
		tcmd.setComplication_type( (_cqr.getComplication_type() == null ? 1 : _cqr.getComplication_type() ));
	
	}
	
	
	/**
	 * get all of the diagnosis codes for a given episode
	 * @param emd - the episode metadata being built
	 * @param _cqr - the query result row object
	 */
	private void doPxCode(EpisodeMetaData emd, CodeQueryResult _cqr) {
				
		PxCodeMetaData tcmd = new PxCodeMetaData();
		emd.getPx_codes().add(tcmd);
		doCommonCodeMapping(tcmd, _cqr);
				
		tcmd.setCore( (_cqr.getCore() != null  &&  _cqr.getCore().equals(Boolean.TRUE)) ? true : false);
		tcmd.setSufficient( (_cqr.getSufficient() != null  &&  _cqr.getSufficient().equals(Boolean.TRUE)) ? true : false);
		tcmd.setPas( (_cqr.getPas() != null && _cqr.getPas().equals(Boolean.TRUE)) ? true : false);		

	}

	
	/**
	 * get all of the diagnosis codes for a given episode
	 * @param emd - the episode metadata being built
	 * @param _cqr - the query result row object
	 */
	private void doDxCode(EpisodeMetaData emd, CodeQueryResult _cqr) {
		
		DxCodeMetaData tcmd = new DxCodeMetaData();
		emd.getDx_codes().add(tcmd);
		doCommonCodeMapping(tcmd, _cqr);
		
	}
	
	
	/**
	 * map a trigger code for an episode
	 * trigger codes must be placed in hash maps so EC can do keyed lookups
	 * @param emd - the episode metadata being built
	 * @param _cqr - the query result row object
	 */
	private void doTriggerCode(EpisodeMetaData emd, CodeQueryResult _cqr) {

				
		TriggerCodeMetaData tcmd = new TriggerCodeMetaData();
		emd.getTrigger_codes().add(tcmd);
		doCommonCodeMapping(tcmd, _cqr);
		tcmd.setQualifying_diagnosis( (_cqr.getQualifying_diagnosis()  == null   ||  _cqr.getQualifying_diagnosis().equals(Boolean.FALSE) ? "0" : "1") );

		
		if (tcmd.getType_id().equals("CPT") ) 
			tcmd.setType_id("PX"); 
		if (tcmd.getType_id().equals("HCPC") )
			tcmd.setType_id("PX");
			
		if (tcmd.getCode_id()==null || tcmd.getCode_id().isEmpty() || tcmd.getType_id()==null || tcmd.getType_id().isEmpty()) {
			return;
		}
			
		HashMap<String, ArrayList<TriggerCodeMetaData>> hm = null;
		if (tcbe.containsKey(tcmd.getType_id())) {
			hm = tcbe.get(tcmd.getType_id());
		} else {
			hm = new HashMap<String, ArrayList<TriggerCodeMetaData>>();
			tcbe.put(tcmd.getType_id(), hm);
		}
			
		ArrayList<TriggerCodeMetaData> hs = null;
		if (hm.containsKey(tcmd.getCode_id())) {
			hs = hm.get(tcmd.getCode_id());
		} else {
			hs = new ArrayList<TriggerCodeMetaData>();
			hm.put(tcmd.getCode_id(), hs);
		}
			
		hs.add(tcmd);
			
		

	}
	
	
	private void doCommonCodeMapping (EpisodeCodeBase cbmd, CodeQueryResult _cqr) {
		cbmd.setCode_id(_cqr.getCode_id());
		cbmd.setType_id(_cqr.getType_id());
		cbmd.setSpecific_type_id("");
		//cbmd.setCode_name(_cqr.getCode_name());
		cbmd.setGroup_id(_cqr.getGroup_id());
		//cbmd.setGroup_name(_cqr.getGroup_name());
	}

	
	/**
	 * build an episode's trigger conditions  -  facility type 'EP' and non-EP
	 * @param emd - episode metadata in progress
	 */
	private void doTriggerConditions(EpisodeMetaData emd) {
		doTriggerConditionsForNonEP(emd);
		doTriggerConditionsForEP(emd);
	}
	
	
	/**
	 * build an episode's trigger conditions where facility type is not 'EP'
	 * @param emd
	 */
	private void doTriggerConditionsForNonEP(EpisodeMetaData emd)	{
		
		Session session = factory.openSession();
		Transaction tx = null;
	
		try{

			tx = session.beginTransaction();
		
			String sql = "SELECT t.FACILITY_TYPE, "
					+ "if(t.PX_CODE_POSITION='None', '', t.PX_CODE_POSITION), "
					+ "if(t.SERVICE_TYPE='None', '', t.SERVICE_TYPE), "
					+ "t.LOGICAL_OPERATOR, "
					+ "if(t.DX_CODE_POSITION='None', '' , t.DX_CODE_POSITION), "
					+ "t.REQ_CONF_CLAIM, t.DUAL_SERVICE_MIN, t.DUAL_SERVICE_MAX, t.END_OF_STUDY "
					+ "FROM episode e "
					+ "JOIN episode_trigger t on e.EPISODE_ID=t.EPISODE_ID "
					+ "AND e.EPISODE_ID=:epid "
					+ "ORDER BY t.FACILITY_TYPE";
			Query query = session.createSQLQuery(sql);
			query.setParameter("epid", emd.getEpisode_id());

			@SuppressWarnings("unchecked")
			List<Object[]> list = query.list();
			
			String facType;
			Boolean bEOS;
			for(Object[] arr : list) {
				
				facType = (String) arr[0];
	            if ( facType == null || facType.equals("EP") ) {}
	            
	            else {
	            	
	            	TriggerConditionMetaData tcmd = new TriggerConditionMetaData();
	            	tcmd.setFacility_type(facType);
	            	tcmd.setPx_code_position((String) arr[1]);
	            	tcmd.setEm_code_position((String) arr[2]);
	            	tcmd.setAnd_or((String) arr[3]);
	            	tcmd.setDx_code_position((String) arr[4]);
	            	tcmd.setReq_conf_code( arr[5]  ==  null  ?  false : (Boolean) arr[5]);
	            	tcmd.setMin_code_separation( Integer.toString((Integer) arr[6]) );
	            	tcmd.setMax_code_separation( Integer.toString((Integer) arr[7]) );
	            	bEOS = (Boolean) arr[8];
	            	if (bEOS != null && bEOS) 
	            		tcmd.setMax_code_separation("EOS");
	            	
	            	emd.getTrigger_conditions().add(tcmd);
	            	emd.getTrigger_conditions_by_fac().put(tcmd.getFacility_type(), tcmd);
	            	
	            }
	        }
			
			
			tx.commit();

			
		} catch (HibernateException e) {
		
			if (tx != null) tx.rollback();
			e.printStackTrace();
		
		} finally {
			
			session.close(); 

		}
		
	}
	
	
	
	private void doTriggerConditionsForEP(EpisodeMetaData emd)	{
		
		Session session = factory.openSession();
		Transaction tx = null;
	
		try{

			tx = session.beginTransaction();
		
			String sql = "SELECT elt.LOOKBACK_EPISODE_ID, et.REQ_CONF_CLAIM, et.DUAL_SERVICE_MIN, et.DUAL_SERVICE_MAX, et.END_OF_STUDY "
					+ "FROM episode_lookback_trigger elt "
					+ "JOIN episode_trigger et ON et.episode_id=elt.episode_id "
					+ "AND et.facility_type='EP' "
					+ "AND elt.episode_id=:epid";
			Query query = session.createSQLQuery(sql);
			query.setParameter("epid", emd.getEpisode_id());

			@SuppressWarnings("unchecked")
			List<Object[]> list = query.list();
			
			Boolean bEOS;
			for(Object[] arr : list) {
	            	
				TriggerConditionMetaData tcmd = new TriggerConditionMetaData();
				tcmd.setFacility_type("EP");
				tcmd.setEpisode_id((String) arr[0]);
				tcmd.setReq_conf_code((Boolean) arr[1]);
				tcmd.setMin_code_separation( Integer.toString((Integer) arr[2]) );
				tcmd.setMax_code_separation( Integer.toString((Integer) arr[3]) );
				bEOS = (Boolean) arr[4];
				if (bEOS != null && bEOS) 
					tcmd.setMax_code_separation("EOS");
	            	
				emd.getTrigger_conditions().add(tcmd);
				emd.getTrigger_conditions_ep_trig().put(tcmd.getEpisode_id(), tcmd);
	            	
	        }
			
			
			tx.commit();

			
		} catch (HibernateException e) {
		
			if (tx != null) tx.rollback();
			e.printStackTrace();
		
		} finally {
			
			session.close(); 

		}
		
	}
	
	/**
	 * get acronym for a previously constructed episode
	 * @param epid
	 * @return
	 */
	private String getEpAcronym(String epid) {
		String sR = "";
		for (EpisodeMetaData _emd : epList) {
			if (_emd.getEpisode_id().equals(epid)) {
				sR = _emd.getEpisode_acronym();
				break;
			}	
		}
		return sR;
	}
	
	
	
	private void initialize (String resource) {
		
		// resource string should start with sql://
		if ( ! resource.toLowerCase().startsWith(SQL_URL_PREFIX)) {
			throw new IllegalStateException("Invalid resource for metadata.  URL shoudl start with 'sql://'");
		}
		
		schemaName = resource.substring(SQL_URL_PREFIX.length()).trim();
		
		cList.add(EpisodeDefinition.class);
		cList.add(EpisodeTriggerDefinition.class);
		cList.add(EpisodeToCodeDefinition.class);
		cList.add(EpisodeToCodeICD10.class);		// ICD-10
		cList.add(CodeDefinition.class);
		cList.add(Code10Definition.class);			// ICD-10
		cList.add(GroupDefinition.class);
		cList.add(AssociationDefinition.class);
		
		h = new HibernateHelper(cList, "prd", schemaName);
		factory = h.getFactory("prd", schemaName);
		
	}
	
	public static final String SQL_URL_PREFIX = "sql://";


	
	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {

		Date start = new Date();
		bDebug = true;
		loadParameters(args);
	
		new EpisodeMetaDataFromSQL(parameters.get("IP_TABLE"));
		 
		log.info("Started: " + start + " Ended: " + new Date());
		
	}


	
	private static void loadParameters (String[] args) {
		// load any default parameters from the default parameter array
		for (int i = 0; i < parameterDefaults.length; i++) {
			parameters.put(parameterDefaults[i][0], parameterDefaults[i][1]);
		}
		// overlay or add any incoming parameters
		for (int i = 0; i < args.length; i++) {
			parameters.put(args[i].substring(0, args[i].indexOf('=')), args[i].substring(args[i].indexOf('=')+1)) ;
		}
		
	}
	
	static HashMap<String, String> parameters = RunParameters.parameters;
	static String [][] parameterDefaults = {
			{"IP_TABLE", "sql://episode_builder-5.3.006"},
			{"configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\"}
	};
	
	
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	private static org.apache.log4j.Logger log = Logger.getLogger(EpisodeMetaDataFromSQL.class);
	

}
