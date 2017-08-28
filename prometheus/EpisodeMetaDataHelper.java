




public class EpisodeMetaDataHelper {
	
	
	
	static private HashMap<String, SerializedMetaData> mdList = new HashMap<String, SerializedMetaData>();
	
	
	
	static public SerializedMetaData getMetaData (String sResource) {
		
		SerializedMetaData _md = null;
		
		// first look in cache
		if (mdList.containsKey(sResource)) {
			_md = mdList.get(sResource);
			log.debug("MetaData loaded from cache");
		}
		
		// then look in ecr database
		else if (loadSerializedMetaData(sResource)) {
			_md = mdList.get(sResource);
			log.info("MetaData loaded from metadata store");
		}
		// load it from source as a last resort
		else if (sResource.toLowerCase().startsWith(EpisodeMetaDataFromSQL.SQL_URL_PREFIX)) {
			loadMetaDataFromDB(sResource);
			_md = mdList.get(sResource);
			log.info("MetaData loaded from SQL");
		}
		else {
			loadMetaDataFromXML(sResource);
			_md = mdList.get(sResource);
			log.info("MetaData loaded from XML");
		}
		
		log.debug("Providing EC with metadata containing " + _md.epList.size() + " episodes");
			
		return _md;
	}
	
	
	private static boolean loadSerializedMetaData(String sResource) {
		
		boolean bR = false;

		EpisodeMetaDataInterface mdIf = new EpisodeMetaDataFromStore(sResource);
		bR = mapMetaData(mdIf, sResource);
			
		return bR;
	}
	
	

	private static boolean loadMetaDataFromDB(String sResource) {
		
		boolean bR = false;

		EpisodeMetaDataInterface mdIf = new EpisodeMetaDataFromSQL(sResource);
		bR = mapMetaData(mdIf, sResource);
			
		return bR;
	}
	
	

	private static boolean loadMetaDataFromXML(String sResource) {
		
		boolean bR = false;

		EpisodeMetaDataInterface mdIf;
		try {
			mdIf = new EpisodeMetaDataFromXML(sResource);
			bR = mapMetaData(mdIf, sResource);
		} catch (ParserConfigurationException | SAXException | IOException e) {
			log.error("A problem occurred trying to obtain the metadata: " + e);
			throw new IllegalStateException(e);
		}
		
			
		return bR;
	}
	
	
	/**
	 * build complete metadata object and put in hash map for later use
	 * @param _mdIf
	 * @param sResource
	 */
	private static boolean mapMetaData (EpisodeMetaDataInterface _mdIf, String sResource) {
		
		boolean bR = false;
		
		SerializedMetaData _smd = new SerializedMetaData();
		_smd.epList = (ArrayList<EpisodeMetaData>) _mdIf.getAllEpisodes();
		_smd.emCodeList = (ArrayList<String>) _mdIf.getAllEMCodes();
		_smd.mdh = _mdIf.getMetaDataHeader();
		
		if (_smd.epList.size() > 0) {
			mdList.put(sResource, _smd);
			log.info("Cached metadata resource: " + sResource);
			bR = true;
		}
		else {
			log.info("Encountered invalid metadata object from " + sResource + " will try next storage medium");
			bR = false;
		}
		
		return bR;
		
	}


	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {

		Date start = new Date();
		loadParameters(args);
	
		EpisodeMetaDataHelper.getMetaData("sql://episode_builder-5.3.006");
		 
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
	
	private static org.apache.log4j.Logger log = Logger.getLogger(EpisodeMetaDataHelper.class);
	

}
