




public class MetaDataSerializationTest extends EpisodeMetaDataAbstract {
	

	private List<Class<?>> cList = new ArrayList<Class<?>>();
	HibernateHelper h;
	SessionFactory factory;

	void process () {
		initialize();
		getMetaData();
	}
	
	/**
	 * get all metadata
	 */
	private void getMetaData () {
		
		log.info("Obtaining Episode Metadata");
		String EP_FILE = parameters.get("IP_TABLE");
		SerializedMetaData _smd = EpisodeMetaDataHelper.getMetaData(EP_FILE);
		log.info("Finish loading metadata");
		
		epList = _smd.epList;
		emCodeList = _smd.emCodeList;
		mdh = _smd.mdh;
		

		log.debug("Episode list is " + epList == null ? " NULL!" : epList.size() + " long");
		
		bDebug = true;
		outputCheck ();
		
		log.info("Finished checking metadata");
		
	}
	

	
	private void initialize () {
		
		cList.add(MetaDataStore.class);

		h = new HibernateHelper(cList, "prd", "ecr");
		factory = h.getFactory("prd", "ecr");
		
	}
	
	
	
	

	public static void main(String[] args) {

		log.info("MetaData load test started");
		MetaDataSerializationTest instance = new MetaDataSerializationTest();
		// get parameters and make them available to all
		RunParameters rp = new RunParameters();
		instance.parameters = rp.loadParameters(args, parameterDefaults);
		instance.process();
		log.info("MetaData load test completed");

	}

	
	HashMap<String, String> parameters = RunParameters.parameters;
	
	static String [][] parameterDefaults = {
		{"IP_TABLE", "sql://episode_builder-5.3.006"},
		{"configfolder", "C:/workspace/ECR_Analytics/trunk/EpisodeConstruction/src/"}
	};
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(MetaDataSerializationTest.class);
	
	

}
