




public class MongoInputCollectionSet {
	
	
	private HashMap<String, String> parameters;
	
	
	private String sClaimColl;
	private String sRxColl;
	private String sMemberColl;
	private String sEnrollColl;
	private String sProviderColl;
	
	private String dbUrl;
	private String schema;
	//private String dbUser;
	//private String dbPw;
	
	
	private Mongo mongoClient;
	private DB db;
	
	
	
	public MongoInputCollectionSet (HashMap<String, String> parameters) {
		this.parameters = parameters;
		initialize();
	}
	
	void initialize () {
		
		// define the input table
		StringBuffer sb = new StringBuffer();
		sb.append(parameters.get("clientID") == null ? "Test"  :  parameters.get("clientID"));
		sb.append('_');
		sb.append(parameters.get("runname") == null ?  sDefaultRunName : parameters.get("runname").replace(' ', '_'));
		String sCollPrefix = sb.toString();
		String sCollSuffix = parameters.get("rundate") == null ?  "20141231" : parameters.get("rundate");
				
		sClaimColl = sCollPrefix + "ClaimServLine" + sCollSuffix;	
		sRxColl = sCollPrefix + "ClaimRx" + sCollSuffix;	
		sMemberColl = sCollPrefix + "PlanMember" + sCollSuffix;	
		sEnrollColl = sCollPrefix + "Enrollment" + sCollSuffix;	
		sProviderColl = sCollPrefix + "Provider" + sCollSuffix;	
				
		String env = parameters.get("mongo") == null ?  "md1" :  parameters.get("mongo");

		ConnectParameters connParms = new ConnectParameters(env);
		dbUrl = connParms.getDbUrl();
		schema = connParms.getSchema();
		//dbUser = connParms.getDbUser();
		//dbPw = connParms.getDbPw();
				
		log.info("Initializing " + "ClaimServLine" + " ==> " + new Date());
		log.info(">>>  Connection information - U: " + dbUrl + "; S: " + schema + "; C: " + sCollPrefix + "?" + sCollSuffix); 
			    

		try {
			mongoClient = new MongoClient(dbUrl);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
				
		db = mongoClient.getDB(schema);
		//coll = db.getCollection(sMemberColl);
	}
	
	
	public DB getDb() {
		return db;
	}

	public String getsClaimColl() {
		return sClaimColl;
	}

	public String getsRxColl() {
		return sRxColl;
	}

	public String getsMemberColl() {
		return sMemberColl;
	}

	public String getsEnrollColl() {
		return sEnrollColl;
	}

	public String getsProviderColl() {
		return sProviderColl;
	}

	public String getsDefaultRunName() {
		return sDefaultRunName;
	}


	private String sDefaultRunName = "test";
	
	private static org.apache.log4j.Logger log = Logger.getLogger(MongoInputCollectionSet.class);

}
