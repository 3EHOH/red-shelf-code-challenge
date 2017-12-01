




public class ClaimHelper {
	
	
	private HashMap<String, String> parameters;

	private String dbUrl;
	private String schema;
	//private String dbUser;
	//private String dbPw;
	
	MongoInputCollectionSet micky;
	
	
	Mongo mongoClient;
	DB db;
	DBCollection coll;
	DBCursor cursor;
	BasicDBObject query;
	private String sMemberColl;

	
	// this constructor assumes you want to work with the static parameters
	// not thread safe, but usually won't matter
	public ClaimHelper () {
		this.parameters = RunParameters.parameters;
		initialize();
	}
	
	// this constructor uses instance parameters
	// so it's thread safe
	public ClaimHelper (HashMap<String, String> parameters) {
		this.parameters = parameters;
		initialize();
	}
	
	/*
	public ArrayList<String> getMemberList() {

		
		DBCollection dBCollection = db.getCollection(sMemberColl);
		@SuppressWarnings("unchecked")
		List<String> members =  dBCollection.distinct("member_id");
		
		ArrayList<String> rList = new ArrayList<String>();
		rList.addAll(members);
		return rList;
		
	}
	*/
	
	
	public ArrayList<String> getParallel ()  {
		
		log.info("Starting getParallel: " + new Date());
		
		HashSet <String> rSet = new HashSet<String>();
		ArrayList<String> rList = new ArrayList<String>();
		
		ParallelScanOptions parallelScanOptions = ParallelScanOptions
		        .builder()
		        .numCursors(10)
		        .batchSize(10000)
		        .build();

		List<Cursor> cursors = coll.parallelScan(parallelScanOptions);
		int i = 0;
		for (Cursor pCursor: cursors) {
		    while (pCursor.hasNext()) {
		    	i++;
		    	if (i % 10000 == 0)
		    		log.info(new Date() + " got: " + i );
		        //System.out.println((pCursor.next()));
		    	rSet.add( (String) pCursor.next().get("member_id"));
		    }
		}
		
		for (String s : rSet) {
			rList.add(s);
		}
		
		log.info("Completed getParallel: " + new Date());
	
		return rList;

	}
	
	
	
	public ArrayList<String> getMemberList() {
		
		log.info("Starting getMemberList: " + new Date());
		
		DBCollection coll = db.getCollection(sMemberColl);
		
		// create our pipeline operations, first with the $match
		//DBObject match = new BasicDBObject("$match", new BasicDBObject("type", "airfare"));

		// build the $projection operation
		DBObject fields = new BasicDBObject("member_id", "$member_id");
		//DBObject fields = new BasicDBObject("member_id", "1");
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields );

		// Now the $group operation
		//DBObject groupFields = new BasicDBObject( "_id", "$member_id");
		//groupFields.put("average", new BasicDBObject( "$avg", "$amount"));
		//DBObject group = new BasicDBObject("$group", groupFields);

		// Finally the $sort operation
		//DBObject sort = new BasicDBObject("$sort", new BasicDBObject("member_id", 1));
		
		AggregationOptions aggregationOptions = AggregationOptions.builder()
		        .batchSize(10000)
		        .outputMode(AggregationOptions.OutputMode.CURSOR)
		        .allowDiskUse(true)
		        .build();


		// run aggregation
		//List<DBObject> pipeline = Arrays.asList(project, group, sort);
		List<DBObject> pipeline = Arrays.asList(project);
		//AggregationOutput output = coll.aggregate(pipeline, aggregationOptions );
		Cursor cursor  = coll.aggregate(pipeline, aggregationOptions );
		
		int i = 0;
		int testLimit = parameters.get("testlimit") == null ? 0 : Integer.parseInt(parameters.get("testlimit"));
		ArrayList<String> rList = new ArrayList<String>();
		HashSet <String> rSet = new HashSet<String>();
		while (cursor.hasNext()) {
		    //System.out.println(cursor.next());
			i++;
			//String s = (String) cursor.next().get("member_id");
			//rSet.add(s);
			rSet.add((String) cursor.next().get("member_id"));
		    //if (i % 1000000 == 0) {
	    	//	log.info(new Date() + " got member " + s + " at element " + i );
		    //}
		    //if (rSet.size() % 1000000 == 0) {
	    	//	log.info(new Date() + " got member " + rSet.size() + " in list " + i );
		    //}
			if (testLimit > 0 && rSet.size() > testLimit)
				break;
		}
		
		cursor.close();

		i = 0;
		log.info("Found " + rSet.size() + " members");
		for (String s : rSet) {
			i++;
			rList.add(s);
			if (i % 1000000 == 0) {
	    		log.info(new Date() + " listing member " + s + " at element " + i );
		    }
		}
		
		
		log.info("Completed getMemberList: " + new Date());
		
		return rList;
		
	}
	
	
	

	private void initialize ()  {
		
		// define the input table
		
		micky = new MongoInputCollectionSet(parameters);
		db = micky.getDb();		

		sMemberColl = micky.getsMemberColl();
		
		String env = parameters.get("mongo") == null ?  "md1" :  parameters.get("mongo");

		ConnectParameters connParms = new ConnectParameters(env);
		dbUrl = connParms.getDbUrl();
		schema = connParms.getSchema();
		//dbUser = connParms.getDbUser();
		//dbPw = connParms.getDbPw();
		
		log.info("Initializing " + "ClaimServLine" + " ==> " + new Date());
		log.info(">>>  Connection information - U: " + dbUrl + "; S: " + schema + "; C: " + sMemberColl ); 
	    

		try {
			mongoClient = new MongoClient(dbUrl);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
		db = mongoClient.getDB(schema);
		coll = db.getCollection(sMemberColl);
		
		
	}
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ClaimHelper.class);

}
