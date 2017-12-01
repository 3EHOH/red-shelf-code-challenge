





public class PostMappingController {
	
	
	HashMap<String, String> parameters;
	
	
	Mongo mongoClient;
	DB db;
	DBCursor cursor;
	BasicDBObject query;
	
	MongoInputCollectionSet micky;
	
	
	ClaimHelper hClaim;
	ArrayList<String> memberList;

	
	// build indexes for mongo
	// create member work queue
	// put all providers to SQL
	PostMappingController (HashMap<String, String> parameters) {
		this.parameters = parameters;
		initialize();
		log.info("Starting Mongo Index Build");
		buildMongoIndexes();
		log.info("Starting Member Work Queue Build");
		buildMemberWorkQueue();
		log.info("Starting Provider Table Build");
		mapProviders();
	}
	
	
	
	/**
	 * get all unique member id's from medical claims to set up a Q of member run units
	 */
	private void buildMemberWorkQueue ()  {
		
		if (parameters.get("jobuid") == null)
			throw new IllegalStateException("Post-mapping call did not set jobUid - must stop");
		
		if (parameters.get("env") == null)
			throw new IllegalStateException("Post-mapping call did not set env - must stop");
		
		if (parameters.get("runname") == null)
			throw new IllegalStateException("Post-mapping call did not set runname - must stop");
		
		hClaim = new ClaimHelper(parameters);
		memberList = hClaim.getMemberList();
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
		cList.add(JobStepQueue.class);
		
		HibernateHelper h = new HibernateHelper(cList, "ecr", "ecr");
		SessionFactory factory = h.getFactory("ecr", "ecr");
		
		Session session = factory.openSession();
		Transaction tx = null;
		session.setCacheMode(CacheMode.IGNORE);
        session.setFlushMode(FlushMode.COMMIT);
		
		try{
			
			JobStepQueue _JSQ = null;

			int iB = 0;
			tx = session.beginTransaction();
			
 			for (String _member : memberList) {
 				
 				_JSQ = new JobStepQueue(_member);
 				_JSQ.setJobUid(Long.parseLong(parameters.get("jobuid")));
 				_JSQ.setStepName(ProcessJobStep.STEP_NORMALIZATION);
 				_JSQ.setStatus(JobStepQueue.STATUS_READY);
 				_JSQ.setUpdated(new Date());
			    session.save(_JSQ);
			    
			    if( iB % HibernateHelper.BATCH_INSERT_SIZE == 0 ) {
             		session.flush();
             		session.clear();
                }
             	if(iB % 1000000 == 0) { 
       				log.info("executing member queue insert " + iB + " ==> " + new Date());
             	}
             	
             	iB++;
			    
			}
 			
 			tx.commit();
 						
			
		}catch (HibernateException e) {
			
			if (tx!=null) tx.rollback();
			e.printStackTrace();
			
		}finally {
			
			session.close(); 

			try {
				h.close("ecr", "ecr");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
			
		
	}
	
	
	/**
	 * all providers are desired in SQL tables, so do not subject them to run units
	 */
	private void mapProviders() {
		
		GenericInputMongo prvGetter = new GenericInputMongo (new Provider(), parameters);
		InputObjectOutputInterface oi = new InputObjectOutputHibSQL(parameters);
		Provider p;
		
		DBCollection coll = db.getCollection(micky.getsProviderColl());
		DBCursor cursor = coll.find();
		ArrayList<Provider> prvList = new ArrayList<Provider>();
		int i = 0;
		try {
		   while(cursor.hasNext()) {
		       p = (Provider) prvGetter.generateObjectFromDoc(cursor.next());
		       prvList.add(p);
		       if (i > 0 && i % HibernateHelper.BATCH_INSERT_SIZE == 0) {
		    	   oi.writeProviders(prvList);
		    	   prvList = new ArrayList<Provider>();
		       }
		       i++;
		   }
		} finally {
			oi.writeProviders(prvList);
			cursor.close();
		}

		
	}
	
	
	/**
	 * build all the indexes we will need to make the Mongo DB reads efficient
	 */
	private void buildMongoIndexes ()
	{
		
		DBObject indexOptions = new BasicDBObject("background", true);
		
		log.info("Building member index for medical claims");
		DBCollection coll = db.getCollection(micky.getsClaimColl());
		coll.createIndex(new BasicDBObject("member_id", 1), indexOptions);  
		
		log.info("Building claim index for medical claims");
		BasicDBObject cmp_ix = new BasicDBObject("member_id", 1);
		cmp_ix.append("claim_id", 1);
		coll.createIndex(cmp_ix, indexOptions);  

		log.info("Building billType index for medical claims");
		coll.createIndex(new BasicDBObject("type_of_bill", 1), indexOptions);  

		log.info("Building claimType index for medical claims");
		coll.createIndex(new BasicDBObject("claim_line_type_code", 1), indexOptions);  
		
		
		log.info("Building member index for Rx claims");
		coll = db.getCollection(micky.getsRxColl());
		coll.createIndex(new BasicDBObject("member_id", 1), indexOptions);  

		log.info("Building member index for Enrollments");
		coll = db.getCollection(micky.getsEnrollColl());
		coll.createIndex(new BasicDBObject("member_id", 1), indexOptions);  

		log.info("Building member index for Members");
		coll = db.getCollection(micky.getsMemberColl());
		coll.createIndex(new BasicDBObject("member_id", 1), indexOptions);  

		log.info("Building provider index for Providers");
		coll = db.getCollection(micky.getsProviderColl());
		coll.createIndex(new BasicDBObject("provider_id", 1), indexOptions);  
		
		
	}
	
	

	private void initialize ()  {

		micky = new MongoInputCollectionSet(parameters);
		db = micky.getDb();		
		
	}
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(PostMappingController.class);

}
