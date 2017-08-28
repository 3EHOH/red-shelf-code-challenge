





public class GenericInputMongo implements GenericInputInterface {

	private boolean bInitialized;
	private Object o;
	Constructor<?> ctor = null;
	private ArrayList<Object> returnObj;

	String dbUrl;
	String schema;
	String dbUser;
	String dbPw;
	
	static Mongo mongoClient;
	DB db;
	DBCollection coll;
	DBCursor cursor;
	BasicDBObject query;
	private String sCollectionName;
	
	
	
	
	public GenericInputMongo (Object o) {
		this (o, RunParameters.parameters);
	}

	
	public GenericInputMongo (Object o, HashMap<String, String> parameters) {
		
		this.parameters = parameters;
		this.o = o;
		Constructor<?>[] ctors = o.getClass().getDeclaredConstructors();

		for (int i = 0; i < ctors.length; i++) {
		    ctor = ctors[i];
		    if (ctor.getGenericParameterTypes().length == 0)
			break;
		}

	}
	
	
	@Override
	public ArrayList<Object> read(String sFldName, String sValue) {
		
		// first time thru do file name, headers, etc.
		if (!bInitialized)
			initialize();
		
		returnObj = new ArrayList<Object>();
		
		query = new BasicDBObject(sFldName, sValue);

		// Try to read from Mongo
		
		//for (int i=0; i < iMongoRetries; i ++) {
			
			log.debug("Establishing Mongo cursor: " + cursor);
		
			cursor = coll.find(query);

			log.debug("Established Mongo cursor: " + cursor);
			
			try {
				while(cursor.hasNext()) {
					returnObj.add(generateObjectFromDoc(cursor.next()));
				}
		//		i = iMongoRetries + 1;
			} catch (MongoTimeoutException e) {
		//		if (i == iMongoRetries)
					throw new IllegalStateException ("mongo connection timeout");
			} finally {
				log.debug("Closing Mongo cursor: " + cursor);
				cursor.close();
				log.debug("Closed Mongo cursor: " + cursor);
			}
			
		//}
		
		return returnObj;
		
	}
	
	//private int iMongoRetries = 5;
	
	
	public Object generateObjectFromDoc (DBObject doc) {

		//System.out.println(cursor.next());
		
		Object object = getNewInstance();
		
		Field[] f =  o.getClass().getDeclaredFields();
		
		
		
		for (int i=0; i < f.length; i++) {
			try {
				if (f[i].getName().equals("serialVersionUID"))		// don't bother with any serialization UID's
					continue;
				if (f[i].getName().equals("errMgr"))				// or the error manager
					continue;
				if (Modifier.isTransient(f[i].getModifiers()))
					continue;
				if (Modifier.isStatic(f[i].getModifiers()))
					continue;
				
				f[i].setAccessible(true);
				
				
				fld = f[i].getName();
				
				if (doc.get(fld) == null)
					continue;
				
				if (f[i].getType().equals(String.class)) {
					f[i].set(object, ((String)doc.get(fld)).trim());
				}
				else if (f[i].getType().getName().equals("char")) {
					f[i].set(object, ((String)doc.get(fld)).charAt(0));
				}
				else if (f[i].getType().equals(Integer.class)  ||  f[i].getType().equals(int.class)) {
					f[i].set(object, Integer.parseInt((String)doc.get(fld)));
				}
				else if (f[i].getType().equals(Double.class)  ||  f[i].getType().equals(double.class)) {
					f[i].set(object, Double.parseDouble((String)doc.get(fld)));
				}
				else if (f[i].getType().equals(Long.class)  ||  f[i].getType().equals(long.class)) {
					f[i].set(object, Long.parseLong((String)doc.get(fld)));
				}
				else if (f[i].getType().equals(BigDecimal.class)) {
					f[i].set(object, new BigDecimal((String)doc.get(fld)));
				}
				else if (f[i].getType().equals(Boolean.class)  ||  f[i].getType().equals(boolean.class)) {
					f[i].set(object, ((String)doc.get(fld)).equalsIgnoreCase("true") ? true : false);
				}
				else if (f[i].getType().equals(Date.class)) {
					//f[i].set(object, new Date((String)doc.get(fld)));
					try {
						f[i].set(object, DateUtil.doParse(fld, (String)doc.get(fld)));
					} catch (ParseException e) {
						log.error("Problem parsing " + (String)doc.get(fld) + " into " + fld);
					}
				}
				else if (f[i].getType().equals(List.class)  ||  f[i].getType().equals(ArrayList.class)) {
					doListField(f[i], object, (BasicDBObject) doc);
				}
				else if (f[i].getType().equals(HashMap.class)) {
					//doHashMapField(f[i], o);
				}
				else {
					f[i].set(object, doc.get(fld));
				}
				
			} 
			catch (IllegalArgumentException | IllegalAccessException e) {
				// do nothing
			} 
		}
		
		doFinalize(object);
		
		return object;
		
		
	}
	
	private String fld;

	
	
	protected void doListField (Field f, Object o, BasicDBObject doc) throws IllegalArgumentException, IllegalAccessException {
		
		ArrayList<Object> outList=new ArrayList<Object>();
		
		if (doc.get(f.getName()) instanceof String) {
			
			String s = (String) doc.get(f.getName());
			for( String sx : s.split(";") ) {
				outList.add(sx);
			}
			
		}
		
		else {
			
			BasicDBObject innerdoc = (BasicDBObject) doc.get(f.getName());
			BasicDBList list = ( BasicDBList ) innerdoc.get( f.getName() );
		
			ParameterizedType innerObj = (ParameterizedType) f.getGenericType();
			Class<?> type = (Class<?>) innerObj.getActualTypeArguments()[0];
			String s = type.getName();
		
		
		
			// if it's just  alist of strings, do it the easy way
			if (s.contains("String")) {
				if (list != null && list.size() > 0) {
					for ( Object item : list) {
						outList.add((String) item);
					}
				}
			}
			// if it's something else, map it
			else {
				Object innerClass = getInnerClass(s);
				

				GenericInputMongo innermapper = null;
			
				try {
					innermapper = new GenericInputMongo(innerClass);
					if (list != null && list.size() > 0) {
						for ( Object item : list) {
							outList.add(innermapper.generateObjectFromDoc((BasicDBObject)item));
						}
					}
				} catch (Throwable e) {
					e.printStackTrace();
				}

			}
		}
		f.set(o, outList);
		

	}
	
	/**
	 * sorry, this sucks
	 * I could not get around type erasure for the object within a list to present it to a recursion of this class
	 * This list should be expanded as our construction model objects are adorned with other non-string objects
	 * @param s
	 * @return
	 */
	private Object getInnerClass (String s) {
		
		Object oR = null;
		String sR = s.lastIndexOf('.') > -1 ? s.substring(s.lastIndexOf('.') + 1) : s;
		
		switch (sR) {
		case "MedCode":
			oR = new MedCode();
			break;
		case "String":
			oR = new String();
			break;
		default:
			break;	
		}
		
		return oR;

	}
	
	
	
	
	private Object getNewInstance () {
		Object o = null;
		try {
		    ctor.setAccessible(true);
	 	    o = ctor.newInstance();
		} catch (InstantiationException x) {
		    x.printStackTrace();
	 	} catch (InvocationTargetException x) {
	 	    x.printStackTrace();
		} catch (IllegalAccessException x) {
		    x.printStackTrace();
		}
		return o;
	}
	
	

	private void doFinalize (Object o) {
		// look for a finalize method in the object
		try {
			Method mFine = o.getClass().getDeclaredMethod("finalin");
			mFine.invoke(o);
		} catch (NoSuchMethodException e2) {
			// nothing to do
		} catch (SecurityException e2) {
			// nothing to do
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
	}
	
	
	
	
	
	private void initialize ()  {
		
		// define the input table

		/*
		sCollectionName = parameters.get("runname") == null ?  sDefaultRunName 
				+ o.getClass().getName().substring(o.getClass().getName().lastIndexOf(".") + 1) + parameters.get("rundate") : 
					parameters.get("runname").replace(' ', '_') 
					+ o.getClass().getName().substring(o.getClass().getName().lastIndexOf(".") + 1) + parameters.get("rundate");
					*/
				
		sCollectionName = ( parameters.get("clientID") == null  ||  parameters.get("runname") == null )  ?  sDefaultRunName 
				+ o.getClass().getName().substring(o.getClass().getName().lastIndexOf(".") + 1) + parameters.get("rundate") : 
			parameters.get("clientID").replace(' ', '_') + "_" + parameters.get("runname").replace(' ', '_') 
				+ o.getClass().getName().substring(o.getClass().getName().lastIndexOf(".") + 1) + parameters.get("rundate");
		
		String env = parameters.get("mongo") == null ?  "md1" :  parameters.get("mongo");

		ConnectParameters connParms = new ConnectParameters(env);
		dbUrl = connParms.getDbUrl();
		schema = connParms.getSchema();
		dbUser = connParms.getDbUser();
		dbPw = connParms.getDbPw();
		
		log.info("Initializing " + o.getClass().getName().substring(o.getClass().getName().lastIndexOf('.') + 1) + " ==> " + new Date());
		log.info(">>>  Connection information - U: " + dbUrl + "; S: " + schema + "; I: " + dbUser); 
	    
		if (mongoClient == null) {
			try {
				log.debug("Establishing new Mongo connection");
				mongoClient = new MongoClient(dbUrl);
				log.debug("Established new Mongo connection: " + mongoClient);
			} catch (UnknownHostException e) {
				e.printStackTrace();
				throw new IllegalStateException(e);
			}
		} else {
			log.debug("Reusing Mongo connection: " + mongoClient);
		}
		db = mongoClient.getDB(schema);
		coll = db.getCollection(sCollectionName);
		
		
		bInitialized = true;
	}
	
	private String sDefaultRunName = "test";
	
	
	public static void main(String[] args) throws IOException {
		
		GenericInputMongo instance = new GenericInputMongo(new construction.model.ClaimServLine());
		
		// get parameters (if any)
		instance.loadParameters(args);
		
		// always validate for this run
		instance.parameters.put("validate", "yes");
			
		// process
		ArrayList<Object> p =  instance.read("member_id", "86708");
		//Collection<Provider> it = p.;
		for (Object o : p.toArray()) { 
			//goif.write(member);
			log.info((construction.model.ClaimServLine)o);
		}

	}
	
	HashMap<String, String> parameters;
	//HashMap<String, String> parameters = new HashMap<String, String>();
	String [][] parameterDefaults = {
			{"clientID", "Test"},
			{"runname", "Unit_Test_"},
			{"rundate", "20150709"}
	};
	
	/**
	 * load default parameters and 
	 * put any run arguments in the hash map as well
	 * arguments should take the form keyword=value (e.e., studybegin=20140101)
	 * @param args
	 */
	private void loadParameters (String[] args) {
		// load any default parameters from the default parameter array
		for (int i = 0; i < parameterDefaults.length; i++) {
			parameters.put(parameterDefaults[i][0], parameterDefaults[i][1]);
		}
		// overlay or add any incoming parameters
		for (int i = 0; i < args.length; i++) {
			parameters.put(args[i].substring(0, args[i].indexOf('=')), args[i].substring(args[i].indexOf('=')+1)) ;
		}
	}
	
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(GenericInputMongo.class);
	

}
