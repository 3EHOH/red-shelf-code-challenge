




public class GenericOutputMongo implements GenericOutputInterface {

	
	String dbUrl;
	String schema;
	String dbUser;
	String dbPw;
	
	Mongo mongoClient;
	DB db;
	DBCollection coll;
	//BasicDBObject doc;
	
	HashMap<String, String> parameters;
    
	public GenericOutputMongo () {
		this (RunParameters.parameters);
	}
	
	public GenericOutputMongo (HashMap<String, String> parameters) {
		this.parameters = parameters;
	}
	
	public GenericOutputMongo(String collectionName, HashMap<String, String> parameters) {
		sCollectionName = collectionName;
		this.parameters = parameters;
	}
 	
	@Override
	public String write(Object o) {
		
		// first time thru do file name, headers, etc.
		if (!bInitialized)
			initialize(o);
		
		BasicDBObject doc = doMap(o);
		
		
		doBatchInsert(doc);
		
		
		return doc.get( "_id" ).toString();
		
	}
	
	private String fld;
	
	DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	
	private BasicDBObject doMap(Object o) {
		
		doFinalize(o);

		BasicDBObject doc = new BasicDBObject();
		
		Field[] f =  o.getClass().getDeclaredFields();
		Object object = o;
		Object value;
		
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
				value = f[i].get(object);

				
				if (value == null) {
					continue;
				}
				
				fld = f[i].getName();
				
				
				if (f[i].getType().equals(String.class)) {
					doc = doField(doc, ((String) value).trim());
					//sbVals.append("'").append( ((String) value).replace("'", "''").replace('\\', '/') ).append("'").append(sDelimiter);
				}
				else if (f[i].getType().getName().equals("char")) {
					doc = doField(doc, ((Character) value).toString());
				}
				else if (f[i].getType().equals(Integer.class)  ||  f[i].getType().equals(int.class)) {
					doc = doField(doc, ((Integer) value).toString());
				}
				else if (f[i].getType().equals(Double.class)  ||  f[i].getType().equals(double.class)) {
					doc = doField(doc, ((Double) value).toString());
				}
				else if (f[i].getType().equals(Long.class)  ||  f[i].getType().equals(long.class)) {
					doc = doField(doc, ((Long) value).toString());
				}
				else if (f[i].getType().equals(BigDecimal.class)) {
					doc = doField(doc, ((BigDecimal) value).toString());
				}
				else if (f[i].getType().equals(Boolean.class)  ||  f[i].getType().equals(boolean.class)) {
					doc = doField(doc, ((Boolean) value).toString());
				}
				else if (f[i].getType().equals(Date.class)) {
					doc = doField(doc, (sdf.format((Date) value)));
				}
				else if (f[i].getType().equals(List.class)  ||  f[i].getType().equals(ArrayList.class)) {
					
					Field stringListField = o.getClass().getDeclaredField(f[i].getName());
			        ParameterizedType stringListType = (ParameterizedType) stringListField.getGenericType();
			        Class<?> stringListClass = (Class<?>) stringListType.getActualTypeArguments()[0];

			        doc = doListField(doc,  value, f[i].getName(), stringListClass);
				}
				else if (f[i].getType().equals(HashMap.class)) {
					doc = doHashMapField(doc, f[i], o);
				}
				else if (f[i].getType().equals(StringBuffer.class)) {
					doc = doField(doc, ((StringBuffer) value).toString());
				}
				else {
					log.warn(f[i].getName() + " not a known class - is " + f[i].getType());
					try {
						doc = doField(doc, (String) value);
					}
					catch (java.lang.ClassCastException e) {
						log.error("Could not map " + value + " for " + f[i].getName());
					}
				}
			} 
			catch (IllegalArgumentException | IllegalAccessException e) {
				// do nothing
			} catch (NoSuchFieldException e) {
				e.printStackTrace();
			} catch (SecurityException e) {
				e.printStackTrace();
			} 
		}
		
		return doc;
		
	}
	
	private BasicDBObject doField (BasicDBObject doc, String s) {
		//doc.append(fld, s);
		doc.put(fld, s);
		return doc;
	}
	


	
	
	private BasicDBObject doHashMapField(BasicDBObject doc, Field field, Object o) {
		// TODO Auto-generated method stub
		return doc;
	}


	protected BasicDBObject doListField (BasicDBObject doc,  Object o, String fldName, Class<?> stringListClass) {
		
		//doc.put(fld, o);
		List<BasicDBObject> op = new ArrayList<BasicDBObject>();
		
		//log.info("Mapping list field " + fldName);
		
		// handle strings
		if (stringListClass.toString().contains("lang.String")) {
			StringBuffer sb = new StringBuffer();
			String s = (String) doc.get(fldName);
			if (s != null && s.length() > 0)
				sb.append(s);
			for(Object oList:(List<?>) o){
				s = oList.toString();
				if (s != null && s.length() > 0)
					sb.append(s).append(';');
			}
			doc.put(fldName, sb.toString());
		}
		else{
			for(Object oList:(List<?>) o){
				op.add(doMap (oList));
			}
			doc.put(fldName, new BasicDBObject(fldName, op));
		}
		
		
		return doc;
		
	}
	
	//private BasicDBObject sbV;
	

	private void doBatchInsert (BasicDBObject doc) {
		
		//getConnection();
		
		coll.insert(doc);
		
		
		//log.info(sb);
		
		
	}
	
	
	private void doFinalize (Object o) {
		// look for a finalize method in the object
		try {
			Method mFine = o.getClass().getDeclaredMethod("finalout");
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
	
	
	
	
	private void initialize(Object o) {
		
		// define the output table
		if (sCollectionName == null)  {
		
			if (parameters.get("rundate") == null) {
				Date date = Calendar.getInstance().getTime();
				DateFormat formatter = new SimpleDateFormat("yyyy_MM_dd_hhmm");
				sRunDate = formatter.format(date);
				parameters.put("rundate", sRunDate);
			}
			else
				sRunDate = parameters.get("rundate");
		
			sCollectionName = ( parameters.get("clientID") == null  ||  parameters.get("runname") == null )  ?  sDefaultRunName 
				+ o.getClass().getName().substring(o.getClass().getName().lastIndexOf(".") + 1) + sRunDate : 
				parameters.get("clientID").replace(' ', '_') + "_" + parameters.get("runname").replace(' ', '_') 
				+ o.getClass().getName().substring(o.getClass().getName().lastIndexOf(".") + 1) + sRunDate;
		
		}
		
		// look for a getPrimaryKey method in the object
		try {
			mKey = o.getClass().getDeclaredMethod("getPrimaryKey");
		} catch (NoSuchMethodException e2) {
			// nothing to do
		} catch (SecurityException e2) {
			// nothing to do
		}
		
		
				 
		String env = parameters.get("mongo") == null ?  "md1" :  parameters.get("mongo");

		ConnectParameters connParms = new ConnectParameters(env);
		dbUrl = connParms.getDbUrl();
		schema = connParms.getSchema();
		dbUser = connParms.getDbUser();
		dbPw = connParms.getDbPw();
		
		log.info("Initializing " + o.getClass().getName().substring(o.getClass().getName().lastIndexOf('.') + 1) + " ==> " + new Date());
		log.info(">>>  Connection information - U: " + dbUrl + "; S: " + schema + "; I: " + dbUser + " P:" + "********"); 
	    

		try {
			mongoClient = new MongoClient(dbUrl);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
		db = mongoClient.getDB(schema);
		coll = db.getCollection(sCollectionName);
		
		
		bInitialized = true;
		
	}
	
	
	Method mKey;
	
	/*
	private String checkForPrimaryKey(Object obj, Field f) {
		if(mKey == null)
			return "";
		try {
			if ( ((String) mKey.invoke(obj)).equalsIgnoreCase(f.getName()))
				return " PRIMARY KEY ";
			else
				return "";
		} catch (SecurityException e) {
			return "";
		} catch (IllegalAccessException e) {
			return "";
		} catch (IllegalArgumentException e) {
			return "";
		} catch (InvocationTargetException e) {
			return "";
		}
	}
	*/



	boolean bInitialized = false;

	String sDefaultRunName = "Test";
	String sDelimiter = ",";
	String sRunDate;
	String sCollectionName;
	int iBatchIx = 0;


	
	private static org.apache.log4j.Logger log = Logger.getLogger(GenericOutputMongo.class);

	@Override
	public void close() {	}
	

}
