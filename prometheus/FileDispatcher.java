






public class FileDispatcher {
	
	
	private int chunksize = 10;
	private int sequence = 0;
	
	
	private HashMap<String, String> parameters;
	private String resourceName;
	private String objectName;
	private String collectionName;
	
	
	 public FileDispatcher (HashMap<String, String> parameters, ServerControl _sc) {
	    	
	    	this.parameters = parameters;
	    	this.resourceName = _sc.resourceName;
	    	this.objectName = _sc.objectName;
	    	this.collectionName = _sc.collectionName;
	    	
	    	iOpen = false;
	    	
	 }

    
    /**
     * Generate JSON
     * @param payload
     * @return
     */
    public synchronized String getNextMapRequest () {
    	
    	mapper = new ObjectMapper();
    	
    	request = new MappingRequest();
    	sequence++;
    	request.setSequence(sequence);
    	request.setJobUid(parameters.get("jobuid"));
    	request.setObjectName(objectName);
    	request.setMapName(parameters.get("mapname"));
    	request.setCollectionName(collectionName);
    	request.setFileName(resourceName);
    	
    	String s;
    	for (int i = 0; i < chunksize; i++) {
    		s = getNextThing();
    		if (s == null) {
    			if (request.getInput() == null)
    				log.info("Empty block will be ignored");
    			else
    				log.info("Partial block prepared with " + request.getInput().size() + " records");
    			break;
    		}
    		request.addToInput(s);
    	}
    	
    	if (request.getInput() == null || request.getInput().size() == 0)
    		return null;
    	
    	try {

			// Convert object to JSON string
			jsonInString = mapper.writeValueAsString(request);


		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

    	
    	return jsonInString;
    	
    }
    //StringBuffer clientFeed = new StringBuffer();
    ObjectMapper mapper;
    MappingRequest request;
    String jsonInString;
    
    
    
    private String getNextThing () {
    	
    	if (iClosed)
    		return null;
    	
    	if (!iOpen) {
    		inputFile = new InputManager(resourceName);
    		try {
    			inputFile.openFile();
    			inputFile.readFile();	// toss the header out
    		} catch (IOException | ZipException e) {
    			log.error("An error occurred while opening " + resourceName);
    		}
            iOpen = true;
            log.info("Opened " + resourceName);
    	}    
         
    	String line = null;
    	// read a line from the input file
    	try {
    		line = inputFile.readFile();
    		request.setStatus(MappingRequest.FR_STATUS_CONTINUE);
    	} catch (IOException e) {
    		log.error("An error occurred while reading " );
    	} 
    	
    	if (line == null)
			try {
				iOpen = false;
				iClosed = true;
				inputFile.closeFile();
				request.setStatus(MappingRequest.FR_STATUS_FILEEND);
				log.info("Closed " + resourceName);
			} catch (IOException e) {
				e.printStackTrace();
			}
    	
    	return line;
    	
    }
    	
   
    public int getChunksize() {
		return chunksize;
	}
    
    
	private boolean iOpen = false;
	private boolean iClosed = false;
    InputManager inputFile;
    
    static final String TAG_ACTION = "Action";
    static final String TAG_PAYLOAD = "Payload";
    
    
    private static org.apache.log4j.Logger log = Logger.getLogger(FileDispatcher.class);
	

}
