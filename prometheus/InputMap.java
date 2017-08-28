




public class InputMap {
	
	
	private String mapName;
	private String mapContents;
	private List<InputObjectMap> objectMaps = Collections.synchronizedList(new ArrayList<InputObjectMap>());
	private List<String> directives = new ArrayList<String>();
	
	private InputObjectMap iMap;
	private Object workObject;
	

	public InputMap(String mapContents) {
		this.mapContents = mapContents;
	}
	
	
	
	
	public InputMap getMapping () {
		
		//create ObjectMapper instance
		ObjectMapper objectMapper = new ObjectMapper();
		 
		//read JSON like DOM Parser
		JsonNode rootNode = null;
		try {
			rootNode = objectMapper.readTree(mapContents);
		} catch (IOException e) {
			log.error("IOException processing Json from map" + mapContents);
			e.printStackTrace();
		}
		JsonNode mapNode = rootNode.path("map");
		
		log.info("Loading map: " + mapNode.asText());
		 
		JsonNode idNode = mapNode.path("id");
		mapName = idNode.asText();
		//log.info(idNode.asText());
		
		
		Iterator<Entry<String, JsonNode>> nodeIterator = mapNode.fields();

		while (nodeIterator.hasNext()) {

		   Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) nodeIterator.next();

		   //log.info("key --> " + entry.getKey() + " value-->" + entry.getValue());
		   
		   if (entry.getKey().equals("id"))
			   continue;
		   
		   if (ojbNameCheckList.contains(entry.getKey())) {
			   doObjectMap(entry);
		   }
		   
		}
		
		return this;
		
	}
	
	
	
	public String getMapName() {
		return mapName;
	}




	public List<InputObjectMap> getObjectMaps() {
		return objectMaps;
	}




	private void doObjectMap(Entry<String, JsonNode> nodeEntry) {
		
		Iterator<Entry<String, JsonNode>> nodeIterator = nodeEntry.getValue().fields();
		
		iMap = new InputObjectMap();
		iMap.setObjectName(nodeEntry.getKey());
		
		objectMaps.add(iMap);

		while (nodeIterator.hasNext()) {

		   Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) nodeIterator.next();

		   //log.info("key ----> " + entry.getKey() + " value ---->" + entry.getValue());
		   
		  
		   
		   if (entry.getKey().equals("fields")) {
			   doFieldsMap(entry);
		   }

		   if (entry.getKey().equals("methods")) {
			   doMethodsMap(entry);
		   }
		   
		   if (entry.getKey().equals("exclusions")) {
			   doExclusions(entry);
		   }
		   
		   if (entry.getKey().equals("directives")) {
			   addDirectives(entry);
		   }


		}
		
	}
	
	private void doFieldsMap(Entry<String, JsonNode> nodeEntry) {
		
		JsonNode e = nodeEntry.getValue();
		

		Iterator<JsonNode> nodeIterator = e.elements();

		while (nodeIterator.hasNext()) {

			JsonNode entry = nodeIterator.next();

			JsonNode targetNode = entry.path("target");
			JsonNode sourceNode = entry.path("source");
				
			workEntry = iMap.fieldMappings.get(targetNode.asText());
			if (workEntry == null) {
				workEntry = new InputMapEntry();
				workEntry.setTargetField(targetNode.asText());
			}
			SourceAttributes _sa = workEntry.addSourceField(sourceNode.asText());
			doOtherAttributes(entry, _sa);
				
			iMap.fieldMappings.put(targetNode.asText(), workEntry);
			   
		}
			
		
	}
	

	private void doMethodsMap(Entry<String, JsonNode> nodeEntry) {
		
		JsonNode e = nodeEntry.getValue();
		

		Iterator<JsonNode> nodeIterator = e.elements();

		while (nodeIterator.hasNext()) {

			JsonNode entry = nodeIterator.next();
				
			
			JsonNode targetNode = entry.path("target");
			JsonNode sourceNode = entry.path("source");
				
			workEntry = iMap.fieldMappings.get(targetNode.asText());
			if (workEntry == null) {
				workEntry = new InputMapEntry();
			}
			SourceAttributes _sa = workEntry.addSourceField(sourceNode.asText());
			_sa.setMapMethod( getMappingMethod(targetNode.asText()) );
			doOtherAttributes(entry, _sa);
				
			iMap.fieldMappings.put(targetNode.asText(), workEntry);
			   
		}

		
	}
	
	/**
	 * add exclusions to the input map object
	 * @param nodeEntry
	 */
	private void doExclusions(Entry<String, JsonNode> nodeEntry) {
		
		JsonNode e = nodeEntry.getValue();
		

		Iterator<JsonNode> nodeIterator = e.elements();

		while (nodeIterator.hasNext()) {

			JsonNode entry = nodeIterator.next();
							
			JsonNode conditionNode = entry.path("condition");
			
			iMap.addExclusion(new Condition(conditionNode.asText()));
			   
		}
		
	}
	
	
	InputMapEntry workEntry;
	
	
	private Method getMappingMethod (String sMethodName) {
		
		Method m = null;
		
		if (sMethodName.contains(".")) {
			log.error("Hey I didn't program that trick yet!");
		}
		
		else {
		
			try {     
				switch (iMap.objectName) {
				case "ClaimServLine":
					workObject = new ClaimServLine();
					break;
				case "ClaimRx":
					workObject = new ClaimRx();
					break;
				case "Enrollment":
					workObject = new Enrollment();
					break;
				case "PlanMember":
					workObject = new PlanMember();
					break;
				case "Provider":
					workObject = new Provider();
					break;
				default:
					throw new IllegalStateException("Can not map to object " + iMap.objectName);
				}
				m = workObject.getClass().getMethod(sMethodName, new Class[]{String.class});
				//System.out.println("method = " + m.toString());        
			}
	    
			catch(NoSuchMethodException e) {
				System.out.println(e.toString());
			}
		}
		
		return m;

		
	}
	
	
	private void doOtherAttributes (JsonNode entry, SourceAttributes _sa) {
		
		oaNode = entry.path("default");
		if (! oaNode.isMissingNode())
			workEntry.set_default(oaNode.asText());
		
		oaNode = entry.path("fill");
		if (! oaNode.isMissingNode())
			workEntry.set_fill(oaNode.asText());
		
		oaNode = entry.path("length");
		if (! oaNode.isMissingNode())
			_sa.set_length(oaNode.asInt());
		
		oaNode = entry.path("condition");
		if (! oaNode.isMissingNode()) {
			if (oaNode instanceof ArrayNode)
			{
				for (JsonNode _cN : oaNode) {
					_sa.add_condition(_cN.asText());
			    }
			}
			else
				_sa.add_condition(oaNode.asText());
		}
		
		oaNode = entry.path("concatenate");
		if (! oaNode.isMissingNode())
			_sa.setConcatenate(true);
		
	}
	
	JsonNode oaNode;
	
	


	private void addDirectives(Entry<String, JsonNode> nodeEntry) {
		
		JsonNode e = nodeEntry.getValue();
		Iterator<JsonNode> nodeIterator = e.elements();

		while (nodeIterator.hasNext()) {
			
			Iterator<Map.Entry<String, JsonNode>> elt = nodeIterator.next().fields();
			while (elt.hasNext()) {

				Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) elt.next();

				if(entry.getValue().asText().equals("true"))
					directives.add(entry.getKey());

			}
		}
		
	}
	
	public List<String> getDirectivesFromMap () {
		return directives;
	}
	


	private final static String ojbNameCheckList = ClaimServLine.class.getSimpleName() + ClaimRx.class.getSimpleName() + 
			PlanMember.class.getSimpleName() + Enrollment.class.getSimpleName() + construction.model.Provider.class.getSimpleName();
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(InputMap.class);

}
