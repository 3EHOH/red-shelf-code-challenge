





public class ResponseConsumer {

	MappingRequest _mr;
	


	public MappingRequest parseResponse (String jsonInString) {
		
		ObjectMapper mapper = new ObjectMapper();

		try {

			// Convert JSON string to Object
			_mr = mapper.readValue(jsonInString, MappingRequest.class);
			
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (JsonParseException e) {
			log.error("Parse exception on MappingRequest: " + jsonInString);
			e.printStackTrace();	
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return _mr;

	}
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ResponseConsumer.class);
	
	
}
