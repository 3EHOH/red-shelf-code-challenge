




public class ErrorManager {
	
	
	AllDataInterface di;
	HashMap<String, ErrorMessage> errorCatalog = new HashMap<String, ErrorMessage>();  
	
	/**
	 * Constructor - set data interface and parses error configuration file
	 * @param di
	 */
	public ErrorManager (AllDataInterface di) {
		try {
			parseConfig(IP_FILE);
		} catch (ParserConfigurationException | SAXException | IOException e) {
			log.error("Failure trying to parse error configuration file");
			e.printStackTrace();
		}
		this.di = di;
	}
	
	/** 
	 * Read the error definition configuration file and parse it
	 */
	private void parseConfig (String resource) throws ParserConfigurationException, SAXException, IOException
	{
		// Obtain the flat file
		File fXmlFile = new File(resource);

		// Get the DOM Builder Factory 
		DocumentBuilderFactory factory =  DocumentBuilderFactory.newInstance(); 

		// Get the DOM Builder 
		DocumentBuilder builder = factory.newDocumentBuilder(); 

		// Load and Parse the XML document 
		// document contains the complete XML as a Tree. 
		Document document =  builder.parse(fXmlFile); 
		
		// Iterating through the nodes and extracting the data. 
		NodeList nodeList = document.getDocumentElement().getChildNodes(); 
		for (int i = 0; i < nodeList.getLength(); i++) { 

			Node node = nodeList.item(i); 
			String aNode;
			//log.info(i + "|" + node.getNodeName());
			
			if (node instanceof Element) { 
				
				aNode = node.getNodeName();
				log.info(i + "|" + node.getAttributes().getNamedItem("id").getNodeValue());
				
				switch (aNode) { 
				case "message": 
					buildMessageFromNode(node); 
					break;
				default:
					break;
				} 
			}
		} 
	} 
	
	private void buildMessageFromNode (Node sbNode) {
		
		ErrorMessage msg = new ErrorMessage(sbNode.getAttributes().getNamedItem("id").getNodeValue());
		
		NodeList nodeList = sbNode.getChildNodes();
		Node node;
			
		for (int i = 0; i < nodeList.getLength(); i++) { 
			
			node = nodeList.item(i); 
			String aNode;
			
			if (node instanceof Element) { 
				
				aNode = node.getNodeName();

				switch (aNode) { 
				case "level": 
					msg.setLevelFromString(node.getTextContent()); 
					break;
				case "limit": 
					msg.setLimit(Integer.parseInt(node.getTextContent())); 
					break;
				case "text": 
					msg.setText(node.getTextContent()); 
					break; 
				default:
					break;
				} 
			}
		}
		
		errorCatalog.put(msg.getId(), msg);
	
	}
	
	/**
	 * prepare an error message and put it to the proper outputs
	 */
	public void issueError(String id, String errValue, Object o) {
		
		ErrorMessage msg = errorCatalog.get(id);
		StringBuffer sb = new StringBuffer();
		
		if (msg == null) {
			log.debug("Found illegal error message: " + id);
		}
		else if (msg.getCount() > msg.getLimit()) {
			setMsgAccumulator(msg, o);
		}
		else {
			
			sb.append(id).append(" - ");
			
			if (o instanceof ClaimServLine) {
				ClaimServLine csl = (ClaimServLine) o;
				sb.append(msg.getStringFromLevel()).append(": ");
				sb.append("Member: ").append(csl.getMember_id()).append(" : ");
				sb.append("Claim: ").append(csl.getClaim_id()).append(" : ");
				if(csl.getClaim_line_id() != null)
					sb.append("Line: ").append(csl.getClaim_line_id()).append(" : ");
				sb.append(msg.getText());
				if(errValue != null)
					sb.append(" : found ").append(errValue);
				di.getAllServiceClaimErrors().add(sb.toString());
			}
			
			else if (o instanceof ClaimRx) {
				ClaimRx crx = (ClaimRx) o;
				sb.append(msg.getStringFromLevel()).append(": ");
				sb.append("Member: ").append(crx.getMember_id()).append(" : ");
				if(crx.getRx_fill_date() != null)
					sb.append("Fill Date: ").append(crx.getRx_fill_date()).append(" : ");
				sb.append(msg.getText());
				if(errValue != null)
					sb.append(" : found ").append(errValue);
				di.getAllRxClaimErrors().add(sb.toString());
			}
			
			else if (o instanceof Enrollment) {
				Enrollment enr = (Enrollment) o;
				sb.append(msg.getStringFromLevel()).append(": ");
				sb.append("Member: ").append(enr.getMember_id()).append(" : ");
				sb.append(msg.getText());
				if(errValue != null)
					sb.append(" : found ").append(errValue);
				di.getAllEnrollmentErrors().add(sb.toString());
			}
			
			else if (o instanceof PlanMember) {
				PlanMember plm = (PlanMember) o;
				sb.append(msg.getStringFromLevel()).append(": ");
				sb.append("Member: ").append(plm.getMember_id()).append(" : ");
				sb.append(msg.getText());
				if(errValue != null)
					sb.append(" : found ").append(errValue);
				di.getAllPlanMemberErrors().add(sb.toString());
			}
			
			else if (o instanceof Provider) {
				Provider prv = (Provider) o;
				sb.append(msg.getStringFromLevel()).append(": ");
				sb.append("Provider: ").append(prv.getProvider_id()).append(" : ");
				sb.append(msg.getText());
				if(errValue != null)
					sb.append(" : found ").append(errValue);
				di.getAllProviderErrors().add(sb.toString());
			}
			
			msg.setCount(msg.getCount() + 1);
		
		}
		
	}
	
	/**
	 * manage the "nnn" more like this string
	 * @param msg
	 * @param o
	 */
	private void setMsgAccumulator (ErrorMessage msg, Object o)
	{
		StringBuffer sb = new StringBuffer();
		
		if (o instanceof ClaimServLine) {
			if(msg.getAccumulatorRow() == 0) {
				msg.setAccumulatorRow(di.getAllServiceClaimErrors().size());
				di.getAllServiceClaimErrors().add("placeholder");
			}
			msg.setCount(msg.getCount() + 1);
			sb.append( msg.getId()).append(" - ");
			sb.append(msg.getStringFromLevel()).append(": ");
			sb.append(" There are ").append(msg.getCount()-msg.getLimit()).append(" more messages like this");
			di.getAllServiceClaimErrors().set(msg.getAccumulatorRow(), sb.toString());
		}
		
		else if (o instanceof ClaimRx) {
			if(msg.getAccumulatorRow() == 0) {
				msg.setAccumulatorRow(di.getAllRxClaimErrors().size());
				di.getAllRxClaimErrors().add("placeholder");
			}
			msg.setCount(msg.getCount() + 1);
			sb.append( msg.getId()).append(" - ");
			sb.append(msg.getStringFromLevel()).append(": ");
			sb.append(" There are ").append(msg.getCount()-msg.getLimit()).append(" more messages like this");
			di.getAllRxClaimErrors().set(msg.getAccumulatorRow(), sb.toString());
		}
		
		else if (o instanceof Enrollment) {
			if(msg.getAccumulatorRow() == 0) {
				msg.setAccumulatorRow(di.getAllEnrollmentErrors().size());
				di.getAllEnrollmentErrors().add("placeholder");
			}
			msg.setCount(msg.getCount() + 1);
			sb.append( msg.getId()).append(" - ");
			sb.append(msg.getStringFromLevel()).append(": ");
			sb.append(" There are ").append(msg.getCount()-msg.getLimit()).append(" more messages like this");
			di.getAllEnrollmentErrors().set(msg.getAccumulatorRow(), sb.toString());
		}
		
		else if (o instanceof PlanMember) {
			if(msg.getAccumulatorRow() == 0) {
				msg.setAccumulatorRow(di.getAllPlanMemberErrors().size());
				di.getAllPlanMemberErrors().add("placeholder");
			}
			msg.setCount(msg.getCount() + 1);
			sb.append( msg.getId()).append(" - ");
			sb.append(msg.getStringFromLevel()).append(": ");
			sb.append(" There are ").append(msg.getCount()-msg.getLimit()).append(" more messages like this");
			di.getAllPlanMemberErrors().set(msg.getAccumulatorRow(), sb.toString());
		}
		
		else if (o instanceof Provider) {
			if(msg.getAccumulatorRow() == 0) {
				msg.setAccumulatorRow(di.getAllProviderErrors().size());
				di.getAllProviderErrors().add("placeholder");
			}
			msg.setCount(msg.getCount() + 1);
			sb.append( msg.getId()).append(" - ");
			sb.append(msg.getStringFromLevel()).append(": ");
			sb.append(" There are ").append(msg.getCount()-msg.getLimit()).append(" more messages like this");
			di.getAllProviderErrors().set(msg.getAccumulatorRow(), sb.toString());
		}
		
	}
	
	
	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {
		new ErrorManager(null);
	}
	
	public static final String IP_FILE = "ErrorConfig.xml";
	
	
	

	private static org.apache.log4j.Logger log = Logger.getLogger(ErrorManager.class);
	
}
