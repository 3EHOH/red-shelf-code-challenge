




/**
 * Parser for XML metadata exported from Episode Builder
 * 
 * C:\Users\Warren\Dropbox (HCI3 Team)\HCI3 Shared Folder\Prometheus Operations\Builder\Builder Export Function\builderExportScheme_2013-11-01.xsd
 * 
 * @author Warren
 *
 */

public class EpisodeMetaDataFromXML extends EpisodeMetaDataAbstract implements EpisodeMetaDataInterface {
	
	
	
	@Override
	public List<EpisodeMetaData> getAllEpisodes() {
		return epList;
	}
	

	
	public List<String> getAllEMCodes() {
		return emCodeList;
	}

	public MetaDataHeader getMetaDataHeader() {
		return mdh;
	}
	
	/*
	 * Constructor
	 * 
	 * actually reads the metadata and parses it
	 */
	public EpisodeMetaDataFromXML (String resource) throws ParserConfigurationException, SAXException, IOException
	{
		
		if (resource == null)
			throw new IllegalStateException("No resource name was provided.  Can not parse");
		
		log.info("Starting XML parse of " + resource);
		
		// Obtain the flat file
		File fXmlFile = new File(resource.trim());

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
				//log.info(i + "|" + aNode);
				
				switch (aNode) { 
				case "episode": 
					buildEpisodeFromNode(node); 
					break;
				case "em_code":
					getAllEMCodes(node);
					break;
				case "export_date":
					mdh.setExport_date(node.getTextContent());
					break;
				case "export_version":
					mdh.setExport_version(node.getTextContent());
					break;
				case "release_notes":
					mdh.setRelease_notes(node.getTextContent());
					break;
				default:
					break;
				} 
			}
		} 

		bDebug = false;
		outputCheck();
		
		
		log.info("Completed XML parse of " + resource);
		
	} 
	
	

	
	/**
	 * build an episode definition from the episode tag group
	 * @param ep
	 */
	private void buildEpisodeFromNode (Node ep) {
		
		NodeList nodeList = ep.getChildNodes();
		Node node;
		
		RxCodeMetaData _rxmd;
		
		EpisodeMetaData emd = new EpisodeMetaData();
		
		for (int i = 0; i < nodeList.getLength(); i++) { 
			
			node = nodeList.item(i); 
			String aNode;
			
			if (node instanceof Element) { 
				
				aNode = node.getNodeName();
				//log.info("Episode Node: " + aNode);
				switch (aNode) { 
				case "episode_id": 
					emd.setEpisode_id(node.getTextContent()); 
					break; 
				case "episode_acronym": 
					emd.setEpisode_acronym(node.getTextContent()); 
					break; 
				case "episode_name": 
					emd.setEpisode_name(node.getTextContent()); 
					break; 
				case "episode_type": 
					emd.setEpisode_type(node.getTextContent()); 
					break; 
				case "episode_modified_date": 
					try {
						emd.setEpisode_modified_date(sdf.parse(node.getTextContent()));
					} catch (DOMException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} 
					break; 
				case "episode_version": 
					emd.setEpisode_version(node.getTextContent()); 
					break; 
				case "post_claim_assignment": 
					emd.setPost_claim_assignment(Integer.parseInt(node.getTextContent())); 
					break; 
				case "look_back": 
					emd.setLook_back(Integer.parseInt(node.getTextContent())); 
					break; 
				case "look_ahead": 
					/*try {
						emd.setLook_ahead(Integer.parseInt(node.getTextContent()));
					}
					catch (java.lang.NumberFormatException e)			//  handle cases where Look Ahead is set to "End of Study Period"
					{
						emd.setLook_ahead(-1);
					}*/
					emd.setLook_ahead(node.getTextContent());
					break; 
				case "condition_min": 
					emd.setCondition_min(Integer.parseInt(node.getTextContent())); 
					break; 	
				case "trigger_condition":
					TriggerConditionMetaData tmpTc = doTriggerCondition(node);
					emd.getTrigger_conditions().add(tmpTc);
					
					if ( tmpTc.getFacility_type() == null  || tmpTc.getFacility_type().isEmpty()) {
						break;
					}
					
					// if it is an episode trigger condition
					if (tmpTc.getFacility_type().equals("EP")) {
						
						emd.getTrigger_conditions_ep_trig().put(tmpTc.getEpisode_id(), tmpTc);
					} else {
						//it is NOT an episode trigger condition
						emd.getTrigger_conditions_by_fac().put(tmpTc.getFacility_type(), tmpTc);
					}
					
					break;
				case "trigger_code":
					
					TriggerCodeMetaData tmpTcmd = doTriggerCode(node);
					
					
					//if (tmpTcmd.getType_id().equals("CPT") ) { tmpTcmd.setType_id("PX"); }
					//if (tmpTcmd.getType_id().equals("HCPC") ) { tmpTcmd.setType_id("PX"); }
					
					if (tmpTcmd.getType_id().equals("PX")) {
						//log.info("TYPE: " + tmpTcmd.getType_id() + " " + tmpTcmd.getCode_id());
					}
					
					emd.getTrigger_codes().add(tmpTcmd);
					
					HashMap<String, HashMap<String, ArrayList<TriggerCodeMetaData>>> tcbe = new HashMap<String, HashMap<String, ArrayList<TriggerCodeMetaData>>>();
					HashMap<String, ArrayList<TriggerCodeMetaData>> hm = new HashMap<String, ArrayList<TriggerCodeMetaData>>();
					ArrayList<TriggerCodeMetaData> hs = new ArrayList<TriggerCodeMetaData>();
					
					hs.add(tmpTcmd);
					hm.put(tmpTcmd.getCode_id(), hs);
					tcbe.put(tmpTcmd.getType_id(), hm);
					
					//log.info(tmpTcmd.getType_id());
					
					
					if (tmpTcmd.getCode_id()==null || tmpTcmd.getCode_id().isEmpty() || tmpTcmd.getType_id()==null || tmpTcmd.getType_id().isEmpty()) {
						break;
					}
					
					//log.info("Is this null? " + emd.getTrigger_code_by_ep());
					
					if (emd.getTrigger_code_by_ep().isEmpty()) {
						emd.setTrigger_code_by_ep(tcbe);
					} else {
						
						if (!emd.getTrigger_code_by_ep().containsKey(tmpTcmd.getType_id())) {
							emd.getTrigger_code_by_ep().put(tmpTcmd.getType_id(), hm);
						}
						if (!emd.getTrigger_code_by_ep().get(tmpTcmd.getType_id()).containsKey(tmpTcmd.getCode_id())) {
							emd.getTrigger_code_by_ep().get(tmpTcmd.getType_id()).put(tmpTcmd.getCode_id(), hs);
						}

					}
					
					break;
				case "dx_code":
					emd.getDx_codes().add(doDxCode(node));
					break;
				case "px_code":
					emd.getPx_codes().add(doPxCode(node));
					break;
				case "complication_code":
					emd.getComplication_codes().add(doComplicationCode(node));
					break;
				case "association":
					emd.getAssociations().add(doAssociation(node));
					break;
				case "rx_code":
					_rxmd = doRxCode(node);
					emd.getRx_codes().put(_rxmd.getCode_id(), _rxmd);
					break;
				case "sub_type_code":
					emd.getSub_type_codes().add(doSubTypeCode(node));
					break;
				default:
					break;
				} 
			}
		}
		
		epList.add(emd);
		
	}
	
	private void getAllEMCodes (Node ep) {
		
		NodeList nodeList = ep.getChildNodes();
		Node node;
		
		for (int i = 0; i < nodeList.getLength(); i++) { 
			
			node = nodeList.item(i); 
			String aNode;
			
			if (node instanceof Element) { 
				
				aNode = node.getNodeName();
				
				if (aNode.equals("code_id")) {
					emCodeList.add(node.getTextContent());
				}
			}
		}
	}
 
	private SubTypeCodeMetaData doSubTypeCode(Node sbNode) {

		NodeList nodeList = sbNode.getChildNodes();
		Node node;
		
		SubTypeCodeMetaData sbmd = new SubTypeCodeMetaData();
		
		for (int i = 0; i < nodeList.getLength(); i++) { 
			
			node = nodeList.item(i); 
			String aNode;
			
			if (node instanceof Element) { 
				
				aNode = node.getNodeName();
				//log.info("sub type Code Node: " + aNode);
				switch (aNode) { 
				case "sub_type_group_id": 
					sbmd.setSub_type_group_id(node.getTextContent()); 
					break;
				case "sub_type_group_name": 
					sbmd.setSub_type_group_name(node.getTextContent()); 
					break;
				default:
					tryCodeBase(sbmd, node);
					break;
				} 
			}
		}
		return sbmd;
	}

	private RxCodeMetaData doRxCode(Node rxNode) {

		NodeList nodeList = rxNode.getChildNodes();
		Node node;
		
		RxCodeMetaData rxmd = new RxCodeMetaData();
		
		for (int i = 0; i < nodeList.getLength(); i++) { 
			
			node = nodeList.item(i); 
			String aNode;
			
			if (node instanceof Element) { 
				
				aNode = node.getNodeName();
				//log.info("Rx Code Node: " + aNode);
				switch (aNode) { 
				case "rx_assignment": 
					rxmd.setRx_assignment(node.getTextContent()); 
					break;
				default:
					tryCodeBase(rxmd, node);
					break;
				} 
			}
		}
		return rxmd;
	}

	private AssociationMetaData doAssociation(Node asNode) {

		NodeList nodeList = asNode.getChildNodes();
		Node node;
		
		AssociationMetaData asmd = new AssociationMetaData();
		
		for (int i = 0; i < nodeList.getLength(); i++) { 
			
			node = nodeList.item(i); 
			String aNode;
			
			if (node instanceof Element) { 
				
				aNode = node.getNodeName();
				//log.info("Association Node: " + aNode);
				switch (aNode) { 
				case "episode_id": 
					asmd.setEpisode_id(node.getTextContent()); 
					break;
				case "episode_acronym": 
					asmd.setEpisode_acronym(node.getTextContent()); 
					break;
				case "subsidiary_to_procedural": 
					asmd.setSubsidiary_to_procedural(node.getTextContent()); 
					break;
				case "ass_type": 
					asmd.setAss_type(node.getTextContent()); 
					break;
				case "ass_level": 
					asmd.setAss_level(Integer.parseInt(node.getTextContent())); 
					break;
				case "ass_start_day": 
					asmd.setAss_start_day(node.getTextContent()); 
					break;
				case "ass_end_day": 
					asmd.setAss_end_day(node.getTextContent()); 
					break;
				default:
					break;
				} 
			}
		}
		return asmd;
	}

	private ComplicationCodeMetaData doComplicationCode(Node cmNode) {

		NodeList nodeList = cmNode.getChildNodes();
		Node node;
		
		ComplicationCodeMetaData cmmd = new ComplicationCodeMetaData();
		
		for (int i = 0; i < nodeList.getLength(); i++) { 
			
			node = nodeList.item(i); 
			String aNode;
			
			if (node instanceof Element) { 
				
				aNode = node.getNodeName();
				//log.info("Cm Code Node: " + aNode);
				switch (aNode) { 
				case "complication_type":
					/* Added by Mike 2016-01-07 - Might not want to default to 1 */
					if (!node.getTextContent().isEmpty()) {
						cmmd.setComplication_type(Integer.parseInt(node.getTextContent()));
					} else {
						cmmd.setComplication_type(1);
					}
					break;
				default:
					tryCodeBase(cmmd, node);
					break;
				} 
			}
		}
		return cmmd;
	}

	private PxCodeMetaData doPxCode(Node pxNode) {
		
		NodeList nodeList = pxNode.getChildNodes();
		Node node;
		
		PxCodeMetaData pxmd = new PxCodeMetaData();
		
		for (int i = 0; i < nodeList.getLength(); i++) { 
			
			node = nodeList.item(i); 
			String aNode;
			
			if (node instanceof Element) { 
				
				aNode = node.getNodeName();
				//log.info("Px Code Node: " + aNode);
				switch (aNode) { 
				case "core": 
					pxmd.setCore(node.getTextContent().equals("1") ? true : false);  
					break;
				case "sufficient": 
					pxmd.setSufficient(node.getTextContent().equals("1") ? true : false);  
					break;
				case "pas": 
					pxmd.setPas(node.getTextContent().equals("1") ? true : false);  
					break;
				case "betos_category": 
					pxmd.setBetos_category(node.getTextContent()); 
					break;
				default:
					tryCodeBase(pxmd, node);
					break;
				} 
			}
		}
		return pxmd;
	}

	private DxCodeMetaData doDxCode(Node dxNode) {
		
		NodeList nodeList = dxNode.getChildNodes();
		Node node;
		
		DxCodeMetaData dxmd = new DxCodeMetaData();
		
		for (int i = 0; i < nodeList.getLength(); i++) { 
			
			node = nodeList.item(i); 
			String aNode;
			
			if (node instanceof Element) { 
				
				aNode = node.getNodeName();
				//log.info("Dx Code Node: " + aNode);
				switch (aNode) { 
				default:
					tryCodeBase(dxmd, node);
					break;
				} 
			}
		}
		return dxmd;
	}

	private TriggerCodeMetaData doTriggerCode(Node tcNode) {
		
		NodeList nodeList = tcNode.getChildNodes();
		Node node;
		
		TriggerCodeMetaData tcmd = new TriggerCodeMetaData();
		
		for (int i = 0; i < nodeList.getLength(); i++) { 
			
			node = nodeList.item(i); 
			String aNode;
			
			if (node instanceof Element) { 
				
				aNode = node.getNodeName();
				//log.info("Trigger Code Node: " + aNode);
				switch (aNode) { 
				case "qualifying_diagnosis": 
					tcmd.setQualifying_diagnosis(node.getTextContent()); 
					break; 
				default:
					tryCodeBase(tcmd, node);
					break;
				} 
			}
		}
		return tcmd;
	}

	/**
	 * a method to handle tags common to all episode code groups
	 * @param cbmd
	 * @param node
	 */
	private void tryCodeBase(EpisodeCodeBase cbmd, Node node) {
		String aNode = node.getNodeName();
		switch (aNode) { 
		case "code_id": 
			cbmd.setCode_id(node.getTextContent()); 
			break; 
		case "type_id": 
			cbmd.setType_id(node.getTextContent()); 
			break; 
		case "specific_type_id": 
			cbmd.setSpecific_type_id(node.getTextContent()); 
			break; 
		//case "code_name": 
		//	cbmd.setCode_name(node.getTextContent()); 
		//	break; 
		case "group_id": 
			cbmd.setGroup_id(node.getTextContent()); 
			break; 
		//case "group_name": 
		//	cbmd.setGroup_name(node.getTextContent()); 
		//	break; 
		default:
			break;
		} 		
	}

	/**
	 * build an episode's trigger conditions from the trigger condition tag group
	 * @param tcNode
	 * @return
	 */
	private TriggerConditionMetaData doTriggerCondition(Node tcNode) {
		
		NodeList nodeList = tcNode.getChildNodes();
		Node node;
		
		TriggerConditionMetaData tcmd = new TriggerConditionMetaData();
		
		for (int i = 0; i < nodeList.getLength(); i++) { 
			
			node = nodeList.item(i); 
			String aNode;
			
			if (node instanceof Element) { 
				
				aNode = node.getNodeName();
				//log.info("Trigger Condition Node: " + aNode);
				switch (aNode) { 
				case "facility_type": 
					tcmd.setFacility_type(node.getTextContent()); 
					break; 
				case "px_code_position": 
					tcmd.setPx_code_position(node.getTextContent()); 
					break; 
				case "em_code_position": 
					tcmd.setEm_code_position(node.getTextContent()); 
					break; 
				case "and_or": 
					tcmd.setAnd_or(node.getTextContent()); 
					break; 
				case "dx_code_position": 
					tcmd.setDx_code_position(node.getTextContent()); 
					break; 
				case "episode_id": 
					tcmd.setEpisode_id(node.getTextContent()); 
					break; 
				case "requires_confirming_code": 
					tcmd.setReq_conf_code(node.getTextContent().equals("1") ? true : false); 
					break;  
				case "min_code_separation": 
					if (node.getTextContent()==null) {
						tcmd.setMin_code_separation("0");
					} else {
						tcmd.setMin_code_separation(node.getTextContent()); 
					}
					break; 
				case "max_code_separation": 
					if (node.getTextContent()==null) {
						tcmd.setMin_code_separation("0");
					} else {
						tcmd.setMax_code_separation(node.getTextContent());
					}
					break;  
				default:
					break;
				} 
			}
		}
		return tcmd;
	}


	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {
		
		bDebug = true;
		loadParameters(args);
		try {
			new EpisodeMetaDataFromXML(parameters.get("IP_FILE"));
		} 
		catch (ParserConfigurationException | SAXException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void loadParameters (String[] args) {
		// load any default parameters from the default parameter array
		for (int i = 0; i < parameterDefaults.length; i++) {
			parameters.put(parameterDefaults[i][0], parameterDefaults[i][1]);
		}
		// overlay or add any incoming parameters
		for (int i = 0; i < args.length; i++) {
			parameters.put(args[i].substring(0, args[i].indexOf('=')), args[i].substring(args[i].indexOf('=')+1)) ;
		}
		
	}
	
	static HashMap<String, String> parameters = RunParameters.parameters;
	static String [][] parameterDefaults = {
			{"IP_FILE", "C:\\ECRAnalyticsTestFiles\\HCI3-ECR-Definition-Tables-2014-08-18-5.2.006_FULL.xml"},
			{"configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\"}
	};
	
	//public static final String IP_FILE = 
	//		"C:\\Users\\Warren\\Dropbox (HCI3 Team)\\HCI3 Shared Folder\\Prometheus Operations\\Builder\\Builder Export Function\\DBExport-2014-05-02-5.2.005-trNodxFix\\"
	//		+ "HCI3-ECR-Definition-Tables-2014-05-02-5.2.005.xml";
	
	public static final String IP_FILE = 
			"C:\\ECRAnalyticsTestFiles\\HCI3-ECR-Definition-Tables-2014-08-18-5.2.006_FULL.xml";
	

	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	private static org.apache.log4j.Logger log = Logger.getLogger(EpisodeMetaDataFromXML.class);
	
	
}
