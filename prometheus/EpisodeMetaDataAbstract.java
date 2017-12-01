



public abstract class EpisodeMetaDataAbstract {
	
	
	
	protected List<EpisodeMetaData> epList = new ArrayList<EpisodeMetaData>();
	
	protected List<String> emCodeList = new ArrayList<String>();
	
	protected MetaDataHeader mdh = new MetaDataHeader();
	
	
	protected static boolean bDebug = false;
	
	
	
	protected void outputCheck () {
		
		// Printing the episode list populated. 
		if (bDebug)  {
			
			// the gorey details
			
			for (EpisodeMetaData emd : epList) { 
				log.info(emd);
				
				log.info("Trigger conditions for episode: " + emd.getEpisode_id());
				for (TriggerConditionMetaData tcmd : emd.getTrigger_conditions()) {
					log.info(tcmd);
				}
				/*
				log.info("Trigger codes for episode: " + emd.getEpisode_id());
				for (TriggerCodeMetaData tcmd : emd.getTrigger_codes()) {
					log.info(tcmd);
				}
				log.info("Dx codes for episode: " + emd.getEpisode_id());
				for (DxCodeMetaData dxmd : emd.getDx_codes()) {
					log.info(dxmd);
				}
				
				log.info("Px codes for episode: " + emd.getEpisode_id());
				int i = 0;
				for (PxCodeMetaData pxmd : emd.getPx_codes()) {
					log.info(pxmd);
					i++;
					if (i>10)
						break;
				}
				
				log.info("Complication codes for episode: " + emd.getEpisode_id());
				for (ComplicationCodeMetaData cmmd : emd.getComplication_codes()) {
					log.info(cmmd);
				}
				log.info("Associations for episode: " + emd.getEpisode_id());
				for (AssociationMetaData asmd : emd.getAssociations()) {
					log.info(asmd);
				}
				log.info("Rx codes for episode: " + emd.getEpisode_id());
				java.util.Iterator<Entry<String, RxCodeMetaData>> it = emd.getRx_codes().entrySet().iterator();
				RxCodeMetaData rxmd;
				while (it.hasNext()) {
					rxmd = it.next().getValue();
					log.info(rxmd);
				}
				log.info("Sub-types for episode: " + emd.getEpisode_id());
				for (SubTypeCodeMetaData sbmd : emd.getSub_type_codes()) {
					log.info(sbmd);
				}
				*/
				
			}
			
			
			// header stuff
			log.info("Metadata information: " + mdh.getExport_date() + " version " + mdh.getExport_version());
			log.info("Release notes: " + mdh.getRelease_notes());
			
			// counts for episodes
			for (EpisodeMetaData emd : epList) { 
				log.info(emd);
				log.info("Object counts for episode: " + emd.getEpisode_id());
				log.info("--- Trigger conditions: " + emd.getTrigger_conditions().size());
				log.info("--- Trigger codes for episode: " + emd.getTrigger_codes().size());
				
				HashMap<String, HashMap<String, ArrayList<TriggerCodeMetaData>>> _h = emd.getTrigger_code_by_ep();
				for(Entry<String, HashMap<String, ArrayList<TriggerCodeMetaData>>> entry : _h.entrySet()){
		            //System.out.printf("Key : %s and Value: %s %n", entry.getKey(),
		            //                                               entry.getValue());
		            log.info("----- Trigger codes for nomenclature: " + entry.getKey() );
		            
		            for (Entry<String, ArrayList<TriggerCodeMetaData>> _e: entry.getValue().entrySet()) {
		            	log.info("------- Trigger codes for code value: " + _e.getKey()); // + " Size: " + _e.getValue().iterator());
		            	logTCArray(_e.getValue());
		            }
		        }

				log.info("--- Dx codes for episode: " + emd.getDx_codes().size());
				log.info("--- Px codes for episode: " + emd.getPx_codes().size());
				log.info("--- Complication codes for episode: " +emd.getComplication_codes().size());
				log.info("--- Associations for episode: " + emd.getAssociations().size());
				log.info("--- Rx codes for episode: " + emd.getRx_codes().size());
				log.info("--- Sub-types for episode: " + emd.getSub_type_codes().size());
			}
			
			// counts for E&M
			log.info("E&M Code List count: " + emCodeList.size());
			
			for (String s : emCodeList) {
				log.info("--E&M Code: " + s);
			}
			
		}
		
	}
	
	private void logTCArray (ArrayList<TriggerCodeMetaData> _t) {
		for(TriggerCodeMetaData _c : _t) {
			log.info(" --------- " +_c);
		}
	}
	
	private static org.apache.log4j.Logger log = Logger.getLogger(EpisodeMetaDataAbstract.class);
	

}
