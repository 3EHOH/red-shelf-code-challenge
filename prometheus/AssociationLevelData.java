



public class AssociationLevelData {
		
	public void writeAssociationLevelLines(ArrayList<EpisodeShell> allEpisodes) throws IOException {
		
		String[] headerRow = new String[16];
		
		headerRow[0] = "member_id";
		headerRow[1] = "episode_id";
		headerRow[2] = "claim_id";
		headerRow[3] = "claim_line_id";
		headerRow[4] = "ass_episode_id";
		headerRow[5] = "ass_claim_id";
		headerRow[6] = "ass_claim_line_id";
		headerRow[7] = "ass_type";
		headerRow[8] = "ass_level";
		headerRow[9] = "ass_start_day";
		headerRow[10] = "ass_end_day";
		headerRow[11] = "ass_count";
		headerRow[12] = "master_claim_id";
		headerRow[13] = "master_episode_id";
		headerRow[14] = "ass_master_claim_id";
		headerRow[15] = "ass_master_episode_id";
		
		
		List<String[]> allRows = new ArrayList<String[]>();
		
		String sFileName = FileNamer.getFileName("associationLevelData") + ".csv";
		File f = new File(sFileName);
		if(!f.exists()) {
			allRows.add(headerRow);
		}
		
		String[] row = new String[12];
		
		for (EpisodeShell es : allEpisodes) {
			
			for (int i=2; i<=5; i++) {
				String si = "" + i;
				//log.info(es.getEpisode_id() + " | " + si);
				//log.info(es.getAssociated_episodes() + " | " + es.getAssociated_episodes().get(si));
				if (es.getAssociated_episodes()!=null && es.getAssociated_episodes().get(si)!= null && 
						!es.getAssociated_episodes().get(si).isEmpty()) {
					//log.info(":Inside: " + es.getEpisode_id() + " | " + si);
					for (AssociatedEpisode aes : es.getAssociated_episodes().get(si)) {
						//log.info(":Inside2: " + es.getEpisode_id() + " | " + i + " | " + aes.getAssEpisode().getEpisode_id());
						row = new String[16];
						
						row[0] = es.getMember_id();
						row[1] = es.getEpisode_id();
						row[2] = es.getClaim_id();
						row[3] = es.getClaim().getClaim_line_id();
						row[4] = aes.getAssEpisode().getEpisode_id();
						row[5] = aes.getAssEpisode().getClaim().getClaim_id();
						row[6] = aes.getAssEpisode().getClaim().getClaim_line_id();
						row[7] = aes.getAssMetaData().getAss_type();
						row[8] = "" + aes.getAssMetaData().getAss_level();
						row[9] = aes.getAssMetaData().getAss_start_day();
						row[10] = aes.getAssMetaData().getAss_end_day();
						row[11] = "" + aes.getAssEpisode().getAssociationCount();
						row[12] = es.getClaim().getMaster_claim_id();
						row[13] = es.getMaster_episode_id();
						row[14] = aes.getAssEpisode().getClaim().getMaster_claim_id();
						row[15] = aes.getAssEpisode().getMaster_episode_id();
						
						//aes.getAssEpisode().getEpisode_id() + '_' + aes.getAssEpisode().getClaim().getClaim_id() + '_' + aes.getAssEpisode().getAssigned_claim()
						
						allRows.add(row);
						
					}
				}
			}
			
		}	
		
		
		CSVWriter writer =  new CSVWriter( new FileWriter( sFileName, true ) );
		log.info("Creating " + sFileName );
		
		writer.writeAll(allRows);
		
		writer.flush();
		writer.close();
		
	}
	
	private static org.apache.log4j.Logger log = Logger.getLogger(AssociationLevelData.class);

}
