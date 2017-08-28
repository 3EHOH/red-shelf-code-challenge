



public class EpisodeData {
	
	public void writeEpisodeLines(ArrayList<EpisodeShell> allEpisodes) throws IOException {
		
		log.info("Starting Episode Data Output with: " + allEpisodes.size() + " episodes to process" );
		
		String[] headerRow = new String[38];
		
		headerRow[0] = "member_id";
		headerRow[1] = "claim_id";
		headerRow[2] = "claim_line_id";
		headerRow[3] = "episode_id";
		headerRow[4] = "episode_type";
		headerRow[5] = "trig_begin_date";
		headerRow[6] = "trig_end_date";
		headerRow[7] = "episode_begin_date";
		headerRow[8] = "episode_end_date";
		headerRow[9] = "orig_episode_begin_date";
		headerRow[10] = "orig_episode_end_date";
		headerRow[11] = "look_back";
		headerRow[12] = "look_ahead";
		headerRow[13] = "req_conf_claim";
		headerRow[14] = "conf_claim_id";
		headerRow[15] = "conf_claim_line_id";
		headerRow[16] = "min_code_separation";
		headerRow[17] = "max_code_separation";
		headerRow[18] = "trig_by_episode_id";
		// this information is redundant, the episode claim is already the claim of the episode that triggered it...
		//headerRow[19] = "trig_by_episode";
		//headerRow[19] = "trig_by_episode_claim_id";
		//headerRow[20] = "trig_by_episode_claim_line_id";
		headerRow[19] = "dxPassCode";
		headerRow[20] = "pxPassCode";
		headerRow[21] = "emPassCode";
		headerRow[22] = "conf_dxPassCode";
		headerRow[23] = "conf_pxPassCode";
		headerRow[24] = "conf_emPassCode";
		headerRow[25] = "dropped";
		headerRow[26] = "truncated";
		headerRow[27] = "win_claim_id";
		//headerRow[28] = "win_claim_line_id";
		headerRow[28] = "isAssociated";
		headerRow[29] = "AssociationLevel";
		headerRow[30] = "AssociationCount";
		
		headerRow[31] = "AttribCostPhysician";
		headerRow[32] = "AttribCostFacility";
		headerRow[33] = "AttribVisitPhysician";
		headerRow[34] = "AttribVisitFacility";
		headerRow[35] = "master_episode_id";
		headerRow[36] = "win_master_episode_id";
		headerRow[37] = "trig_by_master_episode_id";
		/*
		headerRow[31] = "AssignedDollarsSplit";
		headerRow[32] = "AssignedDollarsUnsplit";
		headerRow[33] = "AssignedDollarsSplit_T";
		headerRow[34] = "AssignedDollarsSplit_TC";
		headerRow[35] = "AssignedDollarsSplit_C";
		headerRow[36] = "AssignedDollarsUnsplit_T";
		headerRow[37] = "AssignedDollarsUnsplit_TC";
		headerRow[38] = "AssignedDollarsUnsplit_C";
		headerRow[39] = "AssociatedDollarsSplit";
		headerRow[40] = "AssociatedDollarsUnSplit";
		headerRow[41] = "AssociatedDollarsSplit_T";
		headerRow[42] = "AssociatedDollarsSplit_C";
		headerRow[43] = "AssociatedDollarsUnSplit_T";
		headerRow[44] = "AssociatedDollarsUnSplit_C";
		*/
		List<String[]> allRows = new ArrayList<String[]>();
		
		String sFileName = FileNamer.getFileName("episodeData") + ".csv";
		File f = new File(sFileName);
		if(!f.exists()) {
			allRows.add(headerRow);
		}
		
		
		String[] row = new String[38];
		
		DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
		
		for (EpisodeShell es : allEpisodes) {
			row = new String[38];
	
			String reqConf = "0";
			if (es.isReq_conf_claim()) { reqConf = "1"; }
			
			String isDrop = "0";
			if (es.isDropped()) { isDrop = "1"; }
			
			String isTrunc = "0";
			if (es.isTruncated()) { isTrunc = "1"; }
			
			String isAss = "0";
			if (es.isAssociated()) { isAss="1"; }
			
			String origBegin = "";
			if (es.getOrig_episode_begin_date()!=null) {
				origBegin=df.format(es.getOrig_episode_begin_date());
			}
			
			String origEnd = "";
			if (es.getOrig_episode_end_date()!=null) {
				origEnd = df.format(es.getOrig_episode_end_date());
			}
			
			String confClaimLineId = "";
			if (es.getConf_claim()!=null) { 
				confClaimLineId=es.getConf_claim().getClaim_line_id();
			}
				
			
			
			row[0] = es.getMember_id();
			row[1] = es.getClaim_id();
			row[2] = es.getClaim().getClaim_line_id();
			row[3] = es.getEpisode_id();
			row[4] = es.getEpisode_type();
			row[5] = df.format(es.getTrig_begin_date());
			row[6] = df.format(es.getTrig_end_date());
			row[7] = df.format(es.getEpisode_begin_date());
			row[8] = es.getEpisode_end_date() == null ? "n/a" : df.format(es.getEpisode_end_date());
			row[9] = origBegin;
			row[10] = origEnd;
			row[11] = "" + es.getLook_back();
			row[12] = "" + es.getLook_ahead();
			row[13] = reqConf;
			row[14] = es.getConf_claim_id();
			row[15] = confClaimLineId;
			row[16] = es.getMin_code_separation();
			row[17] = es.getMax_code_separation();
			row[18] = es.getTrig_by_episode_id();
			row[19] = es.getDxPassCode();
			row[20] = es.getPxPassCode();
			row[21] = es.getEmPassCode();
			row[22] = es.getConf_dxPassCode();
			row[23] = es.getConf_pxPassCode();
			row[24] = es.getConf_emPassCode();
			row[25] = isDrop;
			row[26] = isTrunc;
			row[27] = es.getWin_claim_id();
			row[28] = isAss;
			row[29] = "" + es.getAssociatedLevel();
			row[30] = "" + es.getAssociationCount();
			row[31] = es.getAttr_cost_physician_id();
			row[32] = es.getAttr_cost_facility_id();
			row[33] = es.getAttr_visit_physician_id();
			row[34] = es.getAttr_visit_facility_id();
			row[35] = es.getMaster_episode_id();
			row[36] = es.getWin_master_episode_id();
			row[37] = es.getTrig_by_master_episode_id();
			/*
			row[31] = es.getAssignedDollarsSplit().toString();
			row[32] = es.getAssignedDollarsUnsplit().toString();
			row[33] = es.getAssignedDollarsSplit_T().toString();
			row[34] = es.getAssignedDollarsSplit_TC().toString();
			row[35] = es.getAssignedDollarsSplit_C().toString();
			row[36] = es.getAssignedDollarsUnsplit_T().toString();
			row[37] = es.getAssignedDollarsUnsplit_TC().toString();
			row[38] = es.getAssignedDollarsUnsplit_C().toString();
			row[39] = es.getAssociatedDollarsSplit().toString();
			row[40] = es.getAssociatedDollarsUnsplit().toString();
			row[41] = es.getAssociatedDollarsSplit_T().toString();
			row[42] = es.getAssociatedDollarsSplit_C().toString();
			row[43] = es.getAssociatedDollarsUnsplit_T().toString();
			row[44] = es.getAssociatedDollarsUnsplit_C().toString();
			*/
			allRows.add(row);
		}
		
		
		CSVWriter writer =  new CSVWriter( new FileWriter( sFileName, true ) );
		log.info("Creating " + sFileName );
		
		writer.writeAll(allRows);
		
		writer.flush();
		writer.close();
		
	}
	
	private static org.apache.log4j.Logger log = Logger.getLogger(EpisodeData.class);

}
