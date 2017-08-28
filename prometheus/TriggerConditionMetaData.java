

public class TriggerConditionMetaData implements Serializable {
	
	
	private static final long serialVersionUID = 1L;
	
	/*
	private String ip_px_code_position;
	private String ip_em_code_position;
	private String ip_dx_code_position;
	private String op_px_code_position;
	private String op_em_code_position;
	private String op_dx_code_position;
	private String pr_px_code_position;
	private String pr_em_code_position;
	private String pr_dx_code_position;
	private String episode_id;
	private boolean requires_confirming_code;
	private String min_code_separation;
	private String max_code_separation;
	private String ipc_px_code_position;
	private String ipc_em_code_position;
	private String ipc_dx_code_position;
	private String opc_px_code_position;
	private String opc_em_code_position;
	private String opc_dx_code_position;
	private String prc_px_code_position;
	private String prc_em_code_position;
	private String prc_dx_code_position;
*/
	private String facility_type;
	private String px_code_position;
	private String em_code_position;
	private String and_or;
	private String dx_code_position;
	private String episode_id;
	private boolean req_conf_code;
	private String min_code_separation;
	private String max_code_separation;
	
/*	
	public String getIp_px_code_position() {
		return ip_px_code_position;
	}
	public void setIp_px_code_position(String ip_px_code_position) {
		this.ip_px_code_position = ip_px_code_position;
	}
	public String getIp_em_code_position() {
		return ip_em_code_position;
	}
	public void setIp_em_code_position(String ip_em_code_position) {
		this.ip_em_code_position = ip_em_code_position;
	}
	public String getIp_dx_code_position() {
		return ip_dx_code_position;
	}
	public void setIp_dx_code_position(String ip_dx_code_position) {
		this.ip_dx_code_position = ip_dx_code_position;
	}
	public String getOp_px_code_position() {
		return op_px_code_position;
	}
	public void setOp_px_code_position(String op_px_code_position) {
		this.op_px_code_position = op_px_code_position;
	}
	public String getOp_em_code_position() {
		return op_em_code_position;
	}
	public void setOp_em_code_position(String op_em_code_position) {
		this.op_em_code_position = op_em_code_position;
	}
	public String getOp_dx_code_position() {
		return op_dx_code_position;
	}
	public void setOp_dx_code_position(String op_dx_code_position) {
		this.op_dx_code_position = op_dx_code_position;
	}
	public String getPr_px_code_position() {
		return pr_px_code_position;
	}
	public void setPr_px_code_position(String pr_px_code_position) {
		this.pr_px_code_position = pr_px_code_position;
	}
	public String getPr_em_code_position() {
		return pr_em_code_position;
	}
	public void setPr_em_code_position(String pr_em_code_position) {
		this.pr_em_code_position = pr_em_code_position;
	}
	public String getPr_dx_code_position() {
		return pr_dx_code_position;
	}
	public void setPr_dx_code_position(String pr_dx_code_position) {
		this.pr_dx_code_position = pr_dx_code_position;
	}
	public String getEpisode_id() {
		return episode_id;
	}
	public void setEpisode_id(String episode_id) {
		this.episode_id = episode_id;
	}
	public boolean isRequires_confirming_code() {
		return requires_confirming_code;
	}
	public void setRequires_confirming_code(boolean requires_confirming_code) {
		this.requires_confirming_code = requires_confirming_code;
	}
	public String getMin_code_separation() {
		return min_code_separation;
	}
	public void setMin_code_separation(String min_code_separation) {
		this.min_code_separation = min_code_separation;
	}
	public String getMax_code_separation() {
		return max_code_separation;
	}
	public void setMax_code_separation(String max_code_separation) {
		this.max_code_separation = max_code_separation;
	}
	public String getIpc_px_code_position() {
		return ipc_px_code_position;
	}
	public void setIpc_px_code_position(String ipc_px_code_position) {
		this.ipc_px_code_position = ipc_px_code_position;
	}
	public String getIpc_em_code_position() {
		return ipc_em_code_position;
	}
	public void setIpc_em_code_position(String ipc_em_code_position) {
		this.ipc_em_code_position = ipc_em_code_position;
	}
	public String getIpc_dx_code_position() {
		return ipc_dx_code_position;
	}
	public void setIpc_dx_code_position(String ipc_dx_code_position) {
		this.ipc_dx_code_position = ipc_dx_code_position;
	}
	public String getOpc_px_code_position() {
		return opc_px_code_position;
	}
	public void setOpc_px_code_position(String opc_px_code_position) {
		this.opc_px_code_position = opc_px_code_position;
	}
	public String getOpc_em_code_position() {
		return opc_em_code_position;
	}
	public void setOpc_em_code_position(String opc_em_code_position) {
		this.opc_em_code_position = opc_em_code_position;
	}
	public String getOpc_dx_code_position() {
		return opc_dx_code_position;
	}
	public void setOpc_dx_code_position(String opc_dx_code_position) {
		this.opc_dx_code_position = opc_dx_code_position;
	}
	public String getPrc_px_code_position() {
		return prc_px_code_position;
	}
	public void setPrc_px_code_position(String prc_px_code_position) {
		this.prc_px_code_position = prc_px_code_position;
	}
	public String getPrc_em_code_position() {
		return prc_em_code_position;
	}
	public void setPrc_em_code_position(String prc_em_code_position) {
		this.prc_em_code_position = prc_em_code_position;
	}
	public String getPrc_dx_code_position() {
		return prc_dx_code_position;
	}
	public void setPrc_dx_code_position(String prc_dx_code_position) {
		this.prc_dx_code_position = prc_dx_code_position;
	}
*/	
	
	public String getFacility_type() {
		return facility_type;
	}

	public void setFacility_type(String facility_type) {
		this.facility_type = facility_type;
	}

	public String getPx_code_position() {
		return px_code_position;
	}

	public void setPx_code_position(String px_code_position) {
		this.px_code_position = px_code_position;
	}

	public String getEm_code_position() {
		return em_code_position;
	}

	public void setEm_code_position(String em_code_position) {
		this.em_code_position = em_code_position;
	}

	public String getAnd_or() {
		return and_or;
	}

	public void setAnd_or(String and_or) {
		this.and_or = and_or;
	}

	public String getDx_code_position() {
		return dx_code_position;
	}

	public void setDx_code_position(String dx_code_position) {
		this.dx_code_position = dx_code_position;
	}

	public String getEpisode_id() {
		return episode_id;
	}

	public void setEpisode_id(String episode_id) {
		this.episode_id = episode_id;
	}

	public boolean getReq_conf_code() {
		return req_conf_code;
	}

	public void setReq_conf_code(boolean req_conf_code) {
		this.req_conf_code = req_conf_code;
	}

	public String getMin_code_separation() {
		return min_code_separation;
	}

	public void setMin_code_separation(String min_code_separation) {
		this.min_code_separation = min_code_separation;
	}

	public String getMax_code_separation() {
		return max_code_separation;
	}

	public void setMax_code_separation(String max_code_separation) {
		this.max_code_separation = max_code_separation;
	}

	public String toString () {
		String s = "tcmd";
		String sx;
		Field[] f =  this.getClass().getDeclaredFields();
		for (int i=0; i < f.length; i++) {
			try {
				if (f[i].get(this) == null)
					continue;
				else if (f[i].getType().equals(String.class)) {
					sx = (String) f[i].get(this);
					if (sx.length() > 0)
					{
						s = s + "," + f[i].getName() + "=" + sx;
					}
				}	
			} 
			catch (IllegalArgumentException | IllegalAccessException e) {
				// do nothing
			}
		}
		return s;
	}

}
