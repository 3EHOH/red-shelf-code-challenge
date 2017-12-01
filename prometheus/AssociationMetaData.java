


public class AssociationMetaData implements Serializable {
	
	
	private static final long serialVersionUID = 1L;
	
	
	private String episode_id;
	private String episode_acronym;
	private String subsidiary_to_procedural;
	private String ass_type;
	private int ass_level;
	private String ass_start_day;
	private String ass_end_day;
	//private boolean isDropped;
	
	public String getEpisode_id() {
		return episode_id;
	}
	public void setEpisode_id(String episode_id) {
		this.episode_id = episode_id;
	}
	public String getEpisode_acronym() {
		return episode_acronym;
	}
	public void setEpisode_acronym(String episode_acronym) {
		this.episode_acronym = episode_acronym;
	}
	public String getSubsidiary_to_procedural() {
		return subsidiary_to_procedural;
	}
	public void setSubsidiary_to_procedural(String subsidiary_to_procedural) {
		this.subsidiary_to_procedural = subsidiary_to_procedural;
	}
	public String getAss_type() {
		return ass_type;
	}
	public void setAss_type(String ass_type) {
		this.ass_type = ass_type;
	}
	public int getAss_level() {
		return ass_level;
	}
	public void setAss_level(int ass_level) {
		this.ass_level = ass_level;
	}
	public String getAss_start_day() {
		return ass_start_day;
	}
	public void setAss_start_day(String ass_start_day) {
		this.ass_start_day = ass_start_day;
	}
	public String getAss_end_day() {
		return ass_end_day;
	}
	public void setAss_end_day(String ass_end_day) {
		this.ass_end_day = ass_end_day;
	}

	
	public String toString () {
		String s = "asmd";
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
