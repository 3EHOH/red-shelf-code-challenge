

public class MetaDataHeader implements Serializable {
	
	
	private static final long serialVersionUID = 1L;
	
	
	private String export_date;
	private String export_version;
	private String release_notes;
	
	public String getExport_date() {
		return export_date;
	}
	public void setExport_date(String export_date) {
		this.export_date = export_date;
	}
	public String getExport_version() {
		return export_version;
	}
	public void setExport_version(String export_version) {
		this.export_version = export_version;
	}
	public String getRelease_notes() {
		return release_notes;
	}
	public void setRelease_notes(String release_notes) {
		this.release_notes = release_notes;
	}

}
