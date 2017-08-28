
public class AssociatedEpisode {
	
	EpisodeShell assEpisode; // the associated episode
	AssociationMetaData assMetaData; // the metadata for this association
	EpisodeShell parentEpisode; // the episode to which this other is potentially associated.

	boolean isDropped;
	boolean subSubToProc;

	public AssociatedEpisode() {
		isDropped = false;
		subSubToProc = false;
		
	}
	
	public EpisodeShell getAssEpisode() {
		return assEpisode;
	}

	public void setAssEpisode(EpisodeShell assEpisode) {
		this.assEpisode = assEpisode;
	}

	public AssociationMetaData getAssMetaData() {
		return assMetaData;
	}

	public void setAssMetaData(AssociationMetaData assMetaData) {
		this.assMetaData = assMetaData;
	}
	
	public boolean isDropped() {
		return isDropped;
	}

	public void setDropped(boolean isDropped) {
		this.isDropped = isDropped;
	}
	
	public EpisodeShell getParentEpisode() {
		return parentEpisode;
	}

	public void setParentEpisode(EpisodeShell parentEpisode) {
		this.parentEpisode = parentEpisode;
	}
	
	public boolean isSubSubToProc() {
		return subSubToProc;
	}

	public void setSubSubToProc(boolean subSubToProc) {
		this.subSubToProc = subSubToProc;
	}
	
}
