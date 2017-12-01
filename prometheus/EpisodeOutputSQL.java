


public class EpisodeOutputSQL extends GenericOutputSQL implements GenericOutputInterface  {
	
	@Override
	// lists will be put to SQL as their own objects, so don't do them here
	protected String doListField (Field f, Object o) throws IllegalArgumentException, IllegalAccessException {
		return ""; 
	}

}
