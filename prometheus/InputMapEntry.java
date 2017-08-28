


public class InputMapEntry {
	
	
	private String targetField;
	private ArrayList<SourceAttributes> sources = new ArrayList<SourceAttributes>();
	private String _default;
	private String _fill;
	
	
	
	public String getTargetField() {
		return targetField;
	}
	public void setTargetField(String targetField) {
		this.targetField = targetField;
	}
	
	public ArrayList<SourceAttributes> getSourceField() {
		return sources;
	}
	public void setSources(ArrayList<SourceAttributes> sourceField) {
		this.sources = sourceField;
	}
	public SourceAttributes addSourceField(String sourceField) {
		SourceAttributes _sa = new SourceAttributes(sourceField);
		this.sources.add(_sa);
		return _sa;
	}
	
	public String get_default() {
		return _default;
	}
	public void set_default(String _default) {
		this._default = _default;
	}
	
	public String get_fill() {
		return _fill;
	}
	public void set_fill(String _fill) {
		this._fill = _fill;
	}
	

	
	
	
	class SourceAttributes {
		
		private String sourceFieldName;
		private Method mapMethod;
		private ArrayList<Condition> conditions;
		private ColumnFinder cf;
		private boolean concatenate = false;
		private Integer _length;
		
		SourceAttributes (String sourceField) {
			this.setSourceFieldName(sourceField);
		}
		
		public Method getMapMethod() {
			return mapMethod;
		}
		public void setMapMethod(Method mapMethod) {
			this.mapMethod = mapMethod;
		}

		public ArrayList<Condition> get_conditions() {
			return conditions;
		}
		public void set_conditions(ArrayList<Condition> _conditions) {
			this.conditions = _conditions;
		}
		
		/**
		 * parse the string into a Condition
		 * @param s
		 */
		public void add_condition (String s) {	
			if (conditions == null) {
				conditions = new ArrayList<Condition>();
			}
			conditions.add(new Condition(s));	
		}

		public String getSourceFieldName() {
			return sourceFieldName;
		}

		public void setSourceFieldName(String sourceFieldName) {
			this.sourceFieldName = sourceFieldName;
		}

		public ColumnFinder getCf() {
			return cf;
		}

		public void setCf(ColumnFinder cf) {
			this.cf = cf;
		}

		public boolean isConcatenate() {
			return concatenate;
		}

		public void setConcatenate(boolean concatenate) {
			this.concatenate = concatenate;
		}
		
		public Integer get_length() {
			return _length;
		}
		public void set_length(Integer _length) {
			this._length = _length;
		}
		
				
	}

	

}
