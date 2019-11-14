package learning.example.springbatch;

public enum Degree {

	SCIENCE("Science"), COMMERCE("Commerce"), BUSINESS("Business"), ENGINEERING("Engineering");
	
	private String value;
	
	private Degree(String value) {
		this.value = value;
	}
	
	public static Degree getEnum(String value) {
		for(Degree degree : values()) {
			if(degree.getValue().equals(value)) {
				return degree;
			}
		}
		return null;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
}
