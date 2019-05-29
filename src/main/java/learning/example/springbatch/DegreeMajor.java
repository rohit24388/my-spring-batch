package learning.example.springbatch;

public enum DegreeMajor {

	SCIENCE("Science"), COMMERCE("Commerce"), BUSINESS("Business");
	
	private String value;
	
	private DegreeMajor(String value) {
		this.value = value;
	}
	
	public static DegreeMajor getEnum(String value) {
		for(DegreeMajor degreeMajor : values()) {
			if(degreeMajor.getValue().equals(value)) {
				return degreeMajor;
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
