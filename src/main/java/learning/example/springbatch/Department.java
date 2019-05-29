package learning.example.springbatch;

public enum Department {

	ACTUARY("Actuary"), AGENCY("Agency"), IT("IT");
	
	private String value;
	
	private Department(String value) {
		this.value = value;
	}
	
	public static Department getEnum(String value) {
		for(Department department : values()) {
			if(department.getValue().equals(value)) {
				return department;
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
