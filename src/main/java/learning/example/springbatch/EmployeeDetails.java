package learning.example.springbatch;

public class EmployeeDetails extends Employee {
	private String degreeMajor;

	public EmployeeDetails() {		
	}
	
	public EmployeeDetails(Employee employee, String degreeMajor) {
		super(employee.getPersonId(), employee.getFirstName(), employee.getLastName(), degreeMajor);
		super.setEmployeeId(employee.getEmployeeId());
		this.degreeMajor = degreeMajor;
	}

	public String getDegreeMajor() {
		return degreeMajor;
	}

	public void setDegreeMajor(String degreeMajor) {
		this.degreeMajor = degreeMajor;
	}

	@Override
	public String toString() {
		return super.toString() + ", degree/major: " + degreeMajor;
	}
}
