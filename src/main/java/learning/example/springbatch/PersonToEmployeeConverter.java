package learning.example.springbatch;

import org.springframework.batch.item.ItemProcessor;

public class PersonToEmployeeConverter implements ItemProcessor<Person, Employee> {

	@Override
	public Employee process(Person person) throws Exception {
		return convertPersonToEmployee(person);
	}
	
	private Employee convertPersonToEmployee(Person person) {
		String department = null;
		try {
			switch (DegreeMajor.getEnum(person.getDegreeMajor())) {
			case BUSINESS:
				department = Department.AGENCY.getValue();
				break;

			case COMMERCE:
				department = Department.ACTUARY.getValue();
				break;

			case SCIENCE:
				department = Department.IT.getValue();
				break;

			default:
			}
		} catch (NullPointerException npe) { // This will be thrown when when the Degree Major is something other than
												// DegreeMajor enum
			return null;
		}
		return new Employee(person.getFirstName(), person.getLastName(), department);
	}

}
