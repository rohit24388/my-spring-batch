package learning.example.springbatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

public class PersonToEmployeeConverter implements ItemProcessor<Person, Employee> {
	
	private static final Logger log = LoggerFactory.getLogger(PersonToEmployeeConverter.class);

	@Override
	public Employee process(Person person) throws Exception {
		log.info("Processing {}", person );
		return convertPersonToEmployee(person);
	}
	
	private Employee convertPersonToEmployee(Person person) {
		String department = null;
		try {
			switch (Degree.getEnum(person.getDegree())) {
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
				return null; // a valid DegreeNajor enum value but something other than Business/Commerce/Science is filtered out by the processor
			}
		} catch (NullPointerException npe) { // This will be thrown when when the Degree is something other than
												// DegreeMajor enum
			throw new DegreeNotRecognizedException("'" + person.getDegree() + "' is not a recognized degree major!");
		}
		Employee employee = new Employee();
		employee.setPersonId(person.getPersonId());
		employee.setFirstName(person.getFirstName());
		employee.setLastName(person.getLastName());
		employee.setDepartment(department);
		
		return employee;
	}

}
