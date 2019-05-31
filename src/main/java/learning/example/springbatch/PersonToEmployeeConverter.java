package learning.example.springbatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

public class PersonToEmployeeConverter implements ItemProcessor<Person, Employee> {
	
	private static final Logger log = LoggerFactory.getLogger(PersonToEmployeeConverter.class);

	@Override
	public Employee process(Person person) throws Exception {
		log.info("The person who is going to be conerted to Employee is - " + person );
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
			throw new DegreeMajorNotRecognizedException("'" + person.getDegreeMajor() + "' is not a recognized degree major!");
		}
		return new Employee(person.getPersonId(), person.getFirstName(), person.getLastName(), department);
	}

}
