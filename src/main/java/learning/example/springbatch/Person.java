package learning.example.springbatch;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

@Entity
public class Person {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private int personId;
	private String firstName;
	private String lastName;
	private String degreeMajor;
	
	public Person() {		
	}
	
	public Person(int personId, String firstName, String lastName, DegreeMajor degreeMajor) {
		this.personId = personId;
		this.firstName = firstName;
		this.lastName = lastName;
		this.degreeMajor = degreeMajor.name();
	}
	
	public int getPersonId() {
		return personId;
	}
	public void setPersonId(int personId) {
		this.personId = personId;
	}
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getLastName() {
		return lastName;
	}
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}	
	public String getDegreeMajor() {
		return degreeMajor;
	}
	public void setDegreeMajor(DegreeMajor degreeMajor) {
		this.degreeMajor = degreeMajor.name();
	}

	@Override
	public String toString() {
		return "person_id: " + personId + ", first_name: " + firstName + ", last_name: " + lastName + ", degree_major: " + degreeMajor;
	}
}
