package learning.example.springbatch;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

import lombok.Data;

@Entity
@Data
public class Employee {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private int employeeId;
	@Column(unique=true)
	private int personId;
	private String firstName;
	private String lastName;
	private String department;	
		
	public String toString() {
		return  "person_id: " + personId + ", first_name: " + firstName + 
				", last_name: " + lastName + ", department: " + department;
	}
}
