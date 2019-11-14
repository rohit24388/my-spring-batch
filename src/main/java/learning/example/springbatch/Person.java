package learning.example.springbatch;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

import lombok.Data;

@Entity
@Data
public class Person {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private int personId;
	private String firstName;
	private String lastName;
	private String degree;
}
