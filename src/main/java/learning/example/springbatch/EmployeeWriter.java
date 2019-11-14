package learning.example.springbatch;

import java.util.List;

import javax.persistence.EntityManagerFactory;

import org.springframework.batch.item.database.JpaItemWriter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EmployeeWriter extends JpaItemWriter<Employee> {

	public EmployeeWriter(EntityManagerFactory entityManagerFactory) {
		super.setEntityManagerFactory(entityManagerFactory);
	}
	
	@Override
	public void write(List<? extends Employee> items) {
		log.info("Writing {}", items);
		super.write(items);
	}
}
