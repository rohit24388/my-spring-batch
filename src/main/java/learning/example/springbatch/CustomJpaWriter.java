package learning.example.springbatch;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceException;

import org.hibernate.exception.ConstraintViolationException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

@Component("customJpaWriter")
public class CustomJpaWriter implements ItemWriter<Employee> { 
	
	@PersistenceContext
	private EntityManager entityManger;
	
	@Override
	public void write(List<? extends Employee> employees) throws Exception {
		for(Employee employee : employees) {
			entityManger.persist(employee);
			try {
				entityManger.flush();
			} catch(PersistenceException e) {
				if(e.getCause() instanceof ConstraintViolationException) {
					throw new ConstraintViolationException("Tried to insert an Employee record with a non-unique person_id of " + employee.getPersonId(), null, null);
				}
				throw e;
			}
			finally {
				entityManger.clear();
			}
		}
	}

}
