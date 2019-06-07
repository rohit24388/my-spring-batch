package learning.example.springbatch;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository("errorDao")
public class ErrorDaoImpl implements ErrorDao {

	@PersistenceContext
	private EntityManager entityManager;
	
	@Override
	@Transactional(propagation=Propagation.REQUIRES_NEW)
	public void addLog(ErrorLog log) {
		entityManager.persist(log);
		entityManager.flush();
		entityManager.clear();
	}

}
