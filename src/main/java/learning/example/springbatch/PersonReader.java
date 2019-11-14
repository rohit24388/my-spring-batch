package learning.example.springbatch;

import org.hibernate.SessionFactory;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.database.HibernateCursorItemReader;

public class PersonReader extends HibernateCursorItemReader<Person> {

	PersonReader(SessionFactory sessionFactory) {
		super.setName("hibernateReader");
		super.setSessionFactory(sessionFactory);
		super.setQueryString("from Person p order by p.personId");
	}
	
	@Override
	public synchronized Person read() throws Exception, UnexpectedInputException, ParseException {
		return super.read();
	}
}
