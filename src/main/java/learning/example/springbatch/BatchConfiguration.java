package learning.example.springbatch;

import java.util.List;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

import org.hibernate.SessionFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.HibernateCursorItemReader;
import org.springframework.batch.item.database.builder.HibernateCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Autowired
	public EntityManagerFactory entityManagerFactory;

	@Bean
	public HibernateCursorItemReader<Person> hibernateReader() {
		if (entityManagerFactory.unwrap(SessionFactory.class) == null) {
			throw new NullPointerException("factory is not a hibernate factory");
		}
		SessionFactory sessionFactory = entityManagerFactory.unwrap(SessionFactory.class);
		return new HibernateCursorItemReaderBuilder<Person>().name("hibernateReader").sessionFactory(sessionFactory)
				.queryString("from Person").build();
	}

	public ItemWriter<Person> customWriter() {
		ItemWriter<Person> writer = (List<? extends Person> persons) -> {
			for (Person person : persons) {
				System.out.println("Processing " + person);
			}
		};
		return writer;
	}

	@Bean
	public Job displayJob(Step step1) {
		return jobBuilderFactory.get("displayJob").incrementer(new RunIdIncrementer()).flow(step1).end().build();
	}

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1").<Person, Person>chunk(3).reader(hibernateReader()).writer(customWriter())
				.build();
	}

}
