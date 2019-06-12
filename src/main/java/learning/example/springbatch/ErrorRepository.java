package learning.example.springbatch;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
@Transactional(propagation=Propagation.REQUIRES_NEW)
public interface ErrorRepository extends JpaRepository<BatchErrorLog, Long> {

	@Query("SELECT log FROM BatchErrorLog log WHERE log.jobExecutionId=(:jobExecutionId)")
    List<BatchErrorLog> findByJobExecutionId(@Param("jobExecutionId") Long jobExecutionId);
}
