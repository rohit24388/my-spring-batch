package learning.example.springbatch;

public class DegreeNotRecognizedException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public DegreeNotRecognizedException(String message) {
		super(message);
	}
}
