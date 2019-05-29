package learning.example.springbatch;

public class DegreeMajorNotRecognizedException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public DegreeMajorNotRecognizedException(String message) {
		super(message);
	}
}
