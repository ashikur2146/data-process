package data.processing;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Index {
	@GetMapping("/")
	public String index() {
		return "Data processing index page...";
	}
}