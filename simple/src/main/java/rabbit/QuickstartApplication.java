package rabbit;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.stereotype.Controller;

@Controller
@EnableAutoConfiguration
public class QuickstartApplication {
    public static void main(String[] args) throws Exception {
        SpringApplication.run(QuickstartApplication.class, args);
    }

}
