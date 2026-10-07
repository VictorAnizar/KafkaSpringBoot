package kafka.repaso.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class KafkaApplication {

	private static final Logger log = LoggerFactory.getLogger(KafkaApplication.class.getSimpleName());

	public static void main(String[] args) {

		log.info("Hola mundo kafka");

		//Creation of producer properties
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

		//create the producer

		//Send data

		//flush and close the producer
	}

}
