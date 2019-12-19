package com.tiagoamp.springbootkafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;


@SpringBootApplication
@ComponentScan(basePackages={"com.tiagoamp.springbootkafka"})
public class SpringbootkafkaApplication {

	public static void main(String[] args) {
		
		// start as simple app
		ConfigurableApplicationContext context = SpringApplication.run(SpringbootkafkaApplication.class, args);
		MyKafkaController controller = (MyKafkaController) context.getBean(MyKafkaController.class);
		controller.sendMessageToKafkaTopic("my demo message1");		
		System.exit(0);
		
		// start as REST ws
		//SpringApplication.run(SpringbootkafkaApplication.class, args);
		
	}

}
