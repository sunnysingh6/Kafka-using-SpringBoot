package com.learnkafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import com.learnkafka.service.LibraryEventService;

import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {
	
	@Autowired
	LibraryEventService libraryEventService;
	

	
	@Bean
	public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
			ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
			ConsumerFactory<Object, Object> kafkaConsumerFactory) {
		ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		configurer.configure(factory, kafkaConsumerFactory);
		//factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);	
		factory.setConcurrency(3);
			factory.setErrorHandler((thrownException,data)->{
				log.info("Excpetion in consumer config is {} and the record is {}",thrownException.getMessage(),data);
			});
			factory.setRetryTemplate(retryTemplate());
			factory.setRecoveryCallback(context->{
				if(context.getLastThrowable().getCause() instanceof RecoverableDataAccessException) {
					
					
//					log.info("Inside the recoverable logic");
//					Arrays.asList(context.attributeNames()).forEach(names->{
//						log.info("attribute name is {}",names);
//						log.info("attribute Value is  {}",context.getAttribute(names));
//					});
					ConsumerRecord<Integer, String> consumerRecord=(ConsumerRecord<Integer, String>) context.getAttribute("record");
					libraryEventService.hadleRecovery(consumerRecord);
				}
				else {
					log.info("Inside the Non Recoverable logic");
					throw new RuntimeException(context.getLastThrowable().getMessage());
				}
				return null;
			});
		return factory;
	}

	private RetryTemplate retryTemplate() {
		// TODO Auto-generated method stub
		FixedBackOffPolicy fixedBackOffPolicy=new FixedBackOffPolicy();
		fixedBackOffPolicy.setBackOffPeriod(1000);
		RetryTemplate retryTemplate=new RetryTemplate();
		retryTemplate.setRetryPolicy(simpleRetryPolicy());
		retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
		return retryTemplate;
	}

	private RetryPolicy simpleRetryPolicy() {
		// TODO Auto-generated method stub
		
//		SimpleRetryPolicy simpleRetryPolicy=new SimpleRetryPolicy(); 
//		simpleRetryPolicy.setMaxAttempts(3);
		Map<Class<? extends Throwable>, Boolean> excpetionsMap=new HashMap<>();
		excpetionsMap.put(IllegalArgumentException.class,false);
		excpetionsMap.put(RecoverableDataAccessException.class, true);
		SimpleRetryPolicy simpleRetryPolicy=new SimpleRetryPolicy(3,excpetionsMap,true);
		
		return simpleRetryPolicy;
	}

}
