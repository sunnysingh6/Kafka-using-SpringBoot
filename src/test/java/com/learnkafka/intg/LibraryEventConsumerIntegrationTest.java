package com.learnkafka.intg;

import static org.hamcrest.CoreMatchers.isA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.consumer.LibraryEventConsumer;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.jpa.LibraryEventRepository;
import com.learnkafka.service.LibraryEventService;

import lombok.extern.slf4j.Slf4j;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events"},partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
		"spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
@Slf4j

public class LibraryEventConsumerIntegrationTest {
	
	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;
	
	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;
	
	@Autowired
	KafkaListenerEndpointRegistry endpointRegistry;
	
	@SpyBean
	LibraryEventConsumer libraryEventConsumerSpy;
	
	@SpyBean
	LibraryEventService libraryEventServiceSpy;
	
	@Autowired
	LibraryEventRepository libraryEventRepository;
	
	@Autowired
	ObjectMapper objectMapper;
	
	
	@BeforeEach
	void setUp() {
		
		for(MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
			
			ContainerTestUtils.waitForAssignment(messageListenerContainer,embeddedKafkaBroker.getPartitionsPerTopic());
		}
	}
	
	@AfterEach
	void tearDown()
	{
		libraryEventRepository.deleteAll();
		
	}
	@Test
	void publishNewLibraryEvent() throws InterruptedException, ExecutionException, JsonMappingException, JsonProcessingException {
		
		String json="{\r\n" + 
				"    \"libraryEventId\": null,\r\n" + 
				"    \"book\": {\r\n" + 
				"        \"bookId\": 457,\r\n" + 
				"        \"bookName\": \"Kafka Using Spring Boot\",\r\n" + 
				"        \"bookAuthor\": \"Dilip 1\"\r\n" + 
				"    }\r\n" + 
				"}";
		kafkaTemplate.sendDefault(json).get();
		CountDownLatch latch=new CountDownLatch(1);
		latch.await(3,TimeUnit.SECONDS);
		
		verify(libraryEventConsumerSpy,times(1)).onMessage((ConsumerRecord<Integer, String>) isA(ConsumerRecord.class));
		verify(libraryEventServiceSpy,times(1)).processLibraryEvent ((ConsumerRecord<Integer, String>) isA(ConsumerRecord.class));
		
		List<LibraryEvent> libraryEventList=(List<LibraryEvent>) libraryEventRepository.findAll();
		assert libraryEventList.size()==1;
		libraryEventList.forEach(libraryEvent->{
			
			assert libraryEvent.getLibraryEventId()!=null;
			assertEquals(456, libraryEvent.getBook().getBookId());
		});
		
	}
	
	@Test
	void publishUpdateLibraryEvent() throws JsonMappingException, JsonProcessingException, InterruptedException, ExecutionException {
		
		String json="{\r\n" + 
				"    \"libraryEventId\": null,\r\n" + 
				"    \"book\": {\r\n" + 
				"        \"bookId\": 457,\r\n" + 
				"        \"bookName\": \"Kafka Using Spring Boot\",\r\n" + 
				"        \"bookAuthor\": \"Dilip 1\"\r\n" + 
				"    }\r\n" + 
				"}";
		
		LibraryEvent libraryEvent=objectMapper.readValue(json, LibraryEvent.class);
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEventRepository.save(libraryEvent);
		
		Book updateBook=Book.builder().bookId(456).bookName("Kafka Using SpringBoot 4X").bookAuthor("Dilip").build();
		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		libraryEvent.setBook(updateBook);
		String updatedJosn=objectMapper.writeValueAsString(libraryEvent);
		
		kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(),updatedJosn).get();
		
		CountDownLatch latch=new CountDownLatch(1);
		latch.await(3,TimeUnit.SECONDS);
		
		
		verify(libraryEventConsumerSpy,times(1)).onMessage((ConsumerRecord<Integer, String>) isA(ConsumerRecord.class));
		verify(libraryEventServiceSpy,times(1)).processLibraryEvent ((ConsumerRecord<Integer, String>) isA(ConsumerRecord.class));
		
		LibraryEvent persistedlibraryEvent=libraryEventRepository.findById(libraryEvent.getLibraryEventId()).get();
		assertEquals("Kafka Using SpringBoot 4X", persistedlibraryEvent.getBook().getBookName());
		
	}
	  @Test
	    void publishModifyLibraryEvent_Not_A_Valid_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
	        //given
	        Integer libraryEventId = 123;
	        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
	        System.out.println(json);
	        kafkaTemplate.sendDefault(libraryEventId, json).get();
	        //when
	        CountDownLatch latch = new CountDownLatch(1);
	        latch.await(3, TimeUnit.SECONDS);


	        verify(libraryEventConsumerSpy, atLeast(1)).onMessage((ConsumerRecord<Integer, String>) isA(ConsumerRecord.class));
	        verify(libraryEventServiceSpy, atLeast(1)).processLibraryEvent((ConsumerRecord<Integer, String>) isA(ConsumerRecord.class));

	        Optional<LibraryEvent> libraryEventOptional = libraryEventRepository.findById(libraryEventId);
	        assertFalse(libraryEventOptional.isPresent());
	    }
	  
	  
	   @Test
	    void publishModifyLibraryEvent_Null_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
	        //given
	        Integer libraryEventId = null;
	        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
	        kafkaTemplate.sendDefault(libraryEventId, json).get();
	        //when
	        CountDownLatch latch = new CountDownLatch(1);
	        latch.await(3, TimeUnit.SECONDS);


	        verify(libraryEventConsumerSpy, atLeast(1)).onMessage((ConsumerRecord<Integer, String>) isA(ConsumerRecord.class));
	        verify(libraryEventServiceSpy, atLeast(1)).processLibraryEvent((ConsumerRecord<Integer, String>) isA(ConsumerRecord.class));
	    }
	   
	   @Test
	    void publishModifyLibraryEvent_000_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
	        //given
	        Integer libraryEventId = 000;
	        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
	        kafkaTemplate.sendDefault(libraryEventId, json).get();
	        //when
	        CountDownLatch latch = new CountDownLatch(1);
	        latch.await(3, TimeUnit.SECONDS);


	        verify(libraryEventConsumerSpy, times(3)).onMessage((ConsumerRecord<Integer, String>) isA(ConsumerRecord.class));
	        verify(libraryEventServiceSpy, times(3)).processLibraryEvent((ConsumerRecord<Integer, String>) isA(ConsumerRecord.class));
	    }
	
	
	
	
	
}
