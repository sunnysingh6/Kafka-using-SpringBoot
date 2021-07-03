package com.learnkafka.service;

import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.LibraryEventRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j	
public class LibraryEventService {
	@Autowired
	ObjectMapper objectMapper;
	
	@Autowired
	private LibraryEventRepository libraryEventRepository;
	
	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;
	
	public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonMappingException, JsonProcessingException {
		
		LibraryEvent libraryEvent=
				objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
		log.info("libraryEvent{}",libraryEvent);
		if(libraryEvent.getLibraryEventId()==null && libraryEvent.getLibraryEventId()==000) {
			throw new RecoverableDataAccessException("temprary network");
		}
		switch(libraryEvent.getLibraryEventType()) {
		
		case NEW :
			 save(libraryEvent);
			
			break;
			
		case UPDATE:
			
			validate(libraryEvent);
			save(libraryEvent);
			
			break;
			
		default:
			
			log.info("Invalid Librray Event Type");
		}
	}

	private void validate(LibraryEvent libraryEvent) {
		// TODO Auto-generated method stub
		
		if(libraryEvent.getLibraryEventId()==null) {
			throw new IllegalArgumentException("Library Event Id is missing");
		}
		Optional<LibraryEvent> librarOptional=libraryEventRepository.findById(libraryEvent.getLibraryEventId());
		if(!librarOptional.isPresent()) {
			throw new IllegalArgumentException("Not a Valid Library Event");
		}
		log.info("Validation Successful for the library-event :{}",librarOptional.get());
		
		
	}

	private void save(LibraryEvent libraryEvent) {
		// TODO Auto-generated method stub
		
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEventRepository.save(libraryEvent);
		log.info("Successfully persisted the library Event {}",libraryEvent);
		
	}
	public void hadleRecovery(ConsumerRecord<Integer, String> record) {

		Integer key=record.key();	
		String msg=record.value();
		
		ListenableFuture<SendResult<Integer, String>> listenablefuture=kafkaTemplate.sendDefault(key,msg);
		listenablefuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				// TODO Auto-generated method stub
				handleSuccess(key,msg,result);
				
			}

			@Override
			public void onFailure(Throwable ex) {
				try {
					handleFailure(key,msg,ex);
				} catch (Throwable e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
		});
		
	}
	
	protected void handleFailure(Integer key, String value, Throwable ex) throws Throwable {
		log.info("Error sending the message and the message {}",ex.getMessage());
	    throw ex;
			
	
	
}

protected void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
	// TODO Auto-generated method stub
	log.info("msg sent successfully for he key: {}and the value is {} partititon is {}",key,value,result.getRecordMetadata().partition());
	
}


}
