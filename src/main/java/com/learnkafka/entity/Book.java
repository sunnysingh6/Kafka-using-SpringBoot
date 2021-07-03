package com.learnkafka.entity;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;

import org.jetbrains.annotations.NotNull;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Builder
public class Book {
	
	@Id	
	@NotNull
	private Integer bookId;
	private String bookAuthor;
	private String bookName;
	@OneToOne
	@JoinColumn(name = "librayEventId")
	private LibraryEvent libraryEvent;
	


}
