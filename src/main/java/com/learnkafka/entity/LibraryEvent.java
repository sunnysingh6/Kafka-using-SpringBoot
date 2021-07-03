package com.learnkafka.entity;


import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.validation.Valid;

import org.jetbrains.annotations.NotNull;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;


@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
public class LibraryEvent {
	@Id
	@GeneratedValue
	public Integer libraryEventId;
	@OneToOne(mappedBy = "libraryEvent",cascade=CascadeType.ALL)
	@ToString.Exclude
	public Book book;
	@Enumerated(EnumType.STRING)
	public LibraryEventType libraryEventType;
}
