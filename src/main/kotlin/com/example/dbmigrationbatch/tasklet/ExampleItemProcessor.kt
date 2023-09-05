package com.example.dbmigrationbatch.tasklet

import com.example.dbmigrationbatch.domain.Person
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component

@Component
class ExampleItemProcessor : ItemProcessor<Person, Person> {

    private val log = LoggerFactory.getLogger(this::class.java)

    override fun process(item: Person): Person {
        val transformedPerson = Person(
            personId = item.personId,
            firstName = item.firstName,
            lastName = item.lastName,
        )

        log.info("Processing ($item) into ($transformedPerson)")

        return transformedPerson
    }
}
