package com.example.dbmigrationbatch.configuration

import com.example.dbmigrationbatch.domain.Person
import com.example.dbmigrationbatch.job.JobCompletionNotificationListener
import com.example.dbmigrationbatch.tasklet.ExampleItemProcessor
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider
import org.springframework.batch.item.database.JdbcBatchItemWriter
import org.springframework.batch.item.database.JdbcCursorItemReader
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder
import org.springframework.batch.item.file.FlatFileItemReader
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ClassPathResource
import org.springframework.jdbc.core.RowMapper
import org.springframework.transaction.PlatformTransactionManager
import java.sql.ResultSet
import javax.sql.DataSource

@Configuration
class ExampleConfiguration(
    private val dataSource: DataSource,
) {
    @Bean
    fun jdbcCursorItemReader(): JdbcCursorItemReader<Person> {
        val readDmlWithOrderBy = "SELECT person_id, first_name, last_name FROM test.people_sample ORDER BY person_id"
        return JdbcCursorItemReaderBuilder<Person>()
            .name("jdbcCursorItemReader")
            .dataSource(dataSource)
            .sql(readDmlWithOrderBy)
            .rowMapper(RowToObjectMapper())
            .build()
    }

    class RowToObjectMapper : RowMapper<Person> {
        override fun mapRow(rs: ResultSet, rowNum: Int): Person {
            return Person(
                personId = rs.getLong("person_id"),
                firstName = rs.getString("first_name"),
                lastName = rs.getString("last_name"),
            )
        }
    }

    @Bean
    fun flatFileItemReader(): FlatFileItemReader<Person> {
        return FlatFileItemReaderBuilder<Person>()
            .name("flatFileItemReader")
            .resource(ClassPathResource("sample-data.csv"))
            .delimited()
            .names(*arrayOf<String>("person_id", "firstName", "lastName"))
            .fieldSetMapper(
                object : BeanWrapperFieldSetMapper<Person?>() {
                    init {
                        setTargetType(Person::class.java)
                    }
                },
            )
            .build()
    }

    @Bean
    fun processor(): ExampleItemProcessor {
        return ExampleItemProcessor()
    }

    @Bean
    fun insertWriter(dataSource: DataSource): JdbcBatchItemWriter<Person> {
        return JdbcBatchItemWriterBuilder<Person>()
            .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider<Person>())
            .sql("INSERT INTO people (first_name, last_name, reg_time) VALUES (:firstName, :lastName, NOW())")
            .dataSource(dataSource)
            .build()
    }

    @Bean
    fun updateWriter(dataSource: DataSource): JdbcBatchItemWriter<Person> {
        return JdbcBatchItemWriterBuilder<Person>()
            .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider<Person>())
            .sql("UPDATE people p SET p.last_name = NOW() WHERE p.person_id = :personId")
            .dataSource(dataSource)
            .build()
    }

    @Bean
    fun exampleStep(
        jobRepository: JobRepository,
        transactionManager: PlatformTransactionManager,
        @Qualifier("insertWriter") writer: JdbcBatchItemWriter<Person>,
    ): Step {
        return StepBuilder("exampleStep", jobRepository)
            .chunk<Person, Person>(10000, transactionManager)
            .reader(jdbcCursorItemReader())
            .processor(processor())
            .writer(writer)
            .build()
    }

    @Bean
    fun exampleJob(
        jobRepository: JobRepository,
        listener: JobCompletionNotificationListener,
        step: Step,
    ): Job {
        return JobBuilder("exampleStep", jobRepository)
            .incrementer(RunIdIncrementer())
            .listener(listener)
            .flow(step)
            .end()
            .build()
    }
}
