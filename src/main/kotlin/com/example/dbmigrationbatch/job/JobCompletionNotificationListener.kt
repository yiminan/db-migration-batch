package com.example.dbmigrationbatch.job

import com.example.dbmigrationbatch.domain.Person
import org.slf4j.LoggerFactory
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobExecutionListener
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import java.sql.ResultSet
import java.util.function.Consumer

@Component
class JobCompletionNotificationListener(
    private val jdbcTemplate: JdbcTemplate,
) : JobExecutionListener {
    private val log = LoggerFactory.getLogger(this::class.java)

    private var startDataTime: Long? = null

    override fun beforeJob(jobExecution: JobExecution) {
        log.info("!!! JOB STARTED!")
        startDataTime = System.currentTimeMillis()
    }

    override fun afterJob(jobExecution: JobExecution) {
        if (jobExecution.status == BatchStatus.COMPLETED) {
            log.info("!!! JOB FINISHED!")
            log.info("실행시간: ${executionSecond()} 초")

//            verifyExecutionResult()
        }
    }

    /**
     * 검증이 필요한 경우, 사용
     */
    private fun validateExecutionResult() {
        log.info("!!! JOB VALIDATING!")
        val validationDml = "SELECT first_name, last_name FROM test.people"
        jdbcTemplate.query<Any>(
            validationDml,
        ) { rs: ResultSet, row: Int ->
            Person(
                rs.getLong("person_id"),
                rs.getString("first_name"),
                rs.getString("last_name"),
            )
        }.forEach(
            Consumer<Any> { person: Any? ->
                log.info(
                    "Found <{{}}> in the database.",
                    person,
                )
            },
        )
    }

    private fun executionSecond() = System.currentTimeMillis()
        .minus(startDataTime!!) / 1_000
}
