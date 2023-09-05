package com.example.dbmigrationbatch

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import kotlin.system.exitProcess

@SpringBootApplication
class DbMigrationBatchApplication

fun main(args: Array<String>) {
    exitProcess(SpringApplication.exit(runApplication<DbMigrationBatchApplication>(*args)))
}
