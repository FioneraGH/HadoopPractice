package com.fionera.hadoop.hive

import java.sql.DriverManager

object DangerousDriver {
    private val driverName = "org.apache.hive.jdbc.HiveDriver"

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        try {
            Class.forName(driverName)
        } catch (e: Exception) {
            e.printStackTrace()
            System.exit(1)
        }

        val connection = DriverManager.getConnection("jdbc:hive2://single:10000/db", "hadoop", "hadoop")
        val statement = connection.createStatement()
        val tableName = "t_table"
        val resultSet = statement.executeQuery("desc $tableName")
        while (resultSet.next()) {
            println("${resultSet.getString(1)} ${resultSet.getString(2)}")
        }
        connection.close()
    }
}
