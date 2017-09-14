package com.fionera.hadoop.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.util.Bytes

object HardDriver {
    private val TABLE_NAME = TableName.valueOf("city")
    private val COLUMN_FAMILY_BASE = "base"

    private val configuration = HBaseConfiguration.create()
    private var connection: Connection? = null

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        createTable()
        putValue("1000", "name", "Test" + System.currentTimeMillis())
        putValue("1000", "age", "10")
        putValue("1000", "name", "Test" + System.currentTimeMillis())
        putValue("1001", "body", "Amazing " + System.currentTimeMillis())
        getValue("1000")
        getValue("1001")
//        deleteTable();
    }

    @Throws(Exception::class)
    private fun getConnection(): Connection {
        return if (connection == null) {
            connection = ConnectionFactory.createConnection(configuration)
            connection!!
        } else connection!!
    }

    @Throws(Exception::class)
    private fun createTable() {
        val connection = getConnection()
        val admin = connection.admin as HBaseAdmin
        if (admin.tableExists(TABLE_NAME)) {
            println("table exists.")
        } else {
            val tableDescriptor = HTableDescriptor(TABLE_NAME)
            tableDescriptor.addFamily(HColumnDescriptor(COLUMN_FAMILY_BASE))
            admin.createTable(tableDescriptor)
            println("table created.")
        }
    }

    @Throws(Exception::class)
    private fun putValue(row: String, column: String, value: String) {
        val table = getConnection().getTable(TABLE_NAME) as HTable
        val p = Put(Bytes.toBytes(row))
        p.addColumn(Bytes.toBytes(COLUMN_FAMILY_BASE), Bytes.toBytes(column), Bytes.toBytes(value))
        table.put(p)
        println("value put")
    }

    @Throws(Exception::class)
    private fun getValue(row: String) {
        val table = getConnection().getTable(TABLE_NAME) as HTable
        val g = Get(Bytes.toBytes(row))
        val result = table.get(g)
        println("value got:" + result.toString())
    }

    @Throws(Exception::class)
    private fun deleteTable() {
        val connection = getConnection()
        val admin = connection.admin as HBaseAdmin
        if (admin.tableExists(TABLE_NAME)) {
            try {
                admin.disableTable(TABLE_NAME)
                admin.deleteTable(TABLE_NAME)
                println("table deleted.")
            } catch (e: Exception) {
                e.printStackTrace()
            }
        } else {
            println("table non exists.")
        }
    }
}
