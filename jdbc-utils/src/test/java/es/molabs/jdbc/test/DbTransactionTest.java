/**
 * Copyright (C) 2016 Luis Moral Guerrero <luis.moral@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.molabs.jdbc.test;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.io.IOUtils;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Joiner;

import es.molabs.jdbc.DbKeyHolder;
import es.molabs.jdbc.DbQuery;
import es.molabs.jdbc.DbRunner;
import es.molabs.jdbc.DbTransaction;
import es.molabs.jdbc.exception.DbException;
import es.molabs.jdbc.test.dao.TestTableOneDao;
import es.molabs.jdbc.test.dao.TestTableOneMultipleInsertMapper;
import es.molabs.jdbc.test.dao.TestTableOneRowMapper;
import es.molabs.jdbc.test.handler.TestResultSetMetaDataHandler;

@RunWith(MockitoJUnitRunner.class)
public class DbTransactionTest 
{
	private final static String TEST_TABLE_ONE = "test_table1";
	private final static String TEST_TABLE_TWO = "test_table2";
	private final static String TEST_TABLE_THREE = "test_table3";
	
	private static JdbcConnectionPool dataSource = null;
	private static DbRunner dbRunner = null;
	
	@Test
	public void testGetField() throws Throwable
	{
		String expectedValue = "varchar_value1";
		String value = null;
		
		// Starts a transaction
		DbTransaction transaction = dbRunner.getDbTransaction();
		
		// Test getting a field value
		value = transaction.getField(String.class, "SELECT varchar_field FROM " + TEST_TABLE_ONE + " WHERE id = ?", 1);		
		Assert.assertEquals("Value must be [" + expectedValue + "].", expectedValue, value);
		
		// Test getting a field that does not exists
		value = transaction.getField(String.class, "SELECT varchar_field FROM " + TEST_TABLE_ONE + " WHERE id = ?", 99999);		
		Assert.assertEquals("Value must be [" + "null" + "].", null, value);
		
		// Commits the transaction
		transaction.commit();
	}
	
	@Test
	public void testGetFieldList() throws Throwable
	{
		String varcharValueN = "varchar_value%s";
		List<String> varcharValueList = null;
		int index = 0;
		
		// Starts a transaction
		DbTransaction transaction = dbRunner.getDbTransaction();		
		
		// Test getting a field list
		varcharValueList = transaction.getFieldList(String.class, "SELECT varchar_field FROM " + TEST_TABLE_ONE + " WHERE id IN (" + Joiner.on(",").join(1, 2, 3, 4, 5) + ") ORDER BY Id");
		Iterator<String> iterator = varcharValueList.iterator();
		while (iterator.hasNext())
		{
			String value = iterator.next();
			String expectedValue = String.format(varcharValueN, ++index);
			
			Assert.assertEquals("Value must be [" + expectedValue + "].", expectedValue, value);
		}	
		
		// Test getting a field list that does not exists
		varcharValueList = transaction.getFieldList(String.class, "SELECT varchar_field FROM " + TEST_TABLE_ONE + " WHERE id IN (" + Joiner.on(",").join(9999, 10000, 10001) + ") ORDER BY Id");
		Assert.assertEquals("Value must be [" + "null" + "].", null, varcharValueList);
		
		// Commits the transaction
		transaction.commit();
	}
	
	@Test
	public void testGetObject() throws Throwable
	{
		String expectedVarcharValue = "varchar_value1";
		String expectedClobValue = "clob_value1";
		TestTableOneDao testTableOneDao = null;	
		
		// Starts a transaction
		DbTransaction transaction = dbRunner.getDbTransaction();
		
		// Test getting an object
		testTableOneDao = transaction.getObject(TestTableOneRowMapper.getInstance(), "SELECT id, varchar_field, clob_field FROM " + TEST_TABLE_ONE + " WHERE id = ?", 1);		
		Assert.assertEquals("Value must be [" + expectedVarcharValue + "].", expectedVarcharValue, testTableOneDao.getVarcharField());
		Assert.assertEquals("Value must be [" + expectedClobValue + "].", expectedClobValue, testTableOneDao.getClobField());				
		
		// Test getting an object that does not exists
		testTableOneDao = transaction.getObject(TestTableOneRowMapper.getInstance(), "SELECT id, varchar_field, clob_field FROM " + TEST_TABLE_ONE + " WHERE id = ?", 99999);		
		Assert.assertEquals("Value must be [" + "null" + "].", null, testTableOneDao);
		
		// Commits the transaction
		transaction.commit();
	}
	
	@Test
	public void testGetObjectList() throws Throwable
	{
		String varcharValueN = "varchar_value%s";
		String clobValueN = "clob_value%s";
		List<TestTableOneDao> testTableOneDaoList = null;
		int index = 0;
		
		// Starts a transaction
		DbTransaction transaction = dbRunner.getDbTransaction();		
		
		// Test getting an object list
		testTableOneDaoList = transaction.getObjectList(TestTableOneRowMapper.getInstance(), "SELECT id, varchar_field, clob_field FROM " + TEST_TABLE_ONE + " WHERE id IN (" + Joiner.on(",").join(1, 2, 3, 4, 5) + ") ORDER BY Id");
		Iterator<TestTableOneDao> iterator = testTableOneDaoList.iterator();
		while (iterator.hasNext())
		{
			TestTableOneDao value = iterator.next();
			String expectedVarcharValue = String.format(varcharValueN, ++index);
			String expectedClobValue = String.format(clobValueN, index);
			
			Assert.assertEquals("Value must be [" + expectedVarcharValue + "].", expectedVarcharValue, value.getVarcharField());
			Assert.assertEquals("Value must be [" + expectedClobValue + "].", expectedClobValue, value.getClobField());
		}	
		
		// Test getting an object list that does not exists
		testTableOneDaoList = transaction.getObjectList(TestTableOneRowMapper.getInstance(), "SELECT id, varchar_field, clob_field FROM " + TEST_TABLE_ONE + " WHERE id IN (" + Joiner.on(",").join(9999, 10000, 10001) + ") ORDER BY Id");
		Assert.assertEquals("Value must be [" + "null" + "].", null, testTableOneDaoList);
		
		// Commits the transaction
		transaction.commit();
	}
	
	@Test
	public void testExecuteUpdate() throws Throwable
	{		
		String expectedValue = "varchar_value6";
		String value = null;		
		
		// Executes a insert statement and rollbacks it
		DbTransaction transaction = dbRunner.getDbTransaction();
		transaction.executeUpdate("INSERT INTO " + TEST_TABLE_ONE + " (varchar_field, clob_field) VALUES (?, ?);", expectedValue, "clob_value6");		
		transaction.rollback();
		
		// Checks that the value is not inserted
		value = dbRunner.getDbNonTransaction().getField(String.class, "SELECT varchar_field FROM " + TEST_TABLE_ONE + " WHERE varchar_field = ?", expectedValue);		
		Assert.assertEquals("Value must be [" + "null" + "].", null, value);
				
		// Executes a insert statement and commits it
		transaction = dbRunner.getDbTransaction();
		transaction.executeUpdate("INSERT INTO " + TEST_TABLE_ONE + " (varchar_field, clob_field) VALUES (?, ?);", expectedValue, "clob_value6");		
		transaction.commit();
		
		// Checks that the value is correctly inserted
		value = dbRunner.getDbNonTransaction().getField(String.class, "SELECT varchar_field FROM " + TEST_TABLE_ONE + " WHERE varchar_field = ?", expectedValue);		
		Assert.assertEquals("Value must be [" + expectedValue + "].", expectedValue, value);
	}
	
	@Test
	public void testExecuteBatchUpdate() throws Throwable
	{
		String firstVarcharValue = "varchar_value_batch1";
		String secondVarcharValue = "varchar_value_batch2";
		String thridVarcharValue = "varchar_value_batch3";
		
		Object[][] arguments = new Object[3][2];
		arguments[0][0] = firstVarcharValue;
		arguments[0][1] = "";
		arguments[1][0] = secondVarcharValue;
		arguments[1][1] = "";
		arguments[2][0] = thridVarcharValue;
		arguments[2][1] = "so";
				
		// Executes a batch insert statement and commits it
		DbTransaction transaction = dbRunner.getDbTransaction();
		transaction.executeBatchUpdate("INSERT INTO " + TEST_TABLE_ONE + " (varchar_field, clob_field) VALUES (?, ?)", arguments);		
		transaction.commit();
		
		// Checks that the fields are correctly inserted
		transaction = dbRunner.getDbTransaction();
		long value = transaction.getField(Long.class, "SELECT COUNT (id) FROM " + TEST_TABLE_ONE + " WHERE varchar_field IN (?, ?, ?)", firstVarcharValue, secondVarcharValue, thridVarcharValue);
		Assert.assertEquals("Value must be [" + arguments.length + "].", arguments.length, value);
		transaction.commit();
	}
	
	@Test
	public void testExecuteUpdateWithKeys() throws Throwable
	{
		long expectedId = 1;
		
		// Executes a insert statement and commits it
		DbTransaction transaction = dbRunner.getDbTransaction();
		DbKeyHolder keyHolder = transaction.executeUpdateWithKeys("INSERT INTO " + TEST_TABLE_TWO + " (varchar_field, clob_field) VALUES (?, ?)", "aa", "bb");		
		transaction.commit();
		
		Assert.assertEquals("Value must be [" + expectedId + "].", expectedId, keyHolder.getFirstKey(Long.class).longValue());
	}
	
	@Test
	public void testGetResultSetMetaData() throws Throwable
	{
		String expectedName = "varchar_field";
		
		// Executes a insert statement and commits it
		DbTransaction transaction = dbRunner.getDbTransaction();
		
		TestResultSetMetaDataHandler handler = transaction.getResultSetMetaData(new TestResultSetMetaDataHandler(), "SELECT * FROM " + TEST_TABLE_ONE);		
		transaction.commit();
		
		Assert.assertEquals("Value must be [" + expectedName + "].", expectedName, handler.getSecondColumnName().toLowerCase());
	}
	
	@Test
	public void testMultipleInsert() throws Throwable
	{
		TestTableOneMultipleInsertMapper multipleInsertMapper = new TestTableOneMultipleInsertMapper();
		int expectedCount = multipleInsertMapper.getValueGroups();		
		
		// Executes a multiple insert statement and commits it
		DbTransaction transaction = dbRunner.getDbTransaction();
		int inserts = transaction.multipleInsert("INSERT INTO " + TEST_TABLE_THREE + " (varchar_field, clob_field) VALUES ", multipleInsertMapper);		
		transaction.commit();
		
		// Checks that two rows where updated
		Assert.assertEquals("Value must be [" + expectedCount + "].", expectedCount, inserts);
		
		// Checks that there are two rows in the database
		transaction = dbRunner.getDbTransaction();
		long count = transaction.getField(Long.class, "SELECT COUNT (id) FROM " + TEST_TABLE_THREE);
		transaction.commit();
		
		Assert.assertEquals("Value must be [" + expectedCount + "].", expectedCount, count);		
	}
	
	@Test
	public void testRollBackOnDbException() throws Throwable
	{
		String expectedValue = "varchar_value77";
		String value = null;
		DbTransaction transaction = null;
		boolean connectionClosed = false;
		
		try
		{		
			// Executes a insert statement
			transaction = dbRunner.getDbTransaction();
			transaction.executeUpdate("INSERT INTO " + TEST_TABLE_ONE + " (varchar_field, clob_field) VALUES (?, ?);", expectedValue, "clob_value77");
			// Throws an exception
			transaction.executeUpdate("WRONG SQL");		
			transaction.commit();
		}
		catch (Throwable t)
		{			
		}
		
		// Checks that the value is not inserted
		value = dbRunner.getDbNonTransaction().getField(String.class, "SELECT varchar_field FROM " + TEST_TABLE_ONE + " WHERE varchar_field = ?", expectedValue);		
		Assert.assertEquals("Value must be [" + "null" + "].", null, value);
		
		try
		{
			// Checks getting a field value
			transaction.getField(Integer.class, "SELECT 1");
		}
		catch (DbException DBe)
		{
			// If there is a "null connection" SQLException then the connection is closed
			connectionClosed = (DBe.getCause() instanceof SQLException && ((SQLException) DBe.getCause()).getMessage().toLowerCase().equals("null connection"));
		}
		
		// Checks that the connection is closed
		Assert.assertEquals("Value must be [" + true + "].", true, connectionClosed);
	}
	
	@Test
	public void testSingleDbQueryPerThread()
	{
		// Starts a transaction
		DbTransaction firstTransaction = dbRunner.getDbTransaction();
		
		// Gets another transaction without finishing the previous one
		DbTransaction secondTransaction = dbRunner.getDbTransaction();
		
		// Gets a non transaction without finishing the previous queries
		DbQuery firstQuery = dbRunner.getDbNonTransaction();
		
		// Gets another transaction without finishing the previous queries
		DbTransaction thirdTransaction = dbRunner.getDbTransaction();
		
		//  Gets a non transaction without finishing the previous queries
		DbQuery secondQuery = dbRunner.getDbNonTransaction();
		
		// Check that all queries are the same
		Assert.assertEquals("Value must be [" + firstTransaction + "].", firstTransaction, secondTransaction);
		Assert.assertEquals("Value must be [" + firstTransaction + "].", firstTransaction, firstQuery);
		Assert.assertEquals("Value must be [" + firstTransaction + "].", firstTransaction, thirdTransaction);
		Assert.assertEquals("Value must be [" + firstTransaction + "].", firstTransaction, secondQuery);
		
		// Ends the transaction
		firstTransaction.rollback();
	}
	
	@Test
	public void testConnectionClosed() throws Throwable
	{
		// Starts a transaction
		DbTransaction transaction = dbRunner.getDbTransaction();
		
		transaction.getField(Integer.class, "SELECT id FROM " + TEST_TABLE_ONE + " WHERE id = ?", 1);		
		// Checks that there is one active connection
		Assert.assertEquals("Value must be [" + 1 + "].", 1, dataSource.getActiveConnections());
		
		transaction.getField(Integer.class, "SELECT id FROM " + TEST_TABLE_ONE + " WHERE id = ?", 1);		
		//  Checks that there is one active connection
		Assert.assertEquals("Value must be [" + 1 + "].", 1, dataSource.getActiveConnections());
		
		// Ends the transaction
		transaction.commit();
		
		// Checks that there is no active connections
		Assert.assertEquals("Value must be [" + 0 + "].", 0, dataSource.getActiveConnections());
	}
	
	@BeforeClass
	public static void runBeforeClass() throws Throwable
	{
		String url = "jdbc:h2:./src/test/resources/test-db;"; 
		url = url + "INIT=CREATE SCHEMA IF NOT EXISTS TEST_SCHEMA\\;";
		url = url + "SET SCHEMA TEST_SCHEMA";	
			
		dataSource = JdbcConnectionPool.create(url, "testUser", "testPassword");;
		dbRunner = new DbRunner(dataSource);
		
		QueryRunner query = new QueryRunner(dataSource);
		query.update(readSql("/es/molabs/jdbc/test/script/create_tables.sql"));
	}
	
	private static String readSql(String path) throws Throwable
	{
		String sql = null;
		
		InputStream inputStream = DbTransactionTest.class.getResourceAsStream(path);
		
		sql = IOUtils.toString(inputStream, Charset.defaultCharset());
		inputStream.close();
		
		return sql;
	}
	
	@AfterClass
	public static void runAfterClass() throws Throwable
	{
		// Drops all the tables and deletes the database file
		QueryRunner query = new QueryRunner(dataSource);
		query.update("DROP ALL OBJECTS DELETE FILES");
		
		dbRunner = null;
	}
}
