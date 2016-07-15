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
import es.molabs.jdbc.DbManager;
import es.molabs.jdbc.test.dao.TestTableOneDao;
import es.molabs.jdbc.test.dao.TestTableOneMultipleInsertMapper;
import es.molabs.jdbc.test.dao.TestTableOneRowMapper;
import es.molabs.jdbc.test.handler.TestResultSetMetaDataHandler;

@RunWith(MockitoJUnitRunner.class)
public class DbNonTransactionTest 
{	
	private final static String TEST_TABLE_ONE = "test_table1";
	private final static String TEST_TABLE_TWO = "test_table2";
	private final static String TEST_TABLE_THREE = "test_table3";
	
	private static JdbcConnectionPool dataSource = null;
	private static DbManager dbManager = null;
	
	@Test
	public void testGetField() throws Throwable
	{
		String expectedValue = "varchar_value1";
		String value = null;
		
		// Test getting a field value
		value = dbManager.getDbNonTransaction().getField(String.class, "SELECT varchar_field FROM " + TEST_TABLE_ONE + " WHERE id = ?", 1);		
		Assert.assertEquals("Value must be [" + expectedValue + "].", expectedValue, value);
		
		// Test getting a field that does not exists
		value = dbManager.getDbNonTransaction().getField(String.class, "SELECT varchar_field FROM " + TEST_TABLE_ONE + " WHERE id = ?", 99999);		
		Assert.assertEquals("Value must be [" + "null" + "].", null, value);
	}
	
	@Test
	public void testGetFieldList() throws Throwable
	{
		String varcharValueN = "varchar_value%s";
		List<String> varcharValueList = null;
		int index = 0;
		
		// Test getting a field list
		varcharValueList = dbManager.getDbNonTransaction().getFieldList(String.class, "SELECT varchar_field FROM " + TEST_TABLE_ONE + " WHERE id IN (" + Joiner.on(",").join(1, 2, 3, 4, 5) + ") ORDER BY Id");
		Iterator<String> iterator = varcharValueList.iterator();
		while (iterator.hasNext())
		{
			String value = iterator.next();
			String expectedValue = String.format(varcharValueN, ++index);
			
			Assert.assertEquals("Value must be [" + expectedValue + "].", expectedValue, value);
		}	
		
		// Test getting a field list that does not exists
		varcharValueList = dbManager.getDbNonTransaction().getFieldList(String.class, "SELECT varchar_field FROM " + TEST_TABLE_ONE + " WHERE id IN (" + Joiner.on(",").join(9999, 10000, 10001) + ") ORDER BY Id");
		Assert.assertEquals("Value must be [" + "null" + "].", null, varcharValueList);
	}
	
	@Test
	public void testGetObject() throws Throwable
	{
		String expectedVarcharValue = "varchar_value1";
		String expectedClobValue = "clob_value1";
		TestTableOneDao testTableOneDao = null;
		
		// Test getting an object
		testTableOneDao = dbManager.getDbNonTransaction().getObject(TestTableOneRowMapper.getInstance(), "SELECT id, varchar_field, clob_field FROM " + TEST_TABLE_ONE + " WHERE id = ?", 1);		
		Assert.assertEquals("Value must be [" + expectedVarcharValue + "].", expectedVarcharValue, testTableOneDao.getVarcharField());
		Assert.assertEquals("Value must be [" + expectedClobValue + "].", expectedClobValue, testTableOneDao.getClobField());
		
		// Test getting an object that does not exists
		testTableOneDao = dbManager.getDbNonTransaction().getObject(TestTableOneRowMapper.getInstance(), "SELECT id, varchar_field, clob_field FROM " + TEST_TABLE_ONE + " WHERE id = ?", 99999);		
		Assert.assertEquals("Value must be [" + "null" + "].", null, testTableOneDao);
	}
	
	@Test
	public void testGetObjectList() throws Throwable
	{
		String varcharValueN = "varchar_value%s";
		String clobValueN = "clob_value%s";
		List<TestTableOneDao> testTableOneDaoList = null;
		int index = 0;
			
		// Test getting an object list
		testTableOneDaoList = dbManager.getDbNonTransaction().getObjectList(TestTableOneRowMapper.getInstance(), "SELECT id, varchar_field, clob_field FROM " + TEST_TABLE_ONE + " WHERE id IN (" + Joiner.on(",").join(1, 2, 3, 4, 5) + ") ORDER BY Id");
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
		testTableOneDaoList = dbManager.getDbNonTransaction().getObjectList(TestTableOneRowMapper.getInstance(), "SELECT id, varchar_field, clob_field FROM " + TEST_TABLE_ONE + " WHERE id IN (" + Joiner.on(",").join(9999, 10000, 10001) + ") ORDER BY Id");
		Assert.assertEquals("Value must be [" + "null" + "].", null, testTableOneDaoList);	
	}
	
	@Test
	public void testExecuteUpdate() throws Throwable
	{		
		String expectedValue = "varchar_value6";
		String value = null;		
		
		// Executes a insert statement
		dbManager.getDbNonTransaction().executeUpdate("INSERT INTO " + TEST_TABLE_ONE + " (varchar_field, clob_field) VALUES (?, ?)", expectedValue, "clob_value6");		
		
		// Checks that the value is correctly inserted
		value = dbManager.getDbNonTransaction().getField(String.class, "SELECT varchar_field FROM " + TEST_TABLE_ONE + " WHERE varchar_field = ?", expectedValue);		
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
		arguments[2][1] = "";
						
		// Executes a batch insert statement		
		dbManager.getDbNonTransaction().executeBatchUpdate("INSERT INTO " + TEST_TABLE_ONE + " (varchar_field, clob_field) VALUES (?, ?)", arguments);		
				
		// Checks that the fields are correctly inserted	
		long value = dbManager.getDbNonTransaction().getField(Long.class, "SELECT COUNT (id) FROM " + TEST_TABLE_ONE + " WHERE varchar_field IN (?, ?, ?)", firstVarcharValue, secondVarcharValue, thridVarcharValue);
		Assert.assertEquals("Value must be [" + arguments.length + "].", arguments.length, value);
	}
	
	@Test
	public void testExecuteUpdateWithKeys() throws Throwable
	{
		long expectedId = 1;
		
		// Executes a insert statement
		DbKeyHolder keyHolder = dbManager.getDbNonTransaction().executeUpdateWithKeys("INSERT INTO " + TEST_TABLE_TWO + " (varchar_field, clob_field) VALUES (?, ?)", "aa", "bb");		
		
		Assert.assertEquals("Value must be [" + expectedId + "].", expectedId, keyHolder.getFirstKey(Long.class).longValue());
	}
	
	@Test
	public void testGetResultSetMetaData() throws Throwable
	{
		String expectedName = "varchar_field";
		
		TestResultSetMetaDataHandler handler = dbManager.getDbNonTransaction().getResultSetMetaData(new TestResultSetMetaDataHandler(), "SELECT * FROM " + TEST_TABLE_ONE);
		
		Assert.assertEquals("Value must be [" + expectedName + "].", expectedName, handler.getSecondColumnName().toLowerCase());		
	}
	
	@Test
	public void testMultipleInsert() throws Throwable
	{
		TestTableOneMultipleInsertMapper multipleInsertMapper = new TestTableOneMultipleInsertMapper();
		int expectedCount = multipleInsertMapper.getValueGroups();		
		
		// Executes a multiple insert statement	
		int inserts = dbManager.getDbNonTransaction().multipleInsert("INSERT INTO " + TEST_TABLE_THREE + " (varchar_field, clob_field) VALUES ", multipleInsertMapper);		
		
		// Checks that two rows where updated
		Assert.assertEquals("Value must be [" + expectedCount + "].", expectedCount, inserts);
		
		// Checks that there are two rows in the database
		long count = dbManager.getDbNonTransaction().getField(Long.class, "SELECT COUNT (id) FROM " + TEST_TABLE_THREE);
		
		Assert.assertEquals("Value must be [" + expectedCount + "].", expectedCount, count);		
	}
	
	@Test
	public void testConnectionClosed() throws Throwable
	{
		dbManager.getDbNonTransaction().getField(Integer.class, "SELECT id FROM " + TEST_TABLE_ONE + " WHERE id = ?", 1);		
		// Checks that there is no active connections
		Assert.assertEquals("Value must be [" + 0 + "].", 0, dataSource.getActiveConnections());
		
		dbManager.getDbNonTransaction().getField(Integer.class, "SELECT id FROM " + TEST_TABLE_ONE + " WHERE id = ?", 1);		
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
		dbManager = new DbManager();
		dbManager.init(dataSource);
		
		QueryRunner query = new QueryRunner(dataSource);
		query.update(readSql("/es/molabs/jdbc/test/script/create_tables.sql"));
	}
	
	private static String readSql(String path) throws Throwable
	{
		String sql = null;
		
		InputStream inputStream = DbNonTransactionTest.class.getResourceAsStream(path);
		
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
		
		dbManager = null;
	}
}
