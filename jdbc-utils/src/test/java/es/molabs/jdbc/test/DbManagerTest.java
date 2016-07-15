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

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.io.IOUtils;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import es.molabs.jdbc.DbManager;

@RunWith(MockitoJUnitRunner.class)
public class DbManagerTest 
{
	private static JdbcConnectionPool dataSource = null;
	private static DbManager dbManager = null;
	
	@Test
	public void testInitialization()
	{
		// Initializes the manager again
		dbManager.init(dataSource);
		
		// Checks that is still initialized
		boolean expectedValue = true;
		boolean value = dbManager.isInitialized();
		Assert.assertEquals("Value must be [" + expectedValue + "].", expectedValue, value);
		
		// Destroys the manager
		dbManager.destroy();
		
		// Checks that is not initialized
		expectedValue = false;
		value = dbManager.isInitialized();
		Assert.assertEquals("Value must be [" + expectedValue + "].", expectedValue, value);
		
		// Initializes the manager again
		dbManager.init(dataSource);
		
		// Checks that is initialized
		expectedValue = true;
		value = dbManager.isInitialized();
		Assert.assertEquals("Value must be [" + expectedValue + "].", expectedValue, value);
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
		
		InputStream inputStream = DbManagerTest.class.getResourceAsStream(path);
		
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
		
		dbManager.destroy();
		dbManager = null;
	}
}
