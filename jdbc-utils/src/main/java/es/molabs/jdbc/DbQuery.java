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
package es.molabs.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.molabs.jdbc.dbutils.GeneratedKeysHandler;
import es.molabs.jdbc.dbutils.MultipleRowHandler;
import es.molabs.jdbc.dbutils.ResultSetMetaDataHandler;
import es.molabs.jdbc.dbutils.SingleRowHandler;
import es.molabs.jdbc.exception.DbException;
import es.molabs.jdbc.handler.DbMetaDataHandler;
import es.molabs.jdbc.mapper.DbMultipleInsertMapper;
import es.molabs.jdbc.mapper.DbRowMapper;
import es.molabs.jdbc.mapper.FieldRowMapper;

public abstract class DbQuery
{
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private DbManager dbManager = null;
	private QueryRunner queryRunner = null;
	
	DbQuery(DbManager dbManager)
	{
		this.dbManager = dbManager;
		this.queryRunner = dbManager.getQueryRunner();
	}
	
	protected DbManager getDbManager()
	{
		return dbManager;
	}
	
	protected QueryRunner getQueryRunner()
	{
		return queryRunner;
	}
	
	/**
	 * Returns a single element, if there is more than one element after executing the this method will return the first one. If the query returns no elements this will return null.
	 * 
	 * @param <T> generic type of the return value.
	 * @param clazz class of the return value.
	 * @param sql query to execute. Same format that PreparedStatement.
	 * @param arguments for the SQL Query. 
	 * 
	 * @return The result if any or null.
	 * 
	 * @throws DbException If a database error is throw.
	 */
	public <T> T getField(Class<T> clazz, String sql, Object... arguments) throws DbException 
	{
		T field = null;
		Connection connection = null;
		
		try 
		{
			connection = getConnection();
			
			field = queryRunner.query(connection, sql, new SingleRowHandler<T>(new FieldRowMapper<T>()), arguments);
		} 
		catch (Exception e)
		{
			handleException(e);
		}
		finally
		{
			if (closeAfter()) closeConnection(connection);
		}
		
		return field;
	}

	/**
	 * Returns a List of elements. If the element does not exists it will return null. If the query returns no elements this will return null.
	 * 
	 * @param <T> generic type of the return value.
	 * @param clazz class of the return value.
	 * @param sql query to execute. Same format that PreparedStatement.
	 * @param arguments for the SQL Query.
	 * 
	 * @return The result if any or null.
	 * 
	 * @throws DbException If a database error is throw.
	 */
	public <T> List<T> getFieldList(Class<T> clazz, String sql, Object... arguments) throws DbException 
	{
		List<T> fieldList = null;
		Connection connection = null;
		
		try 
		{
			connection = getConnection();
			
			fieldList = queryRunner.query(connection, sql, new MultipleRowHandler<T>(new FieldRowMapper<T>()), arguments);
		} 
		catch (Exception e)
		{
			handleException(e);
		}
		finally
		{
			if (closeAfter()) closeConnection(connection);
		}
		
		return fieldList;
	}
	
	/**
	 * Returns a single element, if there is more than one element after executing the this method will return the first one. If the query returns no elements this will return null.
	 * 
	 * @param <S> generic DbRowMapper type.
	 * @param <T> generic type of the return value.
	 *
	 * @param dbRowMapper for the resulting object.
	 * @param sql query to execute. Same format that PreparedStatement.
	 * @param arguments for the SQL Query.
	 * 
	 * @return The result if any or null.
	 * 
	 * @throws DbException If a database error is throw.
	 */
	public<S extends DbRowMapper<T>, T> T getObject(S dbRowMapper, String sql, Object...arguments) throws DbException
	{
		T object = null;
		Connection connection = null;
		
		try
		{
			connection = getConnection();
			
			object = queryRunner.query(connection, sql, new SingleRowHandler<T>(dbRowMapper), arguments);
		} 
		catch (Exception e)
		{
			handleException(e);
		}
		finally
		{
			if (closeAfter()) closeConnection(connection);
		}
		
		return object;
	}
	
	/**
	 * Returns a List of elements.
	 * 
 	 * @param <S> generic DbRowMapper type.
	 * @param <T> generic type of the return value.
	 * 
	 * @param dbRowMapper for the resulting object.
	 * @param sql query to execute. Same format that PreparedStatement. If the query returns no elements this will return null.
	 * @param arguments for the SQL Query.
	 * 
	 * @return The result if any or null.
	 * 
	 * @throws DbException If a database error is throw.
	 */
	public<S extends DbRowMapper<T>, T> List<T> getObjectList(S dbRowMapper, String sql, Object...arguments) throws DbException
	{
		List<T> objectList = null;
		Connection connection = null;
		
		try
		{
			connection = getConnection();
			
			objectList = queryRunner.query(connection, sql, new MultipleRowHandler<T>(dbRowMapper), arguments);
		} 
		catch (Exception e)
		{
			handleException(e);
		}
		finally
		{
			if (closeAfter()) closeConnection(connection);
		}
		
		return objectList;
	}
	
	/**
	 * Executes the SQL statement.
	 * 
	 * @param sql query to be executed.
	 * @param arguments for the query as in PreparedStatement.
	 * 
	 * @return Either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0 for SQL statements that return nothing
	 * 
	 * @throws DbException If a database error is throw.
	 */
	public int executeUpdate(String sql, Object...arguments) throws DbException
	{
		int result = 0;
		Connection connection = null;
		
		try 
		{
			connection = getConnection();
			
			result = queryRunner.update(connection, sql, arguments);
		} 
		catch (Exception e)
		{
			handleException(e);
		}
		finally
		{
			if (closeAfter()) closeConnection(connection);
		}
		
		return result;
	}
	
	 /**
     * Execute a batch of SQL INSERT, UPDATE, or DELETE queries.
     *
     * @param sql query to be executed.
     * @param arguments An array of query replacement parameters. Each row in this array is one set of batch replacement values.
     * 
     * @return The number of rows updated per statement.
     * 
     * @throws DbException If a database error is throw.
     */
	public int[] executeBatchUpdate(String sql, Object[][] arguments) throws DbException
	{
		int[] result = null;
		Connection connection = null;
		
		try
		{
			connection = getConnection();
			
			result = queryRunner.batch(connection, sql, arguments);
		}
		catch (Exception e)
		{
			handleException(e);
		}
		finally
		{
			if (closeAfter()) closeConnection(connection);
		}
		
		return result;
	}	
	
	/**
	 * Executes the SQL statement.
	 * 
	 * @param sql query to be executed.
	 * @param arguments for the query as in PreparedStatement.
	 * 
	 * @return The DbKeyHolder with the returned keys.
	 * 
	 * @throws DbException If a database error is throw.
	 */
	public DbKeyHolder executeUpdateWithKeys(String sql, Object...arguments) throws DbException
	{
		DbKeyHolder keyHolder = null;
		Connection connection = null;
		
		try
		{
			connection = getConnection();
			
			keyHolder = queryRunner.insert(connection, sql, GeneratedKeysHandler.getInstance(), arguments);
		}
		catch (Exception e)
		{
			handleException(e);
		}
		finally
		{
			if (closeAfter()) closeConnection(connection);
		}
		
		return keyHolder;
	}	
	
	public<H extends DbMetaDataHandler> H getResultSetMetaData(H handler, String sql, Object...arguments) throws DbException
	{
		Connection connection = null;
		
		try
		{
			connection = getConnection();
			
			queryRunner.query(connection, sql, new ResultSetMetaDataHandler(handler), arguments);
		}
		catch (Exception e)
		{
			handleException(e);
		}
		finally
		{
			if (closeAfter()) closeConnection(connection);
		}
		
		return handler;
	}
	
	/**
	 * Executes a multiple value INSERT SQL statement with starting group 0.
	 * 
	 * @param sql query to be executed.
	 * @param dbMultipleInsertMapper for query's arguments.
	 * 
	 * @return Either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0 for SQL statements that return nothing
	 * 
	 * @throws DbException If a database error is throw.
	 */
	public int multipleInsert(String sql, DbMultipleInsertMapper dbMultipleInsertMapper) throws DbException
	{
		return multipleInsert(sql, 0, dbMultipleInsertMapper);
	}
	
	/**
	 * Executes a multiple value INSERT SQL statement.
	 * 
	 * @param sql query to be executed.
	 * @param startingGroup of the dbMultiplePreparedStatementSetter to build the SQL query.
	 * @param dbMultipleInsertMapper Setter for query's arguments.
	 * 
	 * @return Either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0 for SQL statements that return nothing
	 * 
	 * @throws DbException If a database error is throw.
	 */
	public int multipleInsert(String sql, int startingGroup, DbMultipleInsertMapper dbMultipleInsertMapper) throws DbException
	{
		// If there is no groups there is nothing to insert
		if (dbMultipleInsertMapper.getValueGroups() < 1) return 0;
		
		// SQL builder
		StringBuilder sqlBuilder = new StringBuilder(sql);
		
		boolean first = (startingGroup == 0 ? true : false);
		
		// For each VALUES group
		for (int i=startingGroup; i<dbMultipleInsertMapper.getValueGroups(); i++)
		{
			multipleInsertValues(first, sqlBuilder, dbMultipleInsertMapper.getFieldsToInsert());
			
			if (first) first = false;
		}
		
		// Executes the query
		return executeUpdate(sqlBuilder.toString(), dbMultipleInsertMapper.getValues());
	}
	
	/**
	 * This methods adds groups of (?, ?, ..., values) to the StringBuilder.
	 * 
	 * @param isFirst if is the first group. If true will not add a ',' to the start of the String.
	 * @param sql with the final string.
	 * @param values number of ? to add.
	 * 
	 * @return The StringBuilder with the needed characters added.
	 */
	private StringBuilder multipleInsertValues(boolean isFirst, StringBuilder sql, int values)
	{
		// If its not the first element added to the insert VALUES
		if (!isFirst) sql.append(",");
		
		sql.append("(");
		
		for (int i=0; i<values; i++)
		{
			sql.append("?");
			
			// If its not the last value
			if (i + 1 < values) sql.append(", ");
		}
		
		sql.append(")");
		
		return sql;
	}
	
	private void closeConnection(Connection connection)
	{
		try
		{
			DbUtils.close(connection);			
		}
		catch (SQLException SQLe)
		{
			throw new DbException(SQLe);
		}
	}
	
	protected Logger getLogger()
	{
		return logger;
	}
		
	protected abstract Connection getConnection() throws SQLException;
	
	protected abstract boolean closeAfter();
	
	protected abstract void handleException(Throwable t) throws DbException;
}
