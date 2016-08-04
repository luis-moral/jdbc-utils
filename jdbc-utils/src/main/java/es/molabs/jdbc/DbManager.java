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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.molabs.jdbc.exception.DbException;
import es.molabs.jdbc.exception.DbTransactionInProgressException;

public class DbManager
{
	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	private final static String THREAD_LOCAL_DBTRANSACTION = "t_l_dbTransaction";
		
	private QueryRunner queryRunner = null;
	private boolean initialized;
	
	public DbManager()
	{
		initialized = false;
	}
	
	public void init(DataSource dataSource)
	{		
		if (!initialized)
		{
			queryRunner = new QueryRunner(dataSource);
			
			testConnection();			
			
			// Sets the manager as initialized
			initialized = true;
			
			logger.info("Initialized.");
		}
		else
		{
			logger.warn("Already initialized.");
		}
	}
	
	public void destroy()
	{
		if (initialized)
		{
			// Sets the manager as not initialized
			initialized = false;
			
			queryRunner = null;
			
			logger.info("Destroyed.");
		}
		else
		{
			logger.warn("Already destroyed.");
		}
	}
	
	public boolean isInitialized()
	{
		return initialized;
	}
	
	public DataSource getDataSource()
	{
		return (queryRunner != null ? queryRunner.getDataSource() : null);
	}
	
	QueryRunner getQueryRunner()
	{
		return queryRunner;
	}
	
	public DatabaseMetaData getDatabaseMetaData() throws DbException 
	{
		try
		{
			return getDataSource().getConnection().getMetaData();
		}
		catch (SQLException SQLe)
		{
			throw new DbException(SQLe);
		}
	}
	
	/**
	 * Returns a new DbNonTransaction. If there is a transaction already in progress this method will return that transaction instead.
	 * 
	 * @return A new DbNonTransaction or the in progress transaction.
	 */
	public DbQuery getDbNonTransaction() throws DbException 
	{
		// Gets the active transaction
		DbQuery dbQuery = getActiveDbTransaction();
				
		// If it does not exists
		if (dbQuery == null)
		{
			// Creates a new non transactional query
			dbQuery = newDbNonTransaction();
		}
				
		return dbQuery;
	}
	
	/**
	 * Returns a new read only DbTransaction. If there is a transaction already in progress this method will return that transaction instead.
	 * 
	 * @return A new read only DbTransaction or the in progress transaction.
	 */
	public DbTransaction getReadOnlyDbTransaction() throws DbException 
	{
		return getDbTransaction(true);
	}
	
	/**
	 * Returns a new DbTransaction. If there is a transaction already in progress this method will return that transaction instead.
	 * 
	 * @return A new DbTransaction or the in progress transaction.
	 */
	public DbTransaction getDbTransaction() throws DbException
	{
		return getDbTransaction(false);
	}
	
	private DbTransaction getDbTransaction(boolean readOnly) throws DbException
	{
		// Gets the current active transaction
		DbTransaction transaction = getActiveDbTransaction();
				
		// If it not exists
		if (transaction == null)
		{
			// Creates a new transaction
			transaction = newDbTransaction(readOnly);
		}
		
		// Returns the transaction
		return transaction;
	}	
	
	private DbNonTransaction newDbNonTransaction()
	{	
		checkActiveDbTransaction();
		
		return new DbNonTransaction(this);
	}
	
	private DbTransaction newDbTransaction(boolean readOnly)
	{
		checkActiveDbTransaction();
		
		return new DbTransaction(this, readOnly);
	}
	
	void setActiveDbTransaction(DbTransaction dbTransaction) 
	{
		ThreadLocalUtils.setThreadLocalAttribute(THREAD_LOCAL_DBTRANSACTION, dbTransaction);
	}

	DbTransaction getActiveDbTransaction() 
	{
		return ThreadLocalUtils.getThreadLocalAttribute(THREAD_LOCAL_DBTRANSACTION, DbTransaction.class);
	}

	void clearActiveDbTransaction() 
	{
		setActiveDbTransaction(null);
	}
	
	void cancelTransaction() 
	{
		DbTransaction transaction = getActiveDbTransaction();
		
		// If there is a transaction in progress in the current thread
		if (transaction != null)
		{
			try
			{
				transaction.rollback();
			}
			catch (Exception e)
			{				
			}
		}
	}
	
	private void testConnection()
	{
		getDbNonTransaction().getField(Integer.class, "SELECT 1");
	}
	
	private void checkActiveDbTransaction()
	{
		if (getActiveDbTransaction() != null)
		{
			throw new DbTransactionInProgressException();
		}
	}
}
