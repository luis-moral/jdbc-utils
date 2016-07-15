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

import org.apache.commons.dbutils.DbUtils;

import es.molabs.jdbc.exception.DbException;

public class DbTransaction extends DbQuery
{
	private Connection connection = null;
	private boolean readOnly;
	
	DbTransaction(DbManager dbManager) throws DbException
	{
		this(dbManager, false);
	}
	
	DbTransaction(DbManager dbManager, boolean readOnly) throws DbException 
	{
		super(dbManager);
		
		this.readOnly = readOnly;
		
		try
		{
			// Sets this transaction as active
			dbManager.setActiveDbTransaction(this);
			
			connection = getQueryRunner().getDataSource().getConnection();
			connection.setAutoCommit(false);
			connection.setReadOnly(readOnly);
		}
		catch (Throwable t)
		{
			throw new DbException(t);
		}
	}

	public void commit() throws DbException 
	{		
		try
		{
			DbUtils.commitAndClose(connection);
		}
		catch (Throwable t)
		{
			throw new DbException(t);
		}
		finally
		{
			connection = null;
			
			getDbManager().clearActiveDbTransaction();
		}
	}
	
	public void rollback() throws DbException
	{
		try
		{
			DbUtils.rollbackAndClose(connection);
		}
		catch (Throwable t)
		{
			throw new DbException(t);
		}
		finally
		{
			connection = null;
			
			getDbManager().clearActiveDbTransaction();	
		}
	}
	
	public boolean isReadOnly()
	{
		return readOnly;
	}

	protected Connection getConnection() throws SQLException 
	{
		return connection;
	}
	
	protected boolean closeAfter()
	{
		return false;
	}
	
	protected void handleException(Throwable t) throws DbException 
	{
		rollback();
		
		throw new DbException(t);
	}
	
	protected void finalize() throws Throwable
	{
		if (connection != null)
		{
			rollback();
			
			getLogger().warn("Rollback() called on finalize, this should not happen.");
		}
	}
}
