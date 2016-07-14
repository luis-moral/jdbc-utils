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
package es.molabs.jdbc.mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;

import es.molabs.jdbc.exception.DbException;

public final class DbRowMapperUtils 
{
	private DbRowMapperUtils()
	{
	}	
	
	public static String blobToString(Blob blob)
	{
		if (blob == null) return null;
		
		try
		{
			int offset = -1;
			int chunkSize = 1024;
			long blobLength = blob.length();
			
			if (chunkSize > blobLength) 
			{
				chunkSize = (int)blobLength;
			}
					
			char buffer[] = new char[chunkSize];
			
			StringBuilder stringBuilder = new StringBuilder();
			Reader reader = new InputStreamReader(blob.getBinaryStream());
	
			while ((offset = reader.read(buffer)) != -1) 
			{
				stringBuilder.append(buffer, 0, offset);
			}
	
			return stringBuilder.toString();
		}
		catch (SQLException SQLe)
		{
			throw new DbException(SQLe);
		}
		catch (IOException IOe)
		{
			throw new DbException(IOe);
		}			
	}
	
	public static String clobToString(Clob clob) throws SQLException
	{
		if (clob == null) return null;
		
		try
		{
			Reader reader = clob.getCharacterStream();
	        BufferedReader bufferedReader = new BufferedReader(reader);
	        				
	        // The text length should not be greater than Integer.MAX_VALUE
	        StringBuilder stringBuilder = new StringBuilder((int) clob.length());
			
			String line = null;
	        while ((line = bufferedReader.readLine()) != null) 
	        {
	        	stringBuilder.append(line);
	        }
	        
	        bufferedReader.close();
	        
	        return stringBuilder.toString();
		}
		catch (IOException IOe)
		{
			throw new SQLException(IOe);
		}
	}
}
