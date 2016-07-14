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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DbKeyHolder 
{
	private List<Map<String, Object>> keyList = null;
	
	public DbKeyHolder(ResultSet resultSet) throws SQLException
	{
		keyList = new ArrayList<Map<String, Object>>();
		
		ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
				
		while (resultSet.next())
		{
			Map<String, Object> key = new HashMap<String, Object>(resultSetMetaData.getColumnCount());
			
			// Por cada columna del ResultSet
			for (int i=0; i<resultSetMetaData.getColumnCount(); i++)
			{
				key.put(resultSetMetaData.getColumnLabel(i+1), resultSet.getObject(i+1));
			}
			
			keyList.add(key);
		}
	}

	public List<Map<String, Object>> getKeyList()
	{
		return keyList;
	}
	
	public Map<String, Object> getKeyMap(int index)
	{
		return keyList.get(index);
	}
	
	public<T> T getKey(String name, Class<T> clazz)
	{
		return getKey(0, name, clazz);
	}
	
	@SuppressWarnings("unchecked")
	public<T> T getKey(int index, String name, Class<T> clazz)
	{
		return (T) keyList.get(index).get(name);
	}

	@SuppressWarnings("unchecked")
	public<T> T getFirstKey(Class<T> clazz) 
	{
		try
		{
			return (T) keyList.get(0).entrySet().iterator().next().getValue();
		}
		catch (Throwable t)
		{
			return null;
		}
	}
}
