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
package es.molabs.jdbc.dbutils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.dbutils.ResultSetHandler;

import es.molabs.jdbc.mapper.DbRowMapper;

public class MultipleRowHandler<T> implements ResultSetHandler<List<T>> 
{
	private DbRowMapper<T> rowMapper = null;
	
	public MultipleRowHandler(DbRowMapper<T> rowMapper)
	{
		this.rowMapper = rowMapper;
	}
	
	public List<T> handle(ResultSet rs) throws SQLException 
	{
		List<T> list = null;
		
		int row = 0;
		
		while (rs.next())  
		{
			if (list == null) list = new LinkedList<T>();
			
			list.add(rowMapper.mapRow(rs, ++row));
        }
		
		return list;
	}
}
