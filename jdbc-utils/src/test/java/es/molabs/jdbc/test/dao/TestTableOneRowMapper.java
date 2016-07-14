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
package es.molabs.jdbc.test.dao;

import java.sql.ResultSet;
import java.sql.SQLException;

import es.molabs.jdbc.mapper.DbRowMapper;
import es.molabs.jdbc.mapper.DbRowMapperUtils;

public class TestTableOneRowMapper implements DbRowMapper<TestTableOneDao> 
{
	/*
	 * Parte estatica
	 */
	private static TestTableOneRowMapper INSTANCE = null;
	
	static
	{
		INSTANCE = new TestTableOneRowMapper();
	}
	
	public static TestTableOneRowMapper getInstance()
	{
		return INSTANCE;
	}
	
	
	/*
	 * Parte instanciada
	 */
	private TestTableOneRowMapper()
	{		
	}
	
	public TestTableOneDao mapRow(ResultSet resultSet, int rowNum) throws SQLException 
	{
		return new TestTableOneDao(resultSet.getLong(1), resultSet.getString(2), DbRowMapperUtils.clobToString(resultSet.getClob(3)));
	}
}
