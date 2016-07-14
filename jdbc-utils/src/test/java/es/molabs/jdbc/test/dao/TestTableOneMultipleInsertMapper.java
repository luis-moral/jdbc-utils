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

import es.molabs.jdbc.mapper.DbMultipleInsertMapper;

public class TestTableOneMultipleInsertMapper implements DbMultipleInsertMapper
{	
	public Object[] getValues() 
	{
		return new Object[] {"value1-1", "value1-2", "value2-1", "value2-2"};
	}

	public int getFieldsToInsert() 
	{
		return 2;
	}

	public int getValueGroups() 
	{	
		return 2;
	}	
}
