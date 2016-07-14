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

public interface DbMultipleInsertMapper 
{
	/**
	 * Returns the values to insert in order.
	 * 
	 * @return Values to insert in order.
	 */
	public Object[] getValues();
	
	/**
	 * Returns how may fields to insert in each value group.
	 * 
	 * @return How may fields to insert in each value group.
	 */
	public int getFieldsToInsert();
	
	/**
	 * Returns how many value groups in the insert.
	 * 
	 * @return How many value groups in the insert.
	 */
	public int getValueGroups();
}
