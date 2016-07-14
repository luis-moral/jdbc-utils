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
package es.molabs.jdbc.exception;

public class DbTransactionInProgressException extends DbException
{
	private final static long serialVersionUID = -1430023452124826291L;
	
	public DbTransactionInProgressException()
	{
		super("There is already a transaction in progress. You must finish that transaction if you want a new connection.");
	}
}
