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

import java.util.HashMap;
import java.util.Map;

public class ThreadLocalUtils 
{
	private static ThreadLocal<Map<String, Object>> threadLocalContainer = null;
	
	private ThreadLocalUtils()
	{		
	}
	
	private static ThreadLocal<Map<String, Object>> getThreadLocalContainer()
	{
		if (threadLocalContainer == null) initThreadLocalContainer();
		if (threadLocalContainer.get() == null) initThreadLocalContainerThreadInstance();
		
		return threadLocalContainer;
	}
		
	private synchronized static void initThreadLocalContainer()
	{
		if (threadLocalContainer == null)
		{
			threadLocalContainer = new ThreadLocal<Map<String, Object>>();			
		}		
	}
	
	private synchronized static void initThreadLocalContainerThreadInstance()
	{
		if (threadLocalContainer.get() == null)
		{
			threadLocalContainer.set(new HashMap<String, Object>());
		}
	}
	
	synchronized static void clearThreadLocalContainer()
	{
		if (getThreadLocalContainer() != null) getThreadLocalContainer().remove();		
	}
	
	@SuppressWarnings("unchecked")
	static<T> T getThreadLocalAttribute(String name, Class<T> clazz)
	{
		if (getThreadLocalContainer().get() != null)
		{
			return (T) getThreadLocalContainer().get().get(name);
		}
		
		return null;
	}
	
	static void setThreadLocalAttribute(String name, Object object)
	{
		getThreadLocalContainer().get().put(name, object);
	}
	
	static void removeThreadLocalAttribute(String name)
	{
		if (getThreadLocalContainer().get() != null) getThreadLocalContainer().get().remove(name);
	}
}
