package de.kp.works.ts.util;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Base data window class
 * @author pranab
 *
 * @param <T>
 */
public abstract class DataWindow<T> {
	protected AbstractList<T> dataWindow =  new ArrayList<T>();
	protected long count;
	protected boolean expired;
	protected boolean addFirst = true;
	
	public DataWindow() {
	}
	
	public DataWindow(boolean withSequentialAccess) {
		dataWindow = withSequentialAccess ? new LinkedList<T>() : new ArrayList<T>();
	}
	
	public DataWindow<T> withAddFirst(boolean addFirst) {
		this.addFirst = addFirst;
		return this;
	}

	public void add(T obj) {
		if (null == dataWindow) {
			dataWindow = new ArrayList<T>();
		}
		dataWindow.add(obj);
		++count;
		expire();
	}
	
	public abstract void expire();
	
	public Iterator<T> getIterator() {
		return dataWindow.iterator();
	}

	public int size() {
		return dataWindow.size();
	}
	
	public void set(int index, T obj) {
		dataWindow.set(index, obj);
	}
	
	public T get(int index) {
		return dataWindow.get(index);
	}
	
	public T getEarliest() {
		return dataWindow.get(0);
	}
	
	public T getLatest() {
		return dataWindow.get(dataWindow.size() - 1);
	}
	
	public  void processFullWindow() {
	}
	
	public void clear() {
		dataWindow.clear();
	}
	
	public abstract boolean isFull();

	public AbstractList<T> getDataWindow() {
		return dataWindow;
	}

	public void setDataWindow(AbstractList<T> dataWindow) {
		this.dataWindow = dataWindow;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public boolean isExpired() {
		return expired;
	}
	
	public AbstractList<T> cloneWindow() {
		AbstractList<T> clonedDataWindow =  new ArrayList<T>();
		Collections.copy(clonedDataWindow, dataWindow);
		return clonedDataWindow;
	}
	
	public void replaceRecent(T obj) {
		dataWindow.add(dataWindow.size()-1, obj);
	}
}
