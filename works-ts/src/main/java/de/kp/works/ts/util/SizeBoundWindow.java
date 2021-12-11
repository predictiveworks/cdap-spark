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

import java.io.Serializable;
import java.util.ArrayList;


/**
 * Sliding window bounded by a max size
 *
 */
public class SizeBoundWindow<T> extends DataWindow<T> implements Serializable {

	private static final long serialVersionUID = -1756815274670787596L;

	protected int maxSize;
	private int stepSize = 1;
	private int processStepSize = 1;
	
	public SizeBoundWindow() {
	}

	public SizeBoundWindow(int maxSize) {
		super(true);
		this.maxSize = maxSize;
	}
	
	public SizeBoundWindow(int maxSize, int stepSize) {
		this(maxSize);
		this.stepSize = stepSize;
	}
	
	public SizeBoundWindow(int maxSize, int stepSize, int processStepSize) {
		this(maxSize, stepSize);
		this.processStepSize = processStepSize;
	}
	
	@Override
	public void add(T obj) {
		if (null == dataWindow) {
			dataWindow = new ArrayList<T>();
		}
		if (addFirst) {
			dataWindow.add(obj);
			++count;
			process();
			slide();
		} else {
			process();
			dataWindow.add(obj);
			++count;
			slide();
		}
	}
	

	@Override
	public void expire() {
		//process window data
		process();
		
		//slide window
		slide();
	}
	
	private void process() {
		//process window data
		if (count % processStepSize == 0) {
			processFullWindow();
		}
	}
	
	private void slide() {
		//slide window
		if (dataWindow.size() > maxSize) {
			//manage window
			if (stepSize == maxSize) {
				//tumble
				dataWindow.clear();
			} else {
				//slide by stepSize
				if (stepSize > 0) {
					dataWindow.subList(0, stepSize).clear();
				}
			}
			expired = true;
		} else {
			expired = false;
		}
	}
	
	public boolean isFull() {
		return dataWindow.size() == maxSize;
	}

	public int getMaxSize() {
		return maxSize;
	}

	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

	public int getStepSize() {
		return stepSize;
	}

	public void setStepSize(int stepSize) {
		this.stepSize = stepSize;
	}

	public int getProcessStepSize() {
		return processStepSize;
	}

	public void setProcessStepSize(int processStepSize) {
		this.processStepSize = processStepSize;
	}
}