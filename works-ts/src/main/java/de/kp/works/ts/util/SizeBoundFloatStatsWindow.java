package de.kp.works.ts.util;
/*
 * Copyright (c) 2019 - 2011 Dr. Krusche & Partner PartG. All rights reserved.
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
 * @author Pranab Ghosh
 * 
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SizeBoundFloatStatsWindow extends SizeBoundWindow<Double> {

	private static final long serialVersionUID = 1199463018309098247L;

	private double mean;
	private double stdDev;
	private double sum;
	private double sumSq;
	private double min;
	private double max;
	private double median;
	private int count;
	private boolean processed;
	private boolean fullStat = true;
	
	public SizeBoundFloatStatsWindow(int maxSize) {
		super(maxSize);
	}
	
	public SizeBoundFloatStatsWindow(int maxSize, boolean fullStat) {
		super(maxSize);
		this.fullStat = fullStat;
	}
	
	public void add(Double value) {
		processed = false;
		super.add(value);
	}
	
	public  void processFullWindow() {
		sum = 0;
		count = 0;
		if (fullStat) {
			//everything
			sumSq = 0;
			min = Double.MAX_VALUE;
			max = Double.MIN_VALUE;
			List<Double> values = new ArrayList<>();
			for (Double val : dataWindow) {
				sum += val;
				sumSq += val * val;
				if (val < min)
					min = val;
				if (val > max)
					max = val;
				values.add(val);
				++count;
			}
			
			mean = sum / count;
			double var = sumSq / count - mean * mean;
			var = (var * (count -1)) / count;
			stdDev = Math.sqrt(var);
			
			Collections.sort(values);
			int size = size();
			int mid = size / 2;
			if (size % 2 == 1) {
				median = values.get(mid);
			} else {
				median = (values.get(mid -1) + values.get(mid)) / 2;
			}
		} else {
			//mean only
			for (Double val : dataWindow) {
				sum += val;
				++count;
			}
			mean = sum / count;
		}
		processed = true;
	}

	public double getMean() {
		return mean;
	}

	public double getStdDev() {
		return stdDev;
	}

	public double getMin() {
		return min;
	}

	public double getMax() {
		return max;
	}

	public double getMedian() {
		return median;
	}

	public boolean isProcessed() {
		return processed;
	}

	public void setProcessed(boolean processed) {
		this.processed = processed;
	}
	
	public void forcedProcess() {
		if (!processed) {
			 processFullWindow();
		}
	}

}