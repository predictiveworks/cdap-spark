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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

public class MathUtils {

	/********************
	 * 
	 * COMMON FUNCTIONS
	 * 
	 */

	public static double biSquare(double x) {
		double bs = 0;
		if (x < 1.0) {
			bs = Math.pow((1 - x * x), 2);
		}
		return bs;
	}

	public static double[] createIndex(int size) {

		double[] index = new double[size];

		for (int i = 0; i < size; ++i) {
			index[i] = i;
		}

		return index;

	}

	public static int[] findNeighbors(double[] data, int ref, double[] neighbor) {

		int size = data.length;
		int nSize = neighbor.length;

		int beg;
		int refWithin;

		if (ref < nSize / 2) {
			beg = 0;
			refWithin = ref;

		} else if (ref > size - 1 - nSize / 2) {
			beg = size - nSize;
			refWithin = ref - beg;

		} else {
			beg = ref - nSize / 2;
			refWithin = nSize / 2;
		}

		System.arraycopy(data, beg, neighbor, 0, nSize);
		int[] result = new int[2];
		result[0] = beg;
		result[1] = refWithin;
		return result;
	}

	public static double[][] indexArray(double[] data) {

		// index data
		int size = data.length;
		double[][] table = new double[size][2];

		for (int i = 0; i < size; ++i) {
			table[i][0] = i;
			table[i][1] = data[i];
		}

		return table;

	}

	public static void getAbsolute(double[] vec, double[] absVec) {
		for (int i = 0; i < vec.length; ++i) {
			absVec[i] = Math.abs(vec[i]);
		}
	}

	public static void getAbsolute(double[] vec) {
		for (int i = 0; i < vec.length; ++i) {
			vec[i] = Math.abs(vec[i]);
		}
	}

	public static double getMedian(double[] vec) {
		double med;
		Arrays.sort(vec);
		int size = vec.length;
		int half = size / 2;
		if (size % 2 == 1) {
			med = vec[half];
		} else {
			med = (vec[half - 1] + vec[half]) / 2;
		}
		return med;
	}

	public static double linearRegressionPrediction(Pair<Double, Double> coeffs, double x) {
		return coeffs.getLeft() * x + coeffs.getRight();
	}

	public static void loessSmooth(double[] data, int neighborSize) {
		double[] neighbor = new double[neighborSize];
		double[] index = createIndex(neighborSize);
		for (int i = 0; i < data.length; ++i) {
			int localRef = findNeighbors(data, i, neighbor)[1];
			double[] weights = loessWeight(index, localRef);
			Pair<Double, Double> coeffs = weightedLinearRegression(neighbor, weights);
			data[i] = linearRegressionPrediction(coeffs, localRef);
		}
	}

	public static void loessSmooth(double[] data, int neighborSize, double[] dWeights) {

		double[] neighbor = new double[neighborSize];
		double[] index = createIndex(neighborSize);

		for (int i = 0; i < data.length; ++i) {
			int[] result = findNeighbors(data, i, neighbor);
			int beg = result[0];
			int localRef = result[1];
			double[] weights = loessWeight(index, localRef);
			for (int j = 0; j < neighborSize; ++j) {
				weights[j] *= dWeights[beg + j];
			}
			Pair<Double, Double> coeffs = weightedLinearRegression(neighbor, weights);
			data[i] = linearRegressionPrediction(coeffs, localRef);
		}
	}

	public static double[] loessWeight(double[] data, double ref) {

		int size = data.length;
		double[] weights = new double[size];

		assertCondition(ref >= data[0] && ref <= data[size - 1], "Reference point outside range");

		double max = maxValue(ref - data[0], data[size - 1] - ref);

		for (int i = 0; i < size; ++i) {
			double diff = Math.abs(ref - data[i]) / max;
			double wt = (1.0 - Math.pow(diff, 3));
			wt = Math.pow(wt, 3);
			weights[i] = wt;
		}

		return weights;

	}

	public static double[] lowPassFilter(double[] data, int windowSize) {

		List<Double> filteredData = new ArrayList<>();
		SizeBoundFloatStatsWindow window = new SizeBoundFloatStatsWindow(windowSize, false);
		for (double datum : data) {
			window.add(datum);
			if (window.isFull()) {
				filteredData.add(window.getMean());
			}
		}
		return fromListToDoubleArray(filteredData);
	}

	public static Pair<Double, Double> weightedLinearRegression(double[] data, double[] weights) {
		// index data
		double[][] table = indexArray(data);
		return weightedLinearRegression(table, weights);
	}

	public static Pair<Double, Double> weightedLinearRegression(double[][] table, double[] weights) {

		int count = table.length;
		double wtSum = sum(weights);

		double avX = 0;
		double avY = 0;
		for (int i = 0; i < count; ++i) {
			avX += weights[i] * table[i][0];
			avY += weights[i] * table[i][1];
		}
		avX /= wtSum;
		avY /= wtSum;

		double s1 = 0;
		double s2 = 0;
		for (int i = 0; i < count; ++i) {
			double diffX = table[i][0] - avX;
			double diffY = table[i][1] - avY;
			s1 += weights[i] * (diffX * diffY);
			s2 += weights[i] * (diffX * diffX);
		}
		double b1 = s1 / s2;
		double b0 = avY - b1 * avX;
		return new Pair<>(b1, b0);
	}

	/********************
	 * 
	 * BASIC FUNCTIONS
	 * 
	 */

	public static void assertCondition(Boolean condition, String message) {

		if (!condition)
			throw new IllegalStateException(message);

	}

	public static double[] fromListToDoubleArray(List<Double> valueList) {
		double[] values = new double[valueList.size()];
		for (int i = 0; i < valueList.size(); ++i) {
			values[i] = valueList.get(i);
		}
		return values;
	}

	public static double maxValue(double val1st, double val2nd) {
		return Math.max(val1st, val2nd);
	}

	public static double[] subtractVector(double[] a, double[] b) {
		RealVector va = new ArrayRealVector(a);
		RealVector vb = new ArrayRealVector(b);
		RealVector vc = va.subtract(vb);
		return vc.toArray();
	}

	public static double sum(double[] data) {
		double sum = 0;
		for (double d : data) {
			sum += d;
		}
		return sum;
	}

}
