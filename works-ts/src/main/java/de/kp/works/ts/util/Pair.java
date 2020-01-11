package de.kp.works.ts.util;
/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
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

import java.io.Serializable;

public class Pair<L, R> implements Serializable {

	private static final long serialVersionUID = 8865668050641861419L;

	protected L left;
	protected R right;

	public Pair() {
	}

	/**
	 * @param left
	 * @param right
	 */
	public Pair(L left, R right) {
		this.left = left;
		this.right = right;
	}

	/**
	 * @param left
	 * @param right
	 */
	public void initialize(L left, R right) {
		this.left = left;
		this.right = right;
	}

	/**
	 * @return
	 */
	public L getLeft() {
		return left;
	}

	/**
	 * @param left
	 */
	public void setLeft(L left) {
		this.left = left;
	}

	/**
	 * @return
	 */
	public R getRight() {
		return right;
	}

	/**
	 * @param right
	 */
	public void setRight(R right) {
		this.right = right;
	}

}
