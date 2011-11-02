/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.builtin;

import org.apache.pig.EvalFunc;

/**
 * LOG implements a binding to the Java function
 * {@link java.lang.Math#log(double) Math.log(double)}. 
 * Given a single data atom it returns the natural 
 * logarithm (base e)  of a double
 */
public class LOG extends DoubleBase{
	Double compute(Double input){
		return Math.log(input);
	}
	
	/**
     * @author iman
     */
	@Override
	public boolean isEquivalent(EvalFunc func) {
		if(func instanceof LOG){
			return true;
		}
		return false;
	}
}
