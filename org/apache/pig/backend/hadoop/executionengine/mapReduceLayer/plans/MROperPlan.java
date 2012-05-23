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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Set;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;


/**
 * A Plan used to create the plan of 
 * Map Reduce Operators which can be 
 * converted into the Job Control
 * object. This is necessary to capture
 * the dependencies among jobs
 */
public class MROperPlan extends OperatorPlan<MapReduceOper> {

    private static final long serialVersionUID = 1L;

    public MROperPlan() {
        // TODO Auto-generated constructor stub
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        MRPrinter printer = new MRPrinter(ps, this);
        printer.setVerbose(true);
        try {
            printer.visit();
        } catch (VisitorException e) {
            // TODO Auto-generated catch block
            throw new RuntimeException("Unable to get String representation of plan:" + e );
        }
        return baos.toString();
    }

	public boolean contains(MapReduceOper mro) {
		if(mOps.containsKey(mro)|| mKeys.containsValue(mro)){
			return true;
		}
		return false;
	}
	
	public MROperPlan clone(){
		MROperPlan clonedMRP=new MROperPlan();
		
		MultiMap<MapReduceOper,MapReduceOper> cloningMap=new  MultiMap<MapReduceOper,MapReduceOper>();
		
		Set<MapReduceOper> ops = mOps.keySet();
		for(MapReduceOper op:ops){
			MapReduceOper clonedOp = null;
			try {
				clonedOp = op.clone();
				clonedMRP.add(clonedOp);
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
			if(clonedOp!=null){
				cloningMap.put(op, clonedOp);
			}
		}
		
		//update the mFromEdges & mToEdges
		Set<MapReduceOper> fromEdgeKeys = mFromEdges.keySet();
		for(MapReduceOper op1:fromEdgeKeys){
			Collection<MapReduceOper> op2List=mFromEdges.get(op1);
			for(MapReduceOper op2: op2List){
				MapReduceOper clonedOp1 = (MapReduceOper) cloningMap.get(op1).toArray()[0];
				MapReduceOper clonedOp2 = (MapReduceOper) cloningMap.get(op2).toArray()[0];
				
				if(clonedOp1!=null && clonedOp2!=null){
					clonedMRP.mFromEdges.put(clonedOp1, clonedOp2);
					clonedMRP.mToEdges.put(clonedOp2, clonedOp1);
				}
			}
			
		}
		
				
		//update the mSoftFromEdges & mSoftToEdges
		Set<MapReduceOper> fromSoftEdgeKeys = mSoftFromEdges.keySet();
		for(MapReduceOper op1:fromSoftEdgeKeys){
			Collection<MapReduceOper> op2List=mSoftFromEdges.get(op1);
			for(MapReduceOper op2: op2List){
				MapReduceOper clonedOp1 = (MapReduceOper) cloningMap.get(op1).toArray()[0];
				MapReduceOper clonedOp2 = (MapReduceOper) cloningMap.get(op2).toArray()[0];
				
				if(clonedOp1!=null && clonedOp2!=null){
					clonedMRP.mSoftFromEdges.put(clonedOp1, clonedOp2);
					clonedMRP.mSoftToEdges.put(clonedOp2, clonedOp1);
				}
			}
			
		}
		
		
		
		return clonedMRP;
	}

}
