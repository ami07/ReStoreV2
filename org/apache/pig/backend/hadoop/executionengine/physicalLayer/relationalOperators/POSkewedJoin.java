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

package org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;

/**
 * The PhysicalOperator that represents a skewed join. It must have two inputs.
 * This operator does not do any actually work, it is only a place holder. When it is
 * translated into MR plan, a POSkewedJoin is translated into a sampling job and a join 
 * job.
 * 
 *
 */
public class POSkewedJoin extends PhysicalOperator  {

	
	private static final long serialVersionUID = 1L;
	private boolean[] mInnerFlags;
	
	// The schema is used only by the MRCompiler to support outer join
	transient private List<Schema> inputSchema = new ArrayList<Schema>();
	
	transient private static Log log = LogFactory.getLog(POSkewedJoin.class);
	
	// physical plans to retrive join keys
	// the key of this <code>MultiMap</code> is the PhysicalOperator that corresponds to an input
	// the value is a list of <code>PhysicalPlan</code> to retrieve each join key for this input
    private MultiMap<PhysicalOperator, PhysicalPlan> mJoinPlans;

    public POSkewedJoin(OperatorKey k)  {
        this(k,-1,null, null);
    }

    public POSkewedJoin(OperatorKey k, int rp) {
        this(k, rp, null, null);
    }

    public POSkewedJoin(OperatorKey k, List<PhysicalOperator> inp, boolean []flags) {
        this(k, -1, inp, flags);
    }

    public POSkewedJoin(OperatorKey k, int rp, List<PhysicalOperator> inp, boolean []flags) {
        super(k,rp,inp);
        if (flags != null) {
        	// copy the inner flags
        	mInnerFlags = new boolean[flags.length];
        	for (int i = 0; i < flags.length; i++) {
        		mInnerFlags[i] = flags[i];
        	}
        }
    }
    
    public boolean[] getInnerFlags() {
    	return mInnerFlags;
    }
    
    public MultiMap<PhysicalOperator, PhysicalPlan> getJoinPlans() {
    	return mJoinPlans;
    }
    
    public void setJoinPlans(MultiMap<PhysicalOperator, PhysicalPlan> joinPlans) {
        mJoinPlans = joinPlans;
    }    
    
	@Override
	public void visit(PhyPlanVisitor v) throws VisitorException {
		v.visitSkewedJoin(this);
	}

    @Override
    public String name() {
        return getAliasString() + "SkewedJoin["
                + DataType.findTypeName(resultType) + "]" + " - "
                + mKey.toString();
    }

	@Override
	public boolean supportsMultipleInputs() {		
		return true;
	}

	@Override
	public boolean supportsMultipleOutputs() {	
		return false;
	}
	
	public void addSchema(Schema s) {
		inputSchema.add(s);
	}
	
	public Schema getSchema(int i) {
		return inputSchema.get(i);
	}
	
	/**
	 * @author iman
	 */
	@Override
	public boolean isEquivalent(PhysicalOperator otherOP) {
		// TODO Auto-generated method stub
		if(otherOP instanceof POSkewedJoin){
			
			if(this.requestedParallelism==otherOP.getRequestedParallelism()){
				List<PhysicalOperator> otherOpInputOPsRaw=otherOP.getInputs();
				List<PhysicalOperator> otherOpInputOPs= new ArrayList<PhysicalOperator>(otherOpInputOPsRaw);
				
				for(int i=0;i<inputs.size();i++){
					PhysicalOperator input=inputs.get(i);
					boolean foundEqOp=false;
					for(PhysicalOperator otherOpr:otherOpInputOPs){
						if(input.isEquivalent(otherOpr)){
							//find an equivalent opr, now check the list of associated plans
							List<PhysicalPlan> currentPlans=new ArrayList<PhysicalPlan>(this.mJoinPlans.get(input));
							List<PhysicalPlan> otherPlans=new ArrayList<PhysicalPlan>(((POSkewedJoin) otherOP).mJoinPlans.get(otherOpr));
							if(isEquivalentListOfPlans(currentPlans,otherPlans)){
								//equiv ops and their list of plans
								//remove the found plan from the list of plans of the other op
								otherOpInputOPs.remove(otherOpr);
								//exit the current loop
								foundEqOp=true;
								break;
							}else{
								//we could  find an equivalent op, but with a diff list of expressions, then return false
								return false;
							}
							
							
						}//if found an equivalent operator
					}
					//we could not find an equivalent plan, then return false
					if(!foundEqOp){
						return false;
					}
				}
			}
		}
		return false;
	}

	/**
	 * @author iman
	 */
	private boolean isEquivalentListOfPlans(List<PhysicalPlan> currentPlans, List<PhysicalPlan> otherPlans){
		List<PhysicalPlan> otherOpInputPlans= new ArrayList<PhysicalPlan>(otherPlans);
		for(int i=0;i<currentPlans.size();i++){
			PhysicalPlan plan=currentPlans.get(i);
			//for every physical plan, check if there is an equivalent plan in otherOp plans
			boolean foundEqPlan=false;
			for(PhysicalPlan otherPlan:otherOpInputPlans){
				if(plan.isEquivalent(otherPlan)){
					//find an equivalent plan, now check the flattening condition
					
					//the two plans and their flattening cond are equivalent
					//remove the found plan from the list of plans of the other op
					otherOpInputPlans.remove(otherPlan);
					//exit the current loop
					foundEqPlan=true;
					break;
					
				}
			}
			//we could not find an equivalent plan, then return false
			if(!foundEqPlan){
				return false;
			}
		}
		
		return true;
	}
}
