package org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.plan.DepthFirstMultiPlanWalker;
import org.apache.pig.impl.plan.DepthFirstMultiPlanWalkerReplacer;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;

public class PhyPlanReplacer extends PhyPlanVisitor {

	public PhyPlanReplacer(PhysicalPlan plan,PhysicalPlan planToCompare) {
		super(plan, new DepthFirstMultiPlanWalkerReplacer<PhysicalOperator, PhysicalPlan>(plan));
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public Operator visitPhyOp(PhysicalOperator currentOP, List<Operator> otherPhOps, boolean returnLastMatch) throws VisitorException{
		//check if the plans rooted by these nodes are equivalent
		for(Operator otherPhOp:otherPhOps){
			if(currentOP.isEquivalent((PhysicalOperator)otherPhOp)){
				return otherPhOp;
			}
		}
		
		return null;
	}


}
