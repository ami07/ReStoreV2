package org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.plan.DepthFirstMultiPlanWalker;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.VisitorException;

public class PhyPlanComparator extends PhyPlanVisitor {

	public PhyPlanComparator(PhysicalPlan plan,PhysicalPlan planToCompare) {
		super(plan, new DepthFirstMultiPlanWalker<PhysicalOperator, PhysicalPlan>(plan));
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public List<Operator> visitPhyOp(PhysicalOperator currentOP, List<Operator> otherPhOps) throws VisitorException{
		//check if the plans rooted by these nodes are equivalent
		for(Operator otherPhOp:otherPhOps){
			if(currentOP.isEquivalent((PhysicalOperator) otherPhOp)){
				List<Operator> otherPhOpList=new ArrayList<Operator>();
				otherPhOpList.add(otherPhOp);
				return otherPhOpList;
			}
		}
		
		return null;
	}

}
