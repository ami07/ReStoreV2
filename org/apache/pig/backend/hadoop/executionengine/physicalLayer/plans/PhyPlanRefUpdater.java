package org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.DepthFirstWalker2;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;

public class PhyPlanRefUpdater extends PhyPlanVisitor {

	List<PhysicalOperator> oldOperators;
	POLoad newRefOperator;
	
	public PhyPlanRefUpdater(PhysicalPlan plan, List<PhysicalOperator> oldOperators, POLoad newRefOperator) {
		//super(plan, new DepthFirstWalker2<PhysicalOperator, PhysicalPlan>(plan));
		super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
		if(oldOperators!=null){
			this.oldOperators=new ArrayList<PhysicalOperator>(oldOperators);
		}
		this.newRefOperator=newRefOperator;
	}
	
	@Override
	public void visitUserFunc(POUserFunc userFunc) throws VisitorException {
		System.out.println("hit a POUserFunc operator "+ userFunc);
		PhysicalOperator userFuncRefOperator = userFunc.getReferencedOperator();
		if(userFuncRefOperator != null) {
			System.out.println("the POUserFunc operator ref operator  is not null "+ userFuncRefOperator);
			System.out.println("check if the referenced operator is any of the operators that got split into the other mapper");
			if(oldOperators.contains(userFuncRefOperator)){
				System.out.println("found an operator that is from those who were moved to the split mapper");
				System.out.println("replace that operator iwth the new load op");
				userFunc.setReferencedOperator(newRefOperator);
			}
		}
	}
	@Override
	public void visitPhyOp(PhysicalOperator currentOP, Operator currentOPReplica){
		System.out.println("current operator "+currentOP.getAlias()+"  "+currentOP.toString());
		if(currentOP instanceof POFilter){
			//check the inner plans for the pofilter op
			PhysicalPlan filterPlan = ((POFilter)currentOP).getPlan();
			System.out.println("hit a filter");
			List<PhysicalOperator> planLeaves = filterPlan.getLeaves();
			/*for(PhysicalOperator leaf:planLeaves){
				depthFirst(leaf);
			}*/
			depthFirst(null, filterPlan, planLeaves,new ArrayList<PhysicalOperator>());
		}else if(currentOP instanceof POForEach){
			System.out.println("hit a foreach");
			List<PhysicalPlan> foreachInputPlans = ((POForEach)currentOP).getInputPlans();
			for (PhysicalPlan plan : foreachInputPlans) {
				List<PhysicalOperator> planLeaves = plan.getLeaves();
				/*for(PhysicalOperator leaf:planLeaves){
					depthFirst(leaf);
				}*/
				depthFirst(null, plan, planLeaves,new ArrayList<PhysicalOperator>());
			}
		}
	}

	private void depthFirst(PhysicalOperator op, PhysicalPlan plan, List<PhysicalOperator> successors, List<PhysicalOperator> seen) {
		
		if (successors == null) return;
		for(PhysicalOperator succ:successors){
			if (seen.add(succ)) {
				//update the references of this operator if they are any of the oldOps
				System.out.println(succ.getAlias()+"  "+succ.toString()+" "+succ.getInputs());
				
				List<PhysicalOperator> newSuccessors=new ArrayList<PhysicalOperator>();
				List<PhysicalOperator> successorsToAdd=plan.getSuccessors(succ);
				if(successorsToAdd!=null){
					newSuccessors.addAll(successorsToAdd);
				}
				List<PhysicalOperator> softSuccessorsToAdd=plan.getSoftLinkSuccessors(succ);
				if(softSuccessorsToAdd!=null){
					newSuccessors.addAll(softSuccessorsToAdd);
				}
				
			}
		}
		
	}
	
	

}
