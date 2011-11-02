package org.apache.pig.backend.hadoop.executionengine.physicalLayer;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.DepthLevelWalker;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.ReverseDependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;

public class QuerySharingOptimizer extends PhyPlanVisitor {

	public QuerySharingOptimizer(PhysicalPlan plan) {
		//TODO check if I need this reversedependencywalker!!!!
		super(plan, new DepthLevelWalker<PhysicalOperator, PhysicalPlan>(plan));
		// TODO Auto-generated constructor stub
		List<PhysicalOperator> roots = plan.getRoots();
	}

	@Override
    /*public void visitMROps(List<MapReduceOper> mrOps) throws VisitorException{
		// find the common operators in the list of ops
		
		int opIndex=0;
		while(opIndex<mrOps.size()){
			//check the current op with every other op with a higher index in mrOps
			MapReduceOper currentOP= mrOps.get(opIndex);
			int otherOpIndex=opIndex+1;
			while(opIndex<mrOps.size()){
				//get the second op
				MapReduceOper otherOP=mrOps.get(otherOpIndex);
				
				//check the two opers together
				if(currentOP.isEquivalent(otherOP)){
					//update list of mrOps to remove the otherOp
					//update the list of successors of currentOp to point to the successors of otherOp
					//updste the successors of otherOp to point to currentOp
					try {
						mPlan.replace(otherOP, currentOP);
					} catch (PlanException e) {
						int errCode = 2029;
		                String msg = "Error rewriting a sharing opportunity.";
		                throw new VisitorException(msg, errCode, PigException.BUG, e);
					}
					
				}
				
				//update the index of the operator in the inner loop
				otherOpIndex++;
			}
			
			//update the index for opIndex
			opIndex++;
		}
	}*/
	
	public List<Operator> visitPhyOp(PhysicalOperator currentOP, List<Operator> phOps) throws VisitorException{
		 List<Operator> opsToShare=new  ArrayList<Operator>();
		// find the common operators in the list of ops
		for (Operator otherOP : phOps) {
			//check the two opers together
			if(currentOP.isEquivalent((PhysicalOperator)otherOP)){
				//update list of mrOps to remove the otherOp
				//update the list of successors of currentOp to point to the successors of otherOp
				//update the successors of otherOp to point to currentOp
				try {
					mPlan.replaceWithExisting((PhysicalOperator)otherOP, currentOP);
					if(!opsToShare.contains(currentOP)){
						opsToShare.add(currentOP);
					}
				} catch (PlanException e) {
					int errCode = 2029;
	                String msg = "Error rewriting a sharing opportunity.";
	                throw new VisitorException(msg, errCode, PigException.BUG, e);
				}
				
			}
			
		}
		return opsToShare;
	}
}
