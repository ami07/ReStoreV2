package org.apache.pig.impl.plan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;

public class DepthFirstMultiPlanWalkerReplacer <O extends Operator, P extends OperatorPlan<O>> extends PlanWalker<O,P>{

	List<POStore> planStores=new ArrayList<POStore>();
	
	public DepthFirstMultiPlanWalkerReplacer(P plan) {
		super(plan);
	}
	@Override
	public PlanWalker<O, P> spawnChildWalker(P plan) {
		return new DepthFirstMultiPlanWalkerReplacer<O, P>(plan);
	}

	@Override
	public void walk(PlanVisitor<O, P> visitor) throws VisitorException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public O walk(P mPlan2, PlanVisitor<O, P> visitor) throws VisitorException {
		List<O> rootsPlan1 = mPlan.getRoots();
		List<O> rootsPlan2 = mPlan2.getRoots();
        Set<O> seen = new HashSet<O>();

        try {
			planStores=PlanHelper.getStores((PhysicalPlan)mPlan2);
		} catch (VisitorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
        return depthFirst(null, rootsPlan1, rootsPlan2, seen, mPlan2,visitor, null);
	}
	
	
	private O depthFirst(O node,  Collection<O> newSuccessorsPlan12, Collection<O> newSuccessorsPlan22,
			Set<O> seen, P mPlan2, PlanVisitor<O, P> visitor, O lastMatch) {
		if (newSuccessorsPlan12 == null && newSuccessorsPlan22 == null) return lastMatch;
		if (newSuccessorsPlan12 == null || newSuccessorsPlan22 == null) return null;
		
		List<O> potentialMatchesPlan2=new ArrayList<O>(newSuccessorsPlan22);
		List<O> returnVals=new ArrayList<O>();
		
		Vector<O>newSuccessorsPlan12V=new Vector<O>(newSuccessorsPlan12);
		for ( int i=0;i<newSuccessorsPlan12V.size();i++) {
			O suc = newSuccessorsPlan12V.get(i);
            if (seen.add(suc)) {
        		//find an op in successorPlan2 that is equivalent to one of
            	//the suc
            	O equivalentOp=null;
            	try {
					equivalentOp = (O) suc.visit(visitor, potentialMatchesPlan2,true);
				} catch (VisitorException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				if(equivalentOp==null){
                	
					if(i<(newSuccessorsPlan12.size()-1)){
						//there are other siblings of this operator, so we might explore another branch of the plan
						continue;
					}else if(potentialMatchesPlan2.size()==1 && planStores.contains(potentialMatchesPlan2.get(0))){
						//failure to find an equivalent operator in the other plan then return last match of the following
						//conditions apply
						return lastMatch;
					}else{
						//conditions does not apply, return null
						return null;
					}
                	
                }else{
                	//continue with the navigation
                	Collection<O> newSuccessorsPlan1 = mPlan.getSuccessors(suc);
                	Collection<O> newSuccessorsPlan2 = mPlan2.getSuccessors(equivalentOp);
                	O retVal=depthFirst(suc, newSuccessorsPlan1,newSuccessorsPlan2, seen, mPlan2, visitor,suc);
                    if( retVal ==null){
                    	return null;
                    }else{
                    	if(!returnVals.contains(retVal)){
                    		returnVals.add(retVal);
                    	}
                    }
                    //remove this matched operator when looking at the list of potential matches with other ops
                    potentialMatchesPlan2.remove(equivalentOp);
                    if(potentialMatchesPlan2.isEmpty()){
                    	//no need to continue looking at other operators in the original plan
                    	break;
                    }
                }
				
				
            }
		}
		
		//if we managed to go that far in the plan without any violation then we passed
		if(returnVals.size()>1){
			return null;
		}else{
			return returnVals.get(0);
		}
	}

}
