package org.apache.pig.impl.plan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DepthFirstMultiPlanWalker<O extends Operator, P extends OperatorPlan<O>> extends PlanWalker<O,P> {

	public DepthFirstMultiPlanWalker(P plan) {
		super(plan);
	}

	@Override
	public PlanWalker<O, P> spawnChildWalker(P plan) {
        return new DepthFirstMultiPlanWalker<O, P>(plan);
    }
	
	@Override
	/**
     * Begin traversing the graph for the two plans.
     * @param visitor Visitor this walker is being used by.
     * @throws VisitorException if an error is encountered while walking.
     */
	public boolean walk(PlanVisitor<O,P> visitor, P mPlan2) throws VisitorException {
		List<O> rootsPlan1 = mPlan.getRoots();
		List<O> rootsPlan2 = mPlan2.getRoots();
        Set<O> seen = new HashSet<O>();

        return depthFirst(null, rootsPlan1, rootsPlan2, seen, mPlan2,visitor);
	}

	private boolean  depthFirst(O node, 
			Collection<O> successorsPlan1,
			Collection<O> successorsPlan2, 
			Set<O> seen, 
			P mPlan2,
			PlanVisitor<O, P> visitor) {
		
		if (successorsPlan1 == null && successorsPlan2 == null) return true;
		if (successorsPlan1 == null || successorsPlan2 == null) return false;
		
		List<O> potentialMatchesPlan2=new ArrayList<O>(successorsPlan2);
		for (O suc : successorsPlan1) {
            if (seen.add(suc)) {
        		//find an op in successorPlan2 that is equivalent to one of
            	//the suc
                List<O> equivalentOp=null;
				try {
					equivalentOp = (List<O>) suc.visit(visitor, potentialMatchesPlan2);
				} catch (VisitorException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
                if(equivalentOp==null){
                	//failure to find an equivalent operator in the other plan then return 
                	return false;
                }else{
                	//continue with the navigation
                	Collection<O> newSuccessorsPlan1 = mPlan.getSuccessors(suc);
                	Collection<O> newSuccessorsPlan2 = mPlan2.getSuccessors(equivalentOp.get(0));
                    if(!depthFirst(suc, newSuccessorsPlan1,newSuccessorsPlan2, seen, mPlan2, visitor)){
                    	return false;
                    }
                    
                	//remove this matched operator when looking at the list of potential matches with other ops
                    potentialMatchesPlan2.remove(equivalentOp.get(0));
                }
                
            }
        }
		//if we managed to go that far in the plan without any violation then we passed
		return true;
	}

	@Override
	public void walk(PlanVisitor<O, P> visitor) throws VisitorException {
		// TODO Auto-generated method stub
		
	}

}
