package org.apache.pig.impl.plan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;

/**
 * DepthLevelWalker traverses a plan in a depth first manner, each level at time.  
 */
public class DepthLevelWalker<O extends Operator, P extends OperatorPlan<O>> extends PlanWalker<O, P> {

	/**
     * @param plan Plan for this walker to traverse.
     */
	public DepthLevelWalker(P plan) {
		super(plan);
	}

	@Override
	public PlanWalker<O, P> spawnChildWalker(P plan) {
		return new DepthLevelWalker<O, P>(plan);
	}

	/**
     * Begin traversing the graph.
     * @param visitor Visitor this walker is being used by.
     * @throws VisitorException if an error is encountered while walking.
     */
	@Override
	public void walk(PlanVisitor<O, P> visitor) throws VisitorException {
		List<O> roots = mPlan.getRoots();
        Set<O> seen = new HashSet<O>();

        depthLevel(roots, seen, visitor);
	}

	private void depthLevel(List<O> successors,
            Set<O> seen,
            PlanVisitor<O, P> visitor) throws VisitorException {
		
		if (successors == null || successors.size()<=0) return;
		
		//keep a copy of the current list of operators to visit
		List<O> siblingSuccessors=new ArrayList<O>(successors);
		
		// a list of newSuccessors of the new level to visit next
		List<O> newSuccessors=new ArrayList<O>();
		
		/*for(O successor: successors){
			//update the list of new successors of the current list of successors
			List<O> sucSuccessors=mPlan.getSuccessors(successor);
			if(sucSuccessors!=null){
				newSuccessors.addAll(sucSuccessors);
			}
		}*/
		
		for (int i=0; i<successors.size();i++) {
			O successor=successors.get(i);
			
			if (seen.add(successor)) {
				
				
				//remove the current op from the sibling ops and visit the
				siblingSuccessors.remove(successor);
				
				//visit this op with the list of siblings
				List<O> matchedSuccs = successor.visit(visitor, siblingSuccessors);
				if(matchedSuccs!=null && matchedSuccs.size()>0){
					//add the successors of successor
					List<O> sucSuccessors=mPlan.getSuccessors(successor);
					if(sucSuccessors!=null){
						newSuccessors.addAll(sucSuccessors);
					}
					/*for(O msuc: matchedSuccs){
						sucSuccessors=mPlan.getSuccessors(msuc);
						if(sucSuccessors!=null){
							for(O msucSuccessor: sucSuccessors){
								if(!newSuccessors.contains(msucSuccessor)){
									newSuccessors.add(msucSuccessor);
								}
							}
						}
					}*/
				}
			}
		}
		
		//proceed to the next level
		depthLevel(newSuccessors,seen,visitor);
		
	}

}
