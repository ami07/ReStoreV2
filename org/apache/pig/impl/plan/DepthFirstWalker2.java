package org.apache.pig.impl.plan;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.impl.util.Utils;

public class DepthFirstWalker2<O extends Operator, P extends OperatorPlan<O>> extends PlanWalker<O, P> {

	public DepthFirstWalker2(P plan) {
		super(plan);
	}

	@Override
	public PlanWalker<O, P> spawnChildWalker(P plan) {
		return new DepthFirstWalker2<O, P>(plan);
	}

	@Override
	public void walk(PlanVisitor<O, P> visitor) throws VisitorException {
		List<O> roots = mPlan.getRoots();
        Set<O> seen = new HashSet<O>();

        depthFirst(null, roots, seen, visitor);
		
	}

	private void depthFirst(O node, Collection<O> successors, Set<O> seen,
			PlanVisitor<O, P> visitor) throws VisitorException {
		if (successors == null) return;

        for (O suc : successors) {
            if (seen.add(suc)) {
                suc.visit(visitor, suc);
                Collection<O> newSuccessors = Utils.mergeCollection(mPlan.getSuccessors(suc), mPlan.getSoftLinkSuccessors(suc));
                depthFirst(suc, newSuccessors, seen, visitor);
            }
        }
	}

}
