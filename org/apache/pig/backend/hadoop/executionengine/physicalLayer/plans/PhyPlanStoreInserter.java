package org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.plan.PlanWalker;

public class PhyPlanStoreInserter extends PhyPlanVisitor {

	public PhyPlanStoreInserter(PhysicalPlan plan,
			PlanWalker<PhysicalOperator, PhysicalPlan> walker) {
		super(plan, walker);
		// TODO Auto-generated constructor stub
	}

}
