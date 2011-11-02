package org.apache.pig.backend.hadoop.executionengine.physicalLayer.optimizer;

import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.optimizer.OptimizerException;
import org.apache.pig.impl.plan.optimizer.Transformer;

public abstract class PhysicalTransformer extends Transformer<PhysicalOperator, PhysicalPlan> {

	protected PhysicalTransformer(PhysicalPlan plan,
			PlanWalker<PhysicalOperator, PhysicalPlan> walker) {
		super(plan);
	}

	

}
