package org.apache.pig.backend.hadoop.executionengine.physicalLayer.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.ExecType;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.optimizer.PlanOptimizer;
import org.apache.pig.impl.plan.optimizer.Rule;
import org.apache.pig.impl.plan.optimizer.RuleOperator;
import org.apache.pig.impl.plan.optimizer.RulePlan;

public class PhysicalOptimizer extends PlanOptimizer<PhysicalOperator, PhysicalPlan> {

	private static final String SCOPE = "RULE";
    private static NodeIdGenerator nodeIdGen = NodeIdGenerator.getGenerator();
    
	PigContext pc;
	
	protected PhysicalOptimizer(PhysicalPlan plan) {
		this(plan,ExecType.MAPREDUCE);
	}
	
	public PhysicalOptimizer(PhysicalPlan plan, PigContext pc) {
		super(plan);
		this.pc=pc;
		runOptimizations(plan,pc.getExecType());
	}
	
	protected PhysicalOptimizer(PhysicalPlan plan, ExecType mode) {
		super(plan);
		runOptimizations(plan,mode);
	}

	private void runOptimizations(PhysicalPlan plan, ExecType mode) {
		//rules for the physical optimizer
		//for now we handle only sharing optimization
		//which means that whenever we find a shared operator
		//we actually insert a split (similar to the one done on the logical layer level
		
		
        RulePlan rulePlan = new RulePlan();
        RuleOperator anyPhysicalOperator = new RuleOperator(PhysicalOperator.class, RuleOperator.NodeType.ANY_NODE, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(anyPhysicalOperator);
        mRules.add(new Rule<PhysicalOperator, PhysicalPlan>(rulePlan, new SharingOptimizerSplitInserter(plan,pc),"ImplicitSplitInserterForSharedOperators"));
	}

}
