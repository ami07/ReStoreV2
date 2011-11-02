package org.apache.pig.backend.hadoop.executionengine.physicalLayer.optimizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.LogicalToPhysicalTranslatorException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.LOConst;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.optimizer.OptimizerException;

public class SharingOptimizerSplitInserter extends PhysicalTransformer {

	private final static Log LOG = LogFactory.getLog(SharingOptimizerSplitInserter.class);
	
	protected PigContext pc;
	
	protected SharingOptimizerSplitInserter(PhysicalPlan plan) {
		super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
	}


	public SharingOptimizerSplitInserter(PhysicalPlan plan, PigContext pc) {
		this(plan);
		this.pc=pc;
	}


	@Override
	public boolean check(List<PhysicalOperator> nodes)
			throws OptimizerException {
		//I do not really need this function as I already do this check in the 
		//SharingOptimizer
		
		// Look to see if this is a non-split node with two outputs.  If so
        // it matches.
        if((nodes == null) || (nodes.size() <= 0)) {
            int errCode = 2052;
            String msg = "Internal error. Cannot retrieve operator from null or empty list.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }
        try{
        	PhysicalOperator op=nodes.get(0);
        	List<PhysicalOperator> succs = mPlan.getSuccessors(op);
            if (succs == null || succs.size() < 2) return false;
            if (op instanceof POSplit) return false;
            if (op instanceof POStore) return false;
            //if at least two of the successors are equal then return false
            if(duplicateSuccExist(succs)) return false;
            return true;
        }catch (Exception e) {
            int errCode = 2048;
            String msg = "Error while performing checks to introduce split operators in physical plan.";
            throw new OptimizerException(msg, errCode, PigException.BUG, e);
        }
	}

	private boolean duplicateSuccExist(List<PhysicalOperator> succs) {
		LOG.info("all succs to be examined for duplicates "+succs);
		for(PhysicalOperator succ:succs){
			LOG.info("succ node to be examined is "+succ+" is found at "+succs.indexOf(succ) +" and at "+succs.lastIndexOf(succ));
			if(succs.indexOf(succ)!=succs.lastIndexOf(succ)&& succs.lastIndexOf(succ)!=-1){
				LOG.info("duplicate successors found for an op");
				return true;
			}
		}
		return false;
	}


	@Override
	public void transform(List<PhysicalOperator> nodes)
			throws OptimizerException {
		System.out.println("To make transformation for nodes "+nodes);
		if((nodes == null) || (nodes.size() <= 0)) {
            int errCode = 2052;
            String msg = "Internal error. Cannot retrieve operator from null or empty list.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }
		try{
			String scope = nodes.get(0).getOperatorKey().scope;
            NodeIdGenerator idGen = NodeIdGenerator.getGenerator();
            POSplit splitOp = new POSplit(new OperatorKey(scope, 
                    idGen.getNextNodeId(scope)), nodes.get(0).getRequestedParallelism(),new ArrayList<PhysicalOperator>());
            
            FileSpec splStrFile;
            try {
                splStrFile = new FileSpec(FileLocalizer.getTemporaryPath(null, pc).toString(),new FuncSpec(BinStorage.class.getName()));
            } catch (IOException e1) {
                byte errSrc = pc.getErrorSource();
                int errCode = 0;
                switch(errSrc) {
                case PigException.BUG:
                    errCode = 2016;
                    break;
                case PigException.REMOTE_ENVIRONMENT:
                    errCode = 6002;
                    break;
                case PigException.USER_ENVIRONMENT:
                    errCode = 4003;
                    break;
                }
                String msg = "Unable to obtain a temporary path." ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, errSrc, e1);

            }
            ((POSplit)splitOp).setSplitStore(splStrFile);
            
            mPlan.add(splitOp);
            
            //take care of the predecessors of split operator
            //actually the pred of the split is the nodes.get(0)
            /*try {
                mPlan.connect(nodes.get(0), splitOp);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }*/
            
            // Find all the successors and connect appropriately with split
            List<PhysicalOperator> succs=new ArrayList<PhysicalOperator>(mPlan.getSuccessors(nodes.get(0)));
            
            
            //I am following here the same procedure used in the ImplicitSplitInserter
            // For two successors of nodes.get(0) here is a pictorial
            // representation of the change required:
            // BEFORE:
            // Succ1  Succ2
            //  \       /
            //  nodes.get(0)
            
            //  SHOULD BECOME:
            
            // AFTER:
            // Succ1          Succ2
            //   |              |
            // Filter(True) Filter(True)
            //      \       /
            //        Split
            //          |
            //        nodes.get(0)
            
            // Here is how this will be accomplished.
            // First (the same) Split Operator will be "inserted between" nodes.get(0)
            // and all its successors. The "insertBetween" API is used which makes sure
            // the ordering of operators in the graph is preserved. So we get the following: 
            // Succ1        Succ2
            //    |          |
            //   Split     Split
            //      \      /  
            //      nodes.get(0)
            
            // Then all but the first connection between nodes.get(0) and the Split 
            // Operator are removed using "disconnect" - so we get the following:
            // Succ1          Succ2
            //      \       /
            //        Split
            //          |
            //        nodes.get(0)
            
            // Now a new SplitOutputOperator is "inserted between" the Split operator
            // and the successors. So we get:
            // Succ1          Succ2
            //   |              |
            // Filter(True) Filter(True)
            //      \       /
            //        Split
            //          |
            //        nodes.get(0)
            
            //print the node to be shared
        	LOG.info("node to be shared by adding the split op after it "+ nodes.get(0));
            for(PhysicalOperator succ:succs){
            	
            	//print all its successors
            	LOG.info("a succ node to the node to be shared: "+succ); 
            	mPlan.insertBetween(nodes.get(0), splitOp, succ);
            }
            
            for(int i = 1; i < succs.size(); i++) {
                mPlan.disconnect(nodes.get(0), splitOp); 
            }
            
            for(PhysicalOperator succ:succs){
            	 PhysicalOperator splitOutput = new POFilter(new OperatorKey(scope, idGen
                         .getNextNodeId(scope)), nodes.get(0).getRequestedParallelism());
            	 PhysicalPlan condPlan=new PhysicalPlan();
            	 ConstantExpression cnst = new ConstantExpression(new OperatorKey(scope,
                         idGen.getNextNodeId(scope)));
                 cnst.setValue(new Boolean(true));
                 cnst.setResultType(DataType.BOOLEAN);
                 condPlan.add(cnst);
            	 ((POFilter) splitOutput).setPlan(condPlan);
            	 //splitOp.addOutput(splitOutput);
                 mPlan.add(splitOutput);
                 mPlan.insertBetween(splitOp, splitOutput, succ);
                 /*try {
                     mPlan.connect(splitOp, splitOutput);
                 } catch (PlanException e) {
                     int errCode = 2015;
                     String msg = "Invalid physical operators in the physical plan" ;
                     throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
                 }*/
                 // Patch up the contained plans of succ
                 fixUpContainedPlans(nodes.get(0), splitOutput, succ, null);
            }
            
		}catch (Exception e) {
            int errCode = 2047;
            String msg = "Internal error. Unable to introduce split operators to the physical plan.";
            throw new OptimizerException(msg, errCode, PigException.BUG, e);
        }
	}
	
	private void fixUpContainedPlans(PhysicalOperator physicalOperator,
			PhysicalOperator splitOutput, PhysicalOperator succ, Object object) {
		// TODO Auto-generated method stub
		
	}


	public void setPigContext(PigContext pc) {
        this.pc = pc;
    }


	@Override
	public void reset() throws OptimizerException {
		//do nothing
	}

}
