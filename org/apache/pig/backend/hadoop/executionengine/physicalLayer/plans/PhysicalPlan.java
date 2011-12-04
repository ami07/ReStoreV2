/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.LogicalToPhysicalTranslatorException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.BinaryExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POBinCond;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPreCombinerLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.UnaryComparisonOperator;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.InterStorage;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Pair;

/**
 * 
 * The base class for all types of physical plans. 
 * This extends the Operator Plan.
 *
 */
public class PhysicalPlan extends OperatorPlan<PhysicalOperator> implements Cloneable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

	private static final String SHARED_FILE = "SharedMROutput";

	private static final String TEMP_FILE = "tempOutput";
	
	//@iman
	public static final String DISCOVER_NEWPLANS_HEURISTICS = "sharing.useHeuristics.discoverPlans";
    
    // marker to indicate whether all input for this plan
    // has been sent - this is currently only used in POStream
    // to know if all map() calls and reduce() calls are finished
    // and that there is no more input expected.
    public boolean endOfAllInput = false;

	private static int tmpFileIter=0;

    

    public PhysicalPlan() {
        super();
    }
    
    public void attachInput(Tuple t){
        List<PhysicalOperator> roots = getRoots();
        for (PhysicalOperator operator : roots) {
            operator.attachInput(t);
		}
    }
    
    public void detachInput(){
        for(PhysicalOperator op : getRoots())
            op.detachInput();
    }
    /**
     * Write a visual representation of the Physical Plan
     * into the given output stream
     * @param out : OutputStream to which the visual representation is written
     */
    public void explain(OutputStream out) {
        explain(out, true);
    }

    /**
     * Write a visual representation of the Physical Plan
     * into the given output stream
     * @param out : OutputStream to which the visual representation is written
     * @param verbose : Amount of information to print
     */
    public void explain(OutputStream out, boolean verbose){
        PlanPrinter<PhysicalOperator, PhysicalPlan> mpp = new PlanPrinter<PhysicalOperator, PhysicalPlan>(
                this);
        mpp.setVerbose(verbose);

        try {
            mpp.print(out);
        } catch (VisitorException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Write a visual representation of the Physical Plan
     * into the given printstream
     * @param ps : PrintStream to which the visual representation is written
     * @param format : Format to print in
     * @param verbose : Amount of information to print
     */
    public void explain(PrintStream ps, String format, boolean verbose) {
        ps.println("#-----------------------------------------------");
        ps.println("# Physical Plan:");
        ps.println("#-----------------------------------------------");

        if (format.equals("text")) {
            explain((OutputStream)ps, verbose);
            ps.println("");
        } else if (format.equals("dot")) {
            DotPOPrinter pp = new DotPOPrinter(this, ps);
            pp.setVerbose(verbose);
            pp.dump();
        }
        ps.println("");
  }
    
    @Override
    public void connect(PhysicalOperator from, PhysicalOperator to)
            throws PlanException {
        
        super.connect(from, to);
        to.setInputs(getPredecessors(to));
    }
    
    /*public void connect(List<PhysicalOperator> from, PhysicalOperator to) throws IOException{
        if(!to.supportsMultipleInputs()){
            throw new IOException("Invalid Operation on " + to.name() + ". It doesn't support multiple inputs.");
        }
        
    }*/
    
    @Override
    public void remove(PhysicalOperator op) {
        op.setInputs(null);
        List<PhysicalOperator> sucs = getSuccessors(op);
        if(sucs!=null && sucs.size()!=0){
            for (PhysicalOperator suc : sucs) {
                // successor could have multiple inputs
                // for example = POUnion - remove op from
                // its list of inputs - if after removal
                // there are no other inputs, set successor's
                // inputs to null
                List<PhysicalOperator> succInputs = suc.getInputs();
                succInputs.remove(op);
                if(succInputs.size() == 0)
                    suc.setInputs(null);
                else
                    suc.setInputs(succInputs);
            }
        }
        super.remove(op);
    }
    
    /* (non-Javadoc)
     * @see org.apache.pig.impl.plan.OperatorPlan#replace(org.apache.pig.impl.plan.Operator, org.apache.pig.impl.plan.Operator)
     */
    @Override
    public void replace(PhysicalOperator oldNode, PhysicalOperator newNode)
            throws PlanException {
        List<PhysicalOperator> oldNodeSuccessors = getSuccessors(oldNode);
        super.replace(oldNode, newNode);
        if(oldNodeSuccessors != null) {
            for (PhysicalOperator preds : oldNodeSuccessors) {
                List<PhysicalOperator> inputs = preds.getInputs();
                // now replace oldNode with newNode in
                // the input list of oldNode's successors
                for(int i = 0; i < inputs.size(); i++) {
                    if(inputs.get(i) == oldNode) {
                        inputs.set(i, newNode);
                    }
                }    
            }
        }
        
    }

    /**
	 * @author iman
	 */
    @Override
    public void replace(PhysicalOperator oldNode, List<PhysicalOperator> newNodes) throws PlanException {
    	List<PhysicalOperator> oldNodeSuccessors = getSuccessors(oldNode);
    	super.replace(oldNode, newNodes);
    	
    	if(oldNodeSuccessors != null) {
            for (PhysicalOperator preds : oldNodeSuccessors) {
                List<PhysicalOperator> inputs = preds.getInputs();
                // now replace oldNode with newNode in
                // the input list of oldNode's successors
                if(inputs.remove(oldNode)){
	                //add the new succs as input
	                for(PhysicalOperator newNode:newNodes){
	                	inputs.add(newNode);
	                }
                }
                //for(int i = 0; i < inputs.size(); i++) {
                //    if(inputs.get(i) == oldNode) {
                //        //inputs.set(i, newNode);
                //    	inputs.remove(i);
                //    }
                //}    
            }
        }
	}
	
    /**
	 * @author iman
	 */
    @Override
    public void replaceWithExisting(PhysicalOperator oldNode, PhysicalOperator newNode)
            throws PlanException {
        List<PhysicalOperator> oldNodeSuccessors = getSuccessors(oldNode);
        super.replaceWithExisting(oldNode, newNode);
        if(oldNodeSuccessors != null) {
            for (PhysicalOperator preds : oldNodeSuccessors) {
                List<PhysicalOperator> inputs = preds.getInputs();
                // now replace oldNode with newNode in
                // the input list of oldNode's successors
                for(int i = 0; i < inputs.size(); i++) {
                    if(inputs.get(i) == oldNode) {
                    	if(!inputs.contains(newNode)){
                    		inputs.set(i, newNode);
                    	}
                    }
                }
                inputs.remove(oldNode);
            }
        }
        
    }
    
    /* (non-Javadoc)
     * @see org.apache.pig.impl.plan.OperatorPlan#add(org.apache.pig.impl.plan.Operator)
    @Override
    public void add(PhysicalOperator op) {
        // attach this plan as the plan the operator is part of
        //op.setParentPlan(this);
        super.add(op);
    }
*/

    public boolean isEmpty() {
        return (mOps.size() == 0);
    }

    @Override
    public String toString() {
        if(isEmpty())
            return "Empty Plan!";
        else{
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            explain(baos, true);
            return baos.toString();
        }
    }

    @Override
    public PhysicalPlan clone() throws CloneNotSupportedException {
        PhysicalPlan clone = new PhysicalPlan();

        // Get all the nodes in this plan, and clone them.  As we make
        // clones, create a map between clone and original.  Then walk the
        // connections in this plan and create equivalent connections in the
        // clone.
        Map<PhysicalOperator, PhysicalOperator> matches = 
            new HashMap<PhysicalOperator, PhysicalOperator>(mOps.size());
        for (PhysicalOperator op : mOps.keySet()) {
            PhysicalOperator c = op.clone();
            clone.add(c);
            matches.put(op, c);
        }

        // Build the edges
        for (PhysicalOperator op : mFromEdges.keySet()) {
            PhysicalOperator cloneFrom = matches.get(op);
            if (cloneFrom == null) {
                String msg = "Unable to find clone for op " + op.name();
                throw new CloneNotSupportedException(msg);
            }
            Collection<PhysicalOperator> toOps = mFromEdges.get(op);
            for (PhysicalOperator toOp : toOps) {
                PhysicalOperator cloneTo = matches.get(toOp);
                if (cloneTo == null) {
                    String msg = "Unable to find clone for op " + toOp.name();
                    throw new CloneNotSupportedException(msg);
                }
                try {
                    clone.connect(cloneFrom, cloneTo);
                } catch (PlanException pe) {
                    CloneNotSupportedException cnse = new CloneNotSupportedException();
                    cnse.initCause(pe);
                    throw cnse;
                }
            }
        }

        // Fix up all the inputs in the operators themselves.
        for (PhysicalOperator op : mOps.keySet()) {
            List<PhysicalOperator> inputs = op.getInputs();
            if (inputs == null || inputs.size() == 0) continue;
            List<PhysicalOperator> newInputs = 
                new ArrayList<PhysicalOperator>(inputs.size());
            PhysicalOperator cloneOp = matches.get(op);
            if (cloneOp == null) {
                String msg = "Unable to find clone for op " + op.name();
                throw new CloneNotSupportedException(msg);
            }
            for (PhysicalOperator iOp : inputs) {
            	if(mOps.containsKey(iOp)){//@iman to fix the bug!
	                PhysicalOperator cloneIOp = matches.get(iOp);
	                if (cloneIOp == null) {
	                    String msg = "Unable to find clone for op " + iOp.name();
	                    throw new CloneNotSupportedException(msg);
	                }
	                newInputs.add(cloneIOp);
            	}else{
            		//@iman to fix the bug of an input that is not in the mOps.keyset
            		PhysicalOperator cloneIOp = iOp.clone();
            		newInputs.add(cloneIOp);
            	}
            }
            cloneOp.setInputs(newInputs);
        }
        
        for (PhysicalOperator op : mOps.keySet()) {
            if (op instanceof UnaryComparisonOperator) {
                UnaryComparisonOperator orig = (UnaryComparisonOperator)op;
                UnaryComparisonOperator cloneOp = (UnaryComparisonOperator)matches.get(op);
                cloneOp.setExpr((ExpressionOperator)matches.get(orig.getExpr()));
                cloneOp.setOperandType(orig.getOperandType());
            } else if (op instanceof BinaryExpressionOperator) {
                BinaryExpressionOperator orig = (BinaryExpressionOperator)op;
                BinaryExpressionOperator cloneOp = (BinaryExpressionOperator)matches.get(op);
                cloneOp.setRhs((ExpressionOperator)matches.get(orig.getRhs()));
                cloneOp.setLhs((ExpressionOperator)matches.get(orig.getLhs()));
            } else if (op instanceof POBinCond) {
                POBinCond orig = (POBinCond)op;
                POBinCond cloneOp = (POBinCond)matches.get(op);
                cloneOp.setRhs((ExpressionOperator)matches.get(orig.getRhs()));
                cloneOp.setLhs((ExpressionOperator)matches.get(orig.getLhs()));
                cloneOp.setCond((ExpressionOperator)matches.get(orig.getCond()));
            }
        }

        return clone;
    }
    
    
    /**
	 * @author iman
	 */
	public boolean isEquivalent(PhysicalPlan otherPlan) {
		if(otherPlan instanceof PhysicalPlan){
			//the other operator is also an PhysicalPlan then there is a possibility of equivalence
			//PlanWalker<PhysicalOperator, PhysicalPlan> planWalker ;
			boolean equivalentPlans=false;
			PhyPlanComparator planComparator = new PhyPlanComparator(this,otherPlan);
			try {
				return planComparator.visit(otherPlan);
			} catch (VisitorException e) {
				//TODO add the proper catch code
				e.printStackTrace();
			}
			if(equivalentPlans ){
				return true;
			}
		}
		return false;
	}
	
	/**
	 * @author iman
	 */
	public PhysicalOperator getPlanRecplacedWithView(PhysicalPlan otherPlan) {
		if(otherPlan instanceof PhysicalPlan){
			PhyPlanReplacer planReplacer = new PhyPlanReplacer(this,otherPlan);
			try {
				PhysicalOperator matchedOperator = planReplacer.visit(otherPlan,true);
				if(matchedOperator!=null){
					System.out.println("Last operator before plans got matched is "+matchedOperator);
					return matchedOperator;
				}else{
					System.out.println("Could not match the plans");
					return null;
				}   
			} catch (VisitorException e) {
				//TODO add the proper catch code
				e.printStackTrace();
			}
		}
		return null;
	}

	/**
	 * @author iman
	 */
	public void replaceOperatorWithLoad(PhysicalOperator oldOperator,
			POStore poStore,PigContext pigContext) throws PlanException {
		//create a new load from the store file
		String scope = poStore.getOperatorKey().scope;
		NodeIdGenerator nodeGen = NodeIdGenerator.getGenerator();
		
		FileSpec lFile=poStore.getInputSpec();
		POLoad load = new POLoad(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), lFile);
        load.setLFile(poStore.getSFile());
        load.setPc(pigContext);
        load.setResultType(poStore.getResultType());
        
        
        //replace the plan rooted with oldOperator with the new load
        //FIRST: remove the inputs of the oldOperator and remove all the predecessor nodes of the oldOperator
        //oldOperator.setInputs(null);
        //remove the predecessor nodes
        List<PhysicalOperator> oldNodePredecessors = getPredecessors(oldOperator);
        while(oldNodePredecessors!=null && !oldNodePredecessors.isEmpty()){
        	PhysicalOperator oldPred=oldNodePredecessors.get(0);
        	//remove predecessor from plan
        	remove(oldPred);
        }
        
        String aliasOfLastOpToShare=oldOperator.getAlias();
		if(aliasOfLastOpToShare!=null){
			load.setAlias(aliasOfLastOpToShare);
		}
		
        //SECOND: replace the operators
        replace(oldOperator,load);
	}

	/**
	 * @author iman
	 */
	public void replaceLoadWithOperatorsFromPlan(POLoad load,
			POStore replacablePlanStore, PhysicalPlan replaceablePlanPart) throws PlanException {
		//replace the load with the plan rooted by the store
		
		//get the successors of the load operator
		List<PhysicalOperator> succs = this.getSuccessors(load);
		
		//remove the load op
		//remove(load);
		
		//get the predecessors of the store operator
		List<PhysicalOperator> preds = replaceablePlanPart.getPredecessors(replacablePlanStore);
		
		
		//replace load with the preds
		if(preds==null || preds.size()<=0){
			return;
		}else if(preds.size()==1){
			//replace load with pred
			replace(load,preds.get(0));
		}else{
			//multiple preds, then check if the succ accept that or not
			//if it does not then error
			for(PhysicalOperator succ:succs){
				if(!succ.supportsMultipleInputs()){
					return;
				}
			}
			//now I am sure that all succ support multiple input then replace the load with all the preds
			replace(load,preds);
		}
		//for every succ add the preds and connect them
		for(PhysicalOperator succ:preds){
			List<PhysicalOperator> predspreds = replaceablePlanPart.getPredecessors(succ);
			connectOperators(succ,predspreds,replaceablePlanPart);
		}
	}

	
	/**
	 * @author iman
	 */
	private void connectOperators(PhysicalOperator succ,
			List<PhysicalOperator> preds, PhysicalPlan replacablePlanPart) throws PlanException {
		if(succ==null || preds==null || preds.isEmpty()){
			return;
		}
		//add preds as predecessors to succ
		for(PhysicalOperator pred:preds){
			this.add(pred);
			this.connect(pred,succ);
			
			//get the predecessors of this pred and add them to the plan
			List<PhysicalOperator> predspreds = replacablePlanPart.getPredecessors(pred);
			
			//remove pred from replacablePlanPart
			//replacablePlanPart.remove(pred);
			
			//recursively call for adding preds
			connectOperators(pred,predspreds,replacablePlanPart);
		}
	}

	public Vector<PhysicalPlan> discoverUsefulSubplans(PigContext pc,List<POStore> stores) throws VisitorException, CloneNotSupportedException, PlanException {
		Vector<PhysicalPlan> discoveredPlans=new Vector<PhysicalPlan>();
		
		//find loads of this plan
		List<POLoad> planLoads = PlanHelper.getLoads(this);
		
		if(planLoads==null || planLoads.isEmpty()){
			//no loads in this plan
			return discoveredPlans;
		}
		
		//for every found load, copy the plan until a filter, or foreach is reached
		for(POLoad load:planLoads){
			//get the successors of the load
			List<PhysicalOperator> loadSucc=this.getSuccessors(load);
			//for every successor, check to find a filter or for each
			for(PhysicalOperator succ:loadSucc){
				//check if the succ is a filter or foreach
				if(succ instanceof POFilter || succ instanceof POForEach){
					//this is the subplan that we are looking for
					
					//STEP2:add a split operator and a store after this filter/foreach op
					PhysicalOperator newStore=addSplitAfterOp(succ, pc,stores);
					
					//STEP1:clone the plan to this point
					PhysicalPlan newPlan=createPlanFromLoadAndOp(load.clone(),succ.clone(),newStore.clone());
					
					//STEP3:add the cloned plan to the list of discovered plans
					discoveredPlans.add(newPlan);
				}
			}
		}
		
		return discoveredPlans;
	}

	private PhysicalOperator addSplitAfterOp(PhysicalOperator lastOpToShare,PigContext pc,List<POStore> stores) throws PlanException, VisitorException, CloneNotSupportedException {
		
		String scope = lastOpToShare.getOperatorKey().scope;
		
		//create the split operator and add it to this plan
		//String scope = lastOpToShare.getOperatorKey().scope;
        NodeIdGenerator idGen = NodeIdGenerator.getGenerator();
        POSplit splitOp = new POSplit(new OperatorKey(scope, 
                idGen.getNextNodeId(scope)), lastOpToShare.getRequestedParallelism(),new ArrayList<PhysicalOperator>());
        
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
            throw new PlanException(msg, errCode, errSrc, e1);

        }
        ((POSplit)splitOp).setSplitStore(splStrFile);
        //set the inputs of the split op to be the lastOPToShare
        List<PhysicalOperator> inputsToSplit=new ArrayList<PhysicalOperator>();
        inputsToSplit.add(lastOpToShare);
        splitOp.setInputs(inputsToSplit);
        //add split to plan
        this.add(splitOp);

        
        
      //create store operator and add it to a new plan
		
		NodeIdGenerator nodeGen = NodeIdGenerator.getGenerator();
		//List<POStore> stores = PlanHelper.getStores(this);
		FuncSpec funcSpec=new FuncSpec(PigStorage.class.getName() + "()");
		if(stores!=null && !stores.isEmpty()){
			for(POStore astore:stores){
				funcSpec=astore.getSFile().getFuncSpec();
				break;
			}
		}
		
		
		POStore store = new POStore(new OperatorKey(scope, nodeGen.getNextNodeId(scope)));
		store.setAlias(lastOpToShare.getAlias());
		store.setSFile(new FileSpec(SHARED_FILE+System.currentTimeMillis(), funcSpec));
		if(stores!=null && !stores.isEmpty()){
			store.setInputSpec(stores.get(0).getInputSpec());
		}
        //store.setSignature(loStore.getSignature());
        //store.setSortInfo(loStore.getSortInfo());
        store.setIsTmpStore(false);
        PhysicalPlan storePlan=new PhysicalPlan();
        storePlan.add(store);
        //add the store plan as split plan
        splitOp.addPlan(storePlan);
        
        //now divide this plan to plan until the lastOpToshare and splitPlan
        //get a clone of this plan
        //PhysicalPlan splitPlan=this.clone();
        PhysicalPlan splitPlan=splitPlanAfterOperator(lastOpToShare);
        
        //add the splitPlan as a plan of the splitOP
        splitOp.addPlan(splitPlan);
        
        //add the splitOp as a successor of the lastOpToShare
        connect(lastOpToShare,splitOp);
        /*
        //get the successors of the last operator in the subplan
        List<PhysicalOperator> succs=new ArrayList<PhysicalOperator>(this.getSuccessors(lastOpToShare));
        for(PhysicalOperator succ:succs){
        	insertBetween(lastOpToShare, splitOp, succ);
        }
        //add split after the lastOpToShare
        //connect(lastOpToShare,splitOp);
        //List<PhysicalOperator> splitInputs = new ArrayList<PhysicalOperator>(1);
        //splitInputs.add(lastOpToShare);
        //store.setInputs(splitInputs);
        
        //add store after the splitOp
        connect(splitOp,store);
        List<PhysicalOperator> storeInputs = new ArrayList<PhysicalOperator>(1);
        storeInputs.add(splitOp);
        store.setInputs(storeInputs);
        
        //add the split operator between the lastOpToShare and the new store
        //insertBetween(lastOpToShare, splitOp, store);
        
        //now remove the extra edges from the lastOpToShare and the split operator
        for(int i = 1; i < succs.size(); i++) {
            disconnect(lastOpToShare, splitOp); 
        }
        
        //now add a filter between the splitOp and every successor
        for(PhysicalOperator succ:succs){
       	 PhysicalOperator splitOutput = new POFilter(new OperatorKey(scope, idGen
                    .getNextNodeId(scope)), lastOpToShare.getRequestedParallelism());
       	 PhysicalPlan condPlan=new PhysicalPlan();
       	 ConstantExpression cnst = new ConstantExpression(new OperatorKey(scope,
                    idGen.getNextNodeId(scope)));
            cnst.setValue(new Boolean(true));
            cnst.setResultType(DataType.BOOLEAN);
            condPlan.add(cnst);
       	 ((POFilter) splitOutput).setPlan(condPlan);
       	 //splitOp.addOutput(splitOutput);
         add(splitOutput);
         insertBetween(splitOp, splitOutput, succ);
            
       }
       //add a filter before the store
        PhysicalOperator splitOutput = new POFilter(new OperatorKey(scope, idGen
                .getNextNodeId(scope)), lastOpToShare.getRequestedParallelism());
   	 	PhysicalPlan condPlan=new PhysicalPlan();
   	 	ConstantExpression cnst = new ConstantExpression(new OperatorKey(scope,
                idGen.getNextNodeId(scope)));
        cnst.setValue(new Boolean(true));
        cnst.setResultType(DataType.BOOLEAN);
        condPlan.add(cnst);
   	 	((POFilter) splitOutput).setPlan(condPlan);
   	 	//splitOp.addOutput(splitOutput);
   	 	add(splitOutput);
   	 	insertBetween(splitOp, splitOutput, store);
        */
        return store;
        
	}

	private PhysicalPlan splitPlanAfterOperator(PhysicalOperator lastOpToShare) throws PlanException {
		PhysicalPlan newPlan=new PhysicalPlan();
		
		//get successors of the LastOPToShare
		List<PhysicalOperator> succs=getSuccessors(lastOpToShare);
		//move every succ and its succs into the new plan
		if(succs!=null){
			List<PhysicalOperator> succsCopy=new ArrayList<PhysicalOperator>(succs);
			for(PhysicalOperator succ:succsCopy){
				//move operator and its successors to plan
				moveOperatorAndSuccToPlan(succ,newPlan);
				//remove connection of succ with lastOpToShare from this plan
				disconnect(lastOpToShare,succ);
			}
		}
		
		return newPlan;
	}

	private void moveOperatorAndSuccToPlan(PhysicalOperator operator, PhysicalPlan newPlan) throws PlanException {
		//add operator to new plan
		newPlan.add(operator);
		//get successors of the operator
		List<PhysicalOperator> succs=getSuccessors(operator);
		//move every succ and its succs into the new plan and then copy their connections as well
		if(succs!=null){
			List<PhysicalOperator> succsCopy=new ArrayList<PhysicalOperator>(succs);
			for(PhysicalOperator succ:succsCopy){
				//move operator and its successors to plan
				moveOperatorAndSuccToPlan(succ,newPlan);
				//move connections between op and succ to new plan
				newPlan.connect(operator, succ);
			}
		}
		//remove operator from this plan
		this.remove(operator);
	}

	private PhysicalPlan createPlanFromLoadAndOp(PhysicalOperator load, PhysicalOperator succ, PhysicalOperator store) throws PlanException {
		PhysicalPlan newPlan=new PhysicalPlan();
		//add nodes to this plan
		newPlan.add(load);
		newPlan.add(succ);
		newPlan.add(store);
		
		//create a store and add it to the newPlan
		/*String scope = load.getOperatorKey().scope;
		NodeIdGenerator nodeGen = NodeIdGenerator.getGenerator();
		
		POStore store = new POStore(new OperatorKey(scope, nodeGen.getNextNodeId(scope)));
		store.setAlias(succ.getAlias());
		store.setSFile(new FileSpec(SHARED_FILE+System.currentTimeMillis(), new FuncSpec(PigStorage.class.getName() + "()")));
        //store.setInputSpec(loStore.getInputSpec());
        //store.setSignature(loStore.getSignature());
        //store.setSortInfo(loStore.getSortInfo());
        store.setIsTmpStore(false);
        newPlan.add(store);*/
        
		
		//add edges to this plan
        newPlan.connect(load, succ);
        newPlan.connect(succ, store);
        
		//update inputs of plan operators
        List<PhysicalOperator> succInputs = new ArrayList<PhysicalOperator>(1);
        succInputs.add(load);
        succ.setInputs(succInputs);
        List<PhysicalOperator> storeInputs = new ArrayList<PhysicalOperator>(1);
        storeInputs.add(succ);
        store.setInputs(storeInputs);
        
        
		return newPlan;
	}

	public Vector<PhysicalPlan> discoverUsefulSubplans(PigContext pigContext, Configuration conf,List<POStore> stores, List <Pair<PhysicalPlan, PhysicalPlan>> newMapperRootPlans) throws PlanException, VisitorException, CloneNotSupportedException {
		
		boolean isUseDiscovePlanHeuristics=conf.getBoolean(DISCOVER_NEWPLANS_HEURISTICS, false);
		
		Vector<PhysicalPlan> discoveredPlans=new Vector<PhysicalPlan>();
		
		//find loads of this plan
		List<POLoad> planLoads = PlanHelper.getLoads(this);
		
		if(planLoads==null || planLoads.isEmpty()){
			//no loads in this plan
			return discoveredPlans;
		}
		
		//for every found load, copy the plan until a filter, or foreach is reached
		for(POLoad load:planLoads){
			//get the successors of the load
			List<PhysicalOperator> loadSucc=this.getSuccessors(load);
			if(loadSucc!= null && !loadSucc.isEmpty()){
				List<PhysicalOperator> loadSuccCopy=new ArrayList<PhysicalOperator>(loadSucc);
				//for every successor, check to find a filter or for each
				for(PhysicalOperator succ:loadSuccCopy){
					
					if(isUseDiscovePlanHeuristics){
						//check if the succ is a filter or foreach
						if(succ instanceof POFilter || succ instanceof POForEach){
							//this is the subplan that we are looking for
						//if(! (succ instanceof POLoad) && ! (succ instanceof POStore) && /*this.getPredecessors(succ).size()==1 &&*/ this.getSuccessors(succ).size() >=1 && !(succ instanceof POLocalRearrange)){	
							//STEP1:split the plan by adding a store after this filter/foreach op , then create two other plans
							List<PhysicalOperator> opSuccs = this.getSuccessors(succ); 
							
							PhysicalPlan sharedOperatorsPlan=null;
							if(opSuccs==null || opSuccs.size()==0){
								//sharedOperatorsPlan=duplicateLastMapperOp(succ, pigContext,stores);
							}else{
								 sharedOperatorsPlan=splitPlan(load, succ, pigContext,stores,planLoads, newMapperRootPlans);
							}
							//PhysicalPlan sharedOperatorsPlan=createPlan(load, succ, pigContext,stores,newMapperRootPlans);
							//STEP1:clone the plan to this point
							//PhysicalOperator newStore=null;
							//PhysicalPlan newPlan=createPlanFromLoadAndOp(load.clone(),succ.clone(),newStore.clone());
							
							//STEP2:add the cloned plan to the list of discovered plans
							//discoveredPlans.add(sharedOperatorsPlan.clone());
							if(sharedOperatorsPlan!=null){
								discoveredPlans.add(sharedOperatorsPlan);
							}
						}//end if instance of pofilter or poforeach
					}else{
						//allow inserting store after all operaors, except for the load and operators that have multiple predecessors (e.g. union)
						/*if((succ instanceof POLocalRearrange || succ instanceof POPreCombinerLocalRearrange)){
							//if succ is a localRearrange, move pointer to the following operator if any
							List<PhysicalOperator> succSuccs = this.getSuccessors(succ); 
							if(succSuccs!=null){
								succ=succSuccs.get(0);
							}
						}*/
						if(! (succ instanceof POLoad) && ! (succ instanceof POStore)  &&/*this.getPredecessors(succ).size()==1*//*!(succ instanceof POLocalRearrange) && !(succ instanceof POPreCombinerLocalRearrange) &&*/ ! hasStoreSuccessor(succ)){
							//STEP1:split the plan by adding a store after this filter/foreach op , then create two other plans
							PhysicalPlan sharedOperatorsPlan=null;
							List<PhysicalOperator> opSuccs = this.getSuccessors(succ); 
							if(opSuccs==null || opSuccs.size()==0){
								//sharedOperatorsPlan=duplicateLastMapperOp(succ, pigContext,stores,newMapperRootPlans);
							}else{
								sharedOperatorsPlan=splitPlan(load, succ, pigContext,stores, planLoads, newMapperRootPlans);
							}
							//STEP2:add the cloned plan to the list of discovered plans
							if(sharedOperatorsPlan!=null){
								discoveredPlans.add(sharedOperatorsPlan);
							}
						}
						
					}
				}//end for
			}//end if loadsucc is valid
		}
		return discoveredPlans;
	}

	private PhysicalPlan duplicateLastMapperOp(PhysicalOperator lastOpToShare,
			PigContext pigContext, List<POStore> stores, List<Pair<PhysicalPlan, PhysicalPlan>> newMapperRootPlans) throws CloneNotSupportedException, PlanException, VisitorException {
		
		//initialize a pair to include the two created plans
		//Pair<PhysicalPlan, PhysicalPlan> newMapperRootPlansPair = new Pair<PhysicalPlan,PhysicalPlan>(null, null);
		
		//create a clone of the current plan to be shared
		PhysicalPlan sharedOperatorsPlan=this.clone();
		
		//create a new store to copy the output of the mapper to it
		String scope = lastOpToShare.getOperatorKey().scope;
		NodeIdGenerator nodeGen = NodeIdGenerator.getGenerator();
		//List<POStore> stores = PlanHelper.getStores(this);
		/*FuncSpec funcSpec=new FuncSpec(PigStorage.class.getName() + "()");
		if(stores!=null && !stores.isEmpty()){
			for(POStore astore:stores){
				funcSpec=astore.getSFile().getFuncSpec();
				break;
			}
		}*/
		FuncSpec funcSpec=new FuncSpec(InterStorage.class.getName());
		POStore store = new POStore(new OperatorKey(scope, nodeGen.getNextNodeId(scope)));
		store.setAlias(lastOpToShare.getAlias());
		store.setSFile(new FileSpec(TEMP_FILE+tmpFileIter+System.currentTimeMillis(), funcSpec));
		tmpFileIter++;
		if(stores!=null && !stores.isEmpty()){
			store.setInputSpec(stores.get(0).getInputSpec());
		}
        //store.setSignature(loStore.getSignature());
        //store.setSortInfo(loStore.getSortInfo());
        store.setIsTmpStore(false);
        
        
		
		//clone the store and add it to the plan to share
        POStore storeClone=(POStore) store.clone();
        sharedOperatorsPlan.addAsLeaf(storeClone);
        //newMapperRootPlansPair.first=sharedOperatorsPlan;
        this.addAsLeaf(store);
		
		//clone the last operator to share and add it to this plan after connecting it to the store
        /*PhysicalOperator lastOpToShareClone=lastOpToShare.clone();
        //add to the clone of lastOpToShare and the store to this paln
        this.add(lastOpToShareClone);loadStorePlan
        this.add(store);
        this.connect(lastOpToShareClone, store);
        //copy all the input of lastOpToShare to the new lastOpToShareClone
        List<PhysicalOperator> lastOpToSharePreds=this.getPredecessors(lastOpToShare);
        for(PhysicalOperator pred:lastOpToSharePreds){
        	this.connect(pred,lastOpToShareClone);
        }*/
        
        //empty this plan
        /*this.trimAbove(lastOpToShare);
        this.remove(lastOpToShare);
        //create a load store plan
		PhysicalPlan loadStorePlan = createLoadStorePlan(store,pigContext);
		PhysicalOperator newStore= PlanHelper.getStores(loadStorePlan).get(0);
		PhysicalOperator newLoad= PlanHelper.getLoads(loadStorePlan).get(0);
		//this.add(newStore);
		this.add(newLoad);*/
		//this.connect(newLoad, newStore);
		//add the load store plan to the new root plans
		//newMapperRootPlansPair.second=null;
		
		//newMapperRootPlans.add(newMapperRootPlansPair);
		
		//return the plan to share
		//return sharedOperatorsPlan;
		return null;
	}

	private boolean hasStoreSuccessor(PhysicalOperator op) {
		List<PhysicalOperator> opSuccs = this.getSuccessors(op); 
		if(opSuccs==null || opSuccs.size()==0){
			return false;
		}
		for(PhysicalOperator succ:opSuccs){
			if(succ instanceof POStore){
				return true;
			}
		}
		
		return false;
	}

	private PhysicalPlan createPlan(POLoad load, PhysicalOperator lastOpToShare,
			PigContext pigContext, List<POStore> stores,
			List<PhysicalPlan> newMapperRootPlans) throws CloneNotSupportedException, PlanException {
		//create a clone of this plan
		PhysicalPlan sharedOperatorsPlan=this.clone();
		//trim above the operator that is equivalent to lastOpToShare
		List<PhysicalOperator> loadSucc=sharedOperatorsPlan.getSuccessors(load);
		if(loadSucc!= null && !loadSucc.isEmpty()){
			List<PhysicalOperator> loadSuccCopy=new ArrayList<PhysicalOperator>(loadSucc);
			//for every successor, check to find a filter or for each
			for(PhysicalOperator succ:loadSuccCopy){
				//check if the succ is a filter or foreach
				if((succ instanceof POFilter || succ instanceof POForEach) && succ.isEquivalent(lastOpToShare)){
					//trim above that operator
					sharedOperatorsPlan.trimAbove(succ);
					break;
				}
			}
		}
		//add a new store to that plan
		String scope = lastOpToShare.getOperatorKey().scope;
		NodeIdGenerator nodeGen = NodeIdGenerator.getGenerator();
		//List<POStore> stores = PlanHelper.getStores(this);
		/*FuncSpec funcSpec=new FuncSpec(PigStorage.class.getName() + "()");
		if(stores!=null && !stores.isEmpty()){
			for(POStore astore:stores){
				funcSpec=astore.getSFile().getFuncSpec();
				break;
			}
		}*/
		FuncSpec funcSpec=new FuncSpec(InterStorage.class.getName());
		POStore store = new POStore(new OperatorKey(scope, nodeGen.getNextNodeId(scope)));
		store.setAlias(lastOpToShare.getAlias());
		store.setSFile(new FileSpec(TEMP_FILE+tmpFileIter+System.currentTimeMillis(), funcSpec));
		tmpFileIter++;
		if(stores!=null && !stores.isEmpty()){
			store.setInputSpec(stores.get(0).getInputSpec());
		}
        //store.setSignature(loStore.getSignature());
        //store.setSortInfo(loStore.getSortInfo());
        store.setIsTmpStore(true);
        sharedOperatorsPlan.addAsLeaf(store);
        
		//add plan to newMapperRootPlans and return it
		newMapperRootPlans.add(sharedOperatorsPlan);
		
		//create a load store plan
		PhysicalPlan loadStorePlan = createLoadStorePlan(store,pigContext);
		//add the load store plan to the new root plans
		newMapperRootPlans.add(loadStorePlan);
		
		return sharedOperatorsPlan;
	}

	private PhysicalPlan splitPlan(POLoad load, PhysicalOperator lastOpToShare, PigContext pigContext,
			List<POStore> stores, List<POLoad> planLoads, List<Pair<PhysicalPlan, PhysicalPlan>> newMapperRootPlans) throws PlanException, CloneNotSupportedException, VisitorException {
		
		//initialize a pair to include the two created plans
		Pair<PhysicalPlan, PhysicalPlan> newMapperRootPlansPair = new Pair<PhysicalPlan,PhysicalPlan>(null, null);
		
		//get successors of the lastOpToShare
		List<PhysicalOperator> succsItr=getSuccessors(lastOpToShare);
		List<PhysicalOperator> succs=new ArrayList<PhysicalOperator>();
		if(succsItr!=null){
			succs.addAll(succsItr);
		}
		
		List<POLoad> lastOpToShareDecLoads=new ArrayList<POLoad> ();
		lastOpToShareDecLoads.add(load);
		for(POLoad planLoad:planLoads){
			if(!load.equals(planLoad) && isAncetorNode(planLoad, lastOpToShare)){
				lastOpToShareDecLoads.add(planLoad);
			}
		}
		
		//PLAN: shared mapper
		//copy the operators from load till this operator into a new plan, return the plan
		PhysicalPlan sharedOperatorsPlan= createPlanFromLoadToOperator(lastOpToShareDecLoads, lastOpToShare);
		//add a store operator to that plan
		String scope = lastOpToShare.getOperatorKey().scope;
		NodeIdGenerator nodeGen = NodeIdGenerator.getGenerator();
		//List<POStore> stores = PlanHelper.getStores(this);
		/*FuncSpec funcSpec=new FuncSpec(PigStorage.class.getName() + "()");
		if(stores!=null && !stores.isEmpty()){
			for(POStore astore:stores){
				funcSpec=astore.getSFile().getFuncSpec();
				break;
			}
		}*/
		FuncSpec funcSpec=new FuncSpec(InterStorage.class.getName());
		POStore store = new POStore(new OperatorKey(scope, nodeGen.getNextNodeId(scope)));
		store.setAlias(lastOpToShare.getAlias());
		store.setSFile(new FileSpec(TEMP_FILE+tmpFileIter+System.currentTimeMillis(), funcSpec));
		tmpFileIter++;
		if(stores!=null && !stores.isEmpty()){
			store.setInputSpec(stores.get(0).getInputSpec());
		}
        //store.setSignature(loStore.getSignature());
        //store.setSortInfo(loStore.getSortInfo());
        store.setIsTmpStore(true);
        sharedOperatorsPlan.addAsLeaf(store);
		//add the new plan to root plans
		newMapperRootPlansPair.first=sharedOperatorsPlan;
		
		//PLAN: unshared operators in this mapper
		//create a load to read from the tmp file
		NodeIdGenerator nodeGenLoad = NodeIdGenerator.getGenerator();
		
		FileSpec lFile=store.getInputSpec();
		POLoad tmpLoad = new POLoad(new OperatorKey(scope, nodeGenLoad
                .getNextNodeId(scope)), lFile);
		tmpLoad.setLFile(store.getSFile());
		tmpLoad.setPc(pigContext);
		tmpLoad.setResultType(store.getResultType());
		String aliasOfLastOpToShare=lastOpToShare.getAlias();
		if(aliasOfLastOpToShare!=null){
			tmpLoad.setAlias(aliasOfLastOpToShare);
		}
		
		this.replace(lastOpToShare, tmpLoad);
		//this.add(tmpLoad);
		//add the load as a root to successors of lastOPToShare
		//for(PhysicalOperator succ:succs){
			//connect(tmpLoad,succ);
		//}
		
		//disconnect the lastOPToShare from its successors
		
		//add the load as a predecssor to the sucessors
		
		//create a load store plan
		PhysicalPlan loadStorePlan = createLoadStorePlan(store,pigContext);
		//add the load store plan to the new root plans
		newMapperRootPlansPair.second=loadStorePlan;
		
		newMapperRootPlans.add(newMapperRootPlansPair);
		
		//create a cloan of sharedOperatorsPlan and replace the store to be the sharedStore location from the loadStorePlan
		PhysicalPlan sharedOperatorsPlanClone = sharedOperatorsPlan.clone();
		POStore storeFromLoadStorePlan = (POStore) PlanHelper.getStores(loadStorePlan).get(0).clone();
		POStore currentStore = PlanHelper.getStores(sharedOperatorsPlanClone).get(0);
		sharedOperatorsPlanClone.replace(currentStore, storeFromLoadStorePlan);
		
		return sharedOperatorsPlanClone;
	}

	private boolean isAncetorNode(PhysicalOperator ansOperator,
			PhysicalOperator operator) {
		if(ansOperator.equals(operator)){
			return true;
		}
		List<PhysicalOperator> preds=this.getPredecessors(operator);
		if(preds==null || preds.isEmpty()){
			return false;
		}else{
			for(PhysicalOperator pred:preds){
				if(isAncetorNode(ansOperator, pred)){
					return true;
				}
			}
		}
		return false;
	}

	private PhysicalPlan createPlanFromLoadToOperator(List<POLoad> loads,
			PhysicalOperator lastOpToShare) throws PlanException, CloneNotSupportedException {
		PhysicalPlan newPlan=new PhysicalPlan();
		List<PhysicalOperator> lastOpToShareClone = new ArrayList<PhysicalOperator>();
		moveOperatorAndSuccToPlan(loads.get(0),lastOpToShare, newPlan, lastOpToShareClone);
		for(int i=1;i<loads.size();i++){
			moveOperatorAndSuccToPlan(loads.get(i),lastOpToShare, newPlan, lastOpToShareClone);
		}
		return newPlan;
	}
	
	private PhysicalOperator moveOperatorAndSuccToPlan(PhysicalOperator operator, PhysicalOperator lastOpToShare, PhysicalPlan newPlan, List<PhysicalOperator> lastOperatorClone) throws PlanException, CloneNotSupportedException {
		//add operator to new plan
		PhysicalOperator operatorClone=null;
		/*if(!copyClone){
			operatorClone=operator.clone();
			newPlan.add(operatorClone);
			lastOperatorClone=operatorClone;
		}else{
			operatorClone=lastOperatorClone;
		}
		
		PhysicalOperator operatorClone=operator.clone();
		newPlan.add(operatorClone);*/
		
		if(operator.equals(lastOpToShare)){
			//remove operator from this plan
			//this.remove(operator);
			if(lastOperatorClone.isEmpty()){
				operatorClone=operator.clone();
				newPlan.add(operatorClone);
				lastOperatorClone.add(operatorClone);
			}else{
				operatorClone=lastOperatorClone.get(0);
			}
			return operatorClone;
		}else{
			operatorClone=operator.clone();
			newPlan.add(operatorClone);
			
		}
		//get successors of the operator
		List<PhysicalOperator> succs=getSuccessors(operator);
		//move every succ and its succs into the new plan and then copy their connections as well
		if(succs!=null){
			List<PhysicalOperator> succsCopy=new ArrayList<PhysicalOperator>(succs);
			for(PhysicalOperator succ:succsCopy){
				//move operator and its successors to plan
				PhysicalOperator succClone=moveOperatorAndSuccToPlan(succ,lastOpToShare, newPlan, lastOperatorClone);
				//move connections between op and succ to new plan
				newPlan.connect(operatorClone,  succClone);		
			}
		}
		//remove operator from this plan
		this.remove(operator);
		
		return operatorClone;
	}
	
	private PhysicalPlan createLoadStorePlan(POStore tmpStore, PigContext pigContext) throws PlanException{
		PhysicalPlan newLoadStorePlan=new PhysicalPlan();
		
		String scope = tmpStore.getOperatorKey().scope;
		
		
		//create a load that reads from this tmp Store
		NodeIdGenerator nodeGenLoad = NodeIdGenerator.getGenerator();
		FileSpec lFile=tmpStore.getInputSpec();
		POLoad tmpLoad = new POLoad(new OperatorKey(scope, nodeGenLoad
                .getNextNodeId(scope)), lFile);
		tmpLoad.setLFile(tmpStore.getSFile());
		tmpLoad.setPc(pigContext);
		tmpLoad.setResultType(tmpStore.getResultType());
		String aliasOfLastOpToShare=tmpStore.getAlias();
		if(aliasOfLastOpToShare!=null){
			tmpLoad.setAlias(aliasOfLastOpToShare);
		}
		
		newLoadStorePlan.add(tmpLoad);
		
		//create a store that stores the data into a perm shared location
		NodeIdGenerator nodeGenStore = NodeIdGenerator.getGenerator();
		POStore shareStore = new POStore(new OperatorKey(scope, nodeGenStore.getNextNodeId(scope)));
		shareStore.setAlias(tmpStore.getAlias());
		shareStore.setSFile(new FileSpec(SHARED_FILE+tmpFileIter+System.currentTimeMillis(), tmpStore.getSFile().getFuncSpec()));
		tmpFileIter++;
		//shareStore.setInputSpec(tmpStore.getInputSpec());
		
        //store.setSignature(loStore.getSignature());
        //store.setSortInfo(loStore.getSortInfo());
		shareStore.setIsTmpStore(false);
        newLoadStorePlan.addAsLeaf(shareStore);
        
		//connect the load/Store operators
        //newLoadStorePlan.connect(tmpLoad, shareStore);
		return newLoadStorePlan;
	}

	/**
	 * To replace the load in this plan with the store from the shared plan
	 * @param planWithTempStore
	 * @param planWithSharedStore
	 * @throws VisitorException 
	 */
	public void replaceTmpLoadWithSharedStorage(PhysicalPlan  planWithTempStore, PhysicalPlan planWithSharedStore) throws VisitorException {
		//get the temp store file from planWithTempStore
		POStore tempStore=PlanHelper.getStores(planWithTempStore).get(0);
		
		//get the temp store file name
		String tempStoreFileName = tempStore.getSFile().getFileName();
		
		//get the shared store file name
		String sharedStoreFileName=PlanHelper.getStores(planWithSharedStore).get(0).getSFile().getFileName();
		
		//get the load of temp file
		List<POLoad> planLoads=PlanHelper.getLoads(this);
		POLoad tempLoad=null;
		for(POLoad planLoad:planLoads){
			if(planLoad.getLFile().getFileName().equals(tempStoreFileName)){
				tempLoad=planLoad;
				break;
			}
		}
		
		//change the temp load file name from tmp file name to shared file name
		tempLoad.getLFile().setFileName(sharedStoreFileName);
		
		
		//change the store in planWithTempStore to shared store
		tempStore.getSFile().setFileName(sharedStoreFileName);
		tempStore.setIsTmpStore(false);
	}

	public static String getNewSharedStorageLocation() {
		String newStoreLoc=SHARED_FILE+tmpFileIter+System.currentTimeMillis();
		tmpFileIter++;
		return newStoreLoc;
	}

	public void discoverUsefulSubplansReducer(PigContext pigContext,
			Configuration conf, List<POStore> stores,
			List<PhysicalPlan> newMapperRootPlans) throws PlanException, CloneNotSupportedException {
		
		//find the roots of this plan
		List<PhysicalOperator> roots = new ArrayList<PhysicalOperator>(this.getRoots());
		for(PhysicalOperator op: roots){
			//insert a store after this operator
			if(! (op instanceof POLoad) && ! (op instanceof POStore)  && ! hasStoreSuccessor(op)){
				splitReducerPlan(op, newMapperRootPlans, pigContext, stores);
			}
		}
		
	}

	private void splitReducerPlan(PhysicalOperator op,
			List<PhysicalPlan> newMapperRootPlans,PigContext pigContext, List<POStore> stores ) throws PlanException, CloneNotSupportedException {
		//create a new store
		String scope = op.getOperatorKey().scope;
		NodeIdGenerator nodeGen = NodeIdGenerator.getGenerator();
		//List<POStore> stores = PlanHelper.getStores(this);
		/*FuncSpec funcSpec=new FuncSpec(PigStorage.class.getName() + "()");
		if(stores!=null && !stores.isEmpty()){
			for(POStore astore:stores){
				funcSpec=astore.getSFile().getFuncSpec();
				break;
			}
		}*/
		FuncSpec funcSpec=new FuncSpec(InterStorage.class.getName());
		POStore store = new POStore(new OperatorKey(scope, nodeGen.getNextNodeId(scope)));
		store.setAlias(op.getAlias());
		store.setSFile(new FileSpec(SHARED_FILE+tmpFileIter+System.currentTimeMillis(), funcSpec));
		tmpFileIter++;
		if(stores!=null && !stores.isEmpty()){
			store.setInputSpec(stores.get(0).getInputSpec());
		}
		
		//for each succ of op, create a new plan
		List<PhysicalOperator> succs=getSuccessors(op);
		if(succs!=null){
			List<PhysicalOperator> succCopy=new ArrayList<PhysicalOperator>(succs);
			for(PhysicalOperator succ:succCopy){
				PhysicalPlan newPlan=new PhysicalPlan();
				PhysicalOperator succClone = moveOpsFromLastSharedToLeaf(succ,newPlan);
				//create load and add it to plan
				NodeIdGenerator nodeGenLoad = NodeIdGenerator.getGenerator();
				
				FileSpec lFile=store.getInputSpec();
				POLoad tmpLoad = new POLoad(new OperatorKey(scope, nodeGenLoad
		                .getNextNodeId(scope)), lFile);
				tmpLoad.setLFile(store.getSFile());
				tmpLoad.setPc(pigContext);
				tmpLoad.setResultType(store.getResultType());
				String aliasOfLastOpToShare=op.getAlias();
				if(aliasOfLastOpToShare!=null){
					tmpLoad.setAlias(aliasOfLastOpToShare);
				}
				newPlan.add(tmpLoad);
				newPlan.connect(tmpLoad, succClone);
				newMapperRootPlans.add(newPlan);
			}
		}
		
		//add store to this plan
		this.addAsLeaf(store);
		
	}

	private PhysicalPlan createPlanFromLastSharedToLeaf(PhysicalOperator op) {
		
		return null;
	}
	

	private PhysicalOperator moveOpsFromLastSharedToLeaf(PhysicalOperator operator, PhysicalPlan newPlan) throws CloneNotSupportedException, PlanException {
		
		if(operator==null){
			return null;
		}
		PhysicalOperator operatorClone=null;
		operatorClone=operator.clone();
		newPlan.add(operatorClone);
		
		//get successors of the operator
		List<PhysicalOperator> succs=getSuccessors(operator);
		//move every succ and its succs into the new plan and then copy their connections as well
		if(succs!=null){
			if(succs!=null && !succs.isEmpty()){
				List<PhysicalOperator> succsCopy=new ArrayList<PhysicalOperator>(succs);
				for(PhysicalOperator succ:succsCopy){
					//move operator and its successors to plan
					PhysicalOperator succClone=moveOpsFromLastSharedToLeaf(succ, newPlan);
					//move connections between op and succ to new plan
					newPlan.connect(operatorClone,  succClone);		
				}
			}
		}
		//remove operator from this plan
		this.remove(operator);
		
		return operatorClone;
	}
}
