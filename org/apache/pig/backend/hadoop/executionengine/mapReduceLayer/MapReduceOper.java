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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MRPrinter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanRefUpdater;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Pair;

/**
 * An operator model for a Map Reduce job. 
 * Acts as a host to the plans that will
 * execute in map, reduce and optionally combine
 * phases. These will be embedded in the MROperPlan
 * in order to capture the dependencies amongst jobs.
 */
public class MapReduceOper extends Operator<MROpPlanVisitor> {
    private static final long serialVersionUID = 1L;
    //@iman
	public static final String DISCOVER_NEWPLANS_HEURISTICS = "sharing.useHeuristics.discoverPlans";
    //The physical plan that should be executed
    //in the map phase
    public PhysicalPlan mapPlan;
    
    //The physical plan that should be executed
    //in the reduce phase
    public PhysicalPlan reducePlan;
    
    //The physical plan that should be executed
    //in the combine phase if one exists. Will be used
    //by the optimizer.
    public PhysicalPlan combinePlan;
    
    // key for the map plan
    // this is needed when the key is null to create
    // an appropriate NullableXXXWritable object
    public byte mapKeyType;
    
    //Indicates that the map plan creation
    //is complete
    boolean mapDone = false;
    
    //Indicates that the reduce plan creation
    //is complete
    boolean reduceDone = false;
    
    // Indicates that there is an operator which uses endOfAllInput flag in the 
    // map plan
    boolean endOfAllInputInMap = false;
    
    // Indicates that there is an operator which uses endOfAllInput flag in the 
    // reduce plan
    boolean endOfAllInputInReduce = false;;
    
    //Indicates if this job is an order by job
    boolean globalSort = false;

    // Indicates if this is a limit after a sort
    boolean limitAfterSort = false;
    
    // Indicate if the entire purpose for this map reduce job is doing limit, does not change
    // anything else. This is to help POPackageAnnotator to find the right POPackage to annotate
    boolean limitOnly = false;
    
    OPER_FEATURE feature = OPER_FEATURE.NONE;

    // If true, putting an identity combine in this
    // mapreduce job will speed things up.
    boolean needsDistinctCombiner = false;
    
    // If true, we will use secondary key in the map-reduce job
    boolean useSecondaryKey = false;
    
    //The quantiles file name if globalSort is true
    String quantFile;
    
    //The sort order of the columns;
    //asc is true and desc is false
    boolean[] sortOrder;
    
    // Sort order for secondary keys;
    boolean[] secondarySortOrder;

    public Set<String> UDFs;
    
    public Set<PhysicalOperator> scalars;
    
    // Indicates if a UDF comparator is used
    boolean isUDFComparatorUsed = false;
    
    transient NodeIdGenerator nig;

    private String scope;
    
    int requestedParallelism = -1;
    
    /* Name of the Custom Partitioner used */ 
    String customPartitioner = null;
    
    // Last POLimit value in this map reduce operator, needed by LimitAdjuster
    // to add additional map reduce operator with 1 reducer after this
    long limit = -1;

    // Indicates that this MROper is a splitter MROper. 
    // That is, this MROper ends due to a POSPlit operator.
    private boolean splitter = false;

	// Set to true if it is skewed join
	private boolean skewedJoin = false;

    // Name of the partition file generated by sampling process,
    // Used by Skewed Join
	private String skewedJoinPartitionFile;
	
	// Flag to communicate from MRCompiler to JobControlCompiler what kind of
	// comparator is used by Hadoop for sorting for this MROper. 
	// By default, set to false which will make Pig provide raw comparators. 
	// Set to true in indexing job generated in map-side cogroup, merge join.
	private boolean usingTypedComparator = false;
	
	// Flag to indicate if the small input splits need to be combined to form a larger
	// one in order to reduce the number of mappers. For merge join, both tables
	// are NOT combinable for correctness.
	private boolean combineSmallSplits = true;
	
	private static enum OPER_FEATURE {
	    NONE,
	    // Indicate if this job is a sampling job
	    SAMPLER,
	    // Indicate if this job is a merge indexer
	    INDEXER,
	    // Indicate if this job is a group by job
	    GROUPBY,	    
	    // Indicate if this job is a cogroup job
	    COGROUP,	    
	    // Indicate if this job is a regular join job
	    HASHJOIN;
	};
	
    public MapReduceOper(OperatorKey k) {
        super(k);
        mapPlan = new PhysicalPlan();
        combinePlan = new PhysicalPlan();
        reducePlan = new PhysicalPlan();
        UDFs = new HashSet<String>();
        scalars = new HashSet<PhysicalOperator>();
        nig = NodeIdGenerator.getGenerator();
        scope = k.getScope();
    }

    /*@Override
    public String name() {
        return "MapReduce - " + mKey.toString();
    }*/
    
    /**
	 * clones an operator
	 * @author iman	
     */
    public MapReduceOper clone() throws CloneNotSupportedException {
    	MapReduceOper operClone=new MapReduceOper(new OperatorKey(mKey.scope, NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)));
    	if(mapPlan!=null&&!mapPlan.isEmpty()){
    		operClone.mapPlan=mapPlan.clone();
    	}else{
    		operClone.mapPlan=new PhysicalPlan();;
    	}
    	if(reducePlan!=null&&!reducePlan.isEmpty()){
    		operClone.reducePlan=reducePlan.clone();
    	}else{
    		operClone.reducePlan=new PhysicalPlan();;
    	}
    	if(combinePlan!=null&&!combinePlan.isEmpty()){
    		operClone.combinePlan=combinePlan.clone();
    	}else{
    		operClone.combinePlan=new PhysicalPlan();;
    	}
    	operClone.mapKeyType=mapKeyType;
    	operClone.mapDone=mapDone;
    	operClone.reduceDone=reduceDone;
    	operClone.endOfAllInputInMap=endOfAllInputInMap;
    	operClone.endOfAllInputInReduce=endOfAllInputInReduce;
    	operClone.globalSort=globalSort;
    	operClone.limitAfterSort=limitAfterSort;
    	operClone.limitOnly=limitOnly;
    	operClone.feature=feature;
    	operClone.needsDistinctCombiner=needsDistinctCombiner;
    	operClone.useSecondaryKey=useSecondaryKey;
    	if(quantFile!=null){
    		operClone.quantFile=new String(quantFile);
    	}else{
    		operClone.quantFile=null;
    	}
    	if(sortOrder!=null){
    		operClone.sortOrder=sortOrder.clone();
    	}else{
    		operClone.sortOrder=null;
    	}
    	if(secondarySortOrder!=null){
    		operClone.secondarySortOrder=secondarySortOrder.clone();
    	}else{
    		operClone.secondarySortOrder=null;
    	}
    	if(UDFs!=null){
    		operClone.UDFs=new HashSet<String>(UDFs);
    	}else{
    		operClone.UDFs=null;
    	}
    	if(scalars!=null){
    		operClone.scalars=new HashSet<PhysicalOperator>(scalars);
    	}else{
    		operClone.scalars=null;
    	}
    	operClone.isUDFComparatorUsed=isUDFComparatorUsed;
    	operClone.nig=NodeIdGenerator.getGenerator();
    	operClone.scope=scope;
    	operClone.requestedParallelism=requestedParallelism;
    	if(customPartitioner!=null){
    		operClone.customPartitioner=new String(customPartitioner);
    	}
    	operClone.limit=limit;
    	operClone.splitter=splitter;
    	operClone.skewedJoin=skewedJoin;
    	if(skewedJoinPartitionFile!=null){
    		operClone.skewedJoinPartitionFile=new String(skewedJoinPartitionFile);
    	}else{
    		operClone.skewedJoinPartitionFile=null;
    	}
    	operClone.usingTypedComparator=usingTypedComparator;
    	operClone.combineSmallSplits=combineSmallSplits;
    	/*if(phyToMRMap!=null){
    		operClone.phyToMRMap=phyToMRMap.clone();
    	}else{
    		operClone.phyToMRMap=new MultiMap<PhysicalOperator, PhysicalOperator>();
    	}*/
    	
    	return operClone;
	}
	
    private String shiftStringByTabs(String DFStr, String tab) {
        StringBuilder sb = new StringBuilder();
        String[] spl = DFStr.split("\n");
        for (int i = 0; i < spl.length; i++) {
            sb.append(tab);
            sb.append(spl[i]);
            sb.append("\n");
        }
        sb.delete(sb.length() - "\n".length(), sb.length());
        return sb.toString();
    }
    
    /**
     * Uses the string representation of the 
     * component plans to identify itself.
     */
    @Override
    public String name() {
        String udfStr = getUDFsAsStr();
        
        StringBuilder sb = new StringBuilder("MapReduce" + "(" + requestedParallelism + 
                (udfStr.equals("")? "" : ",") + udfStr + ")" + " - " + mKey.toString()
                + ":\n");
        int index = sb.length();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        if(!mapPlan.isEmpty()){
            mapPlan.explain(baos);
            String mp = new String(baos.toByteArray());
            sb.append(shiftStringByTabs(mp, "|   "));
        }
        else
            sb.append("Map Plan Empty");
        if (!reducePlan.isEmpty()){
            baos.reset();
            reducePlan.explain(baos);
            String rp = new String(baos.toByteArray());
            sb.insert(index, shiftStringByTabs(rp, "|   ") + "\n");
        }
        else
            sb.insert(index, "Reduce Plan Empty" + "\n");
        return sb.toString();
    }

    private String getUDFsAsStr() {
        StringBuilder sb = new StringBuilder();
        if(UDFs!=null && UDFs.size()>0){
            for (String str : UDFs) {
                sb.append(str.substring(str.lastIndexOf('.')+1));
                sb.append(',');
            }
            sb.deleteCharAt(sb.length()-1);
        }
        return sb.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return true;
    }

    @Override
    public void visit(MROpPlanVisitor v) throws VisitorException {
        v.visitMROp(this);
    }
    
    public boolean isMapDone() {
        return mapDone;
    }
    
    public void setMapDone(boolean mapDone){
        this.mapDone = mapDone;
    }
    
    public void setMapDoneSingle(boolean mapDone) throws PlanException{
        this.mapDone = mapDone;
        if (mapDone && mapPlan.getLeaves().size()>1) {
            mapPlan.addAsLeaf(getUnion());
        }
    }
    
    public void setMapDoneMultiple(boolean mapDone) throws PlanException{
        this.mapDone = mapDone;
        if (mapDone && mapPlan.getLeaves().size()>0) {
            mapPlan.addAsLeaf(getUnion());
        }
    }
    
    private POUnion getUnion(){
        return new POUnion(new OperatorKey(scope,nig.getNextNodeId(scope)));
    }
    
    public boolean isReduceDone() {
        return reduceDone;
    }

    public void setReduceDone(boolean reduceDone){
        this.reduceDone = reduceDone;
    }
    
    public boolean isGlobalSort() {
        return globalSort;
    }
    
    public boolean isSkewedJoin() {
    	return (skewedJoinPartitionFile != null);
    }
    
    public void setSkewedJoinPartitionFile(String file) {    	
    	skewedJoinPartitionFile = file;
    }
    
    public String getSkewedJoinPartitionFile() {
    	return skewedJoinPartitionFile;
    }

	public void setSkewedJoin(boolean skJoin) {
		this.skewedJoin = skJoin;
	}

	public boolean getSkewedJoin() {
		return skewedJoin;
	}

    public void setGlobalSort(boolean globalSort) {
        this.globalSort = globalSort;
    }

    public boolean isLimitAfterSort() {
        return limitAfterSort;
    }

    public void setLimitAfterSort(boolean las) {
        limitAfterSort = las;
    }
    
    public boolean isLimitOnly() {
        return limitOnly;
    }
    
    public void setLimitOnly(boolean limitOnly) {
        this.limitOnly = limitOnly;
    }

    public boolean isIndexer() {
        return (feature == OPER_FEATURE.INDEXER);
    }
    
    public void markIndexer() {
        feature = OPER_FEATURE.INDEXER;
    }
    
    public boolean isSampler() {
        return (feature == OPER_FEATURE.SAMPLER);
    }
    
    public void markSampler() {
        feature = OPER_FEATURE.SAMPLER;
    }
    
    public boolean isGroupBy() {
        return (feature == OPER_FEATURE.GROUPBY);
    }
    
    public void markGroupBy() {
        feature = OPER_FEATURE.GROUPBY;
    }
    
    public boolean isCogroup() {
        return (feature == OPER_FEATURE.COGROUP);
    }
    
    public void markCogroup() {
        feature = OPER_FEATURE.COGROUP;
    }
    
    public boolean isRegularJoin() {
        return (feature == OPER_FEATURE.HASHJOIN);
    }
    
    public void markRegularJoin() {
        feature = OPER_FEATURE.HASHJOIN;
    }
    
    public boolean needsDistinctCombiner() { 
        return needsDistinctCombiner;
    }

    public void setNeedsDistinctCombiner(boolean nic) {
        needsDistinctCombiner = nic;
    }

    public String getQuantFile() {
        return quantFile;
    }

    public void setQuantFile(String quantFile) {
        this.quantFile = quantFile;
    }

    public void setSortOrder(boolean[] sortOrder) {
        if(null == sortOrder) return;
        this.sortOrder = new boolean[sortOrder.length];
        for(int i = 0; i < sortOrder.length; ++i) {
            this.sortOrder[i] = sortOrder[i];
        }
    }
    
    public void setSecondarySortOrder(boolean[] secondarySortOrder) {
        if(null == secondarySortOrder) return;
        this.secondarySortOrder = new boolean[secondarySortOrder.length];
        for(int i = 0; i < secondarySortOrder.length; ++i) {
            this.secondarySortOrder[i] = secondarySortOrder[i];
        }
    }
             
    public boolean[] getSortOrder() {
        return sortOrder;
    }

    public boolean[] getSecondarySortOrder() {
        return secondarySortOrder;
    }

    /**
     * @return whether end of all input is set in the map plan
     */
    public boolean isEndOfAllInputSetInMap() {
        return endOfAllInputInMap;
    }

    /**
     * @param endOfAllInputInMap the streamInMap to set
     */
    public void setEndOfAllInputInMap(boolean endOfAllInputInMap) {
        this.endOfAllInputInMap = endOfAllInputInMap;
    }

    /**
     * @return whether end of all input is set in the reduce plan
     */
    public boolean isEndOfAllInputSetInReduce() {
        return endOfAllInputInReduce;
    }

    /**
     * @param endOfAllInputInReduce the streamInReduce to set
     */
    public void setEndOfAllInputInReduce(boolean endOfAllInputInReduce) {
        this.endOfAllInputInReduce = endOfAllInputInReduce;
    }    

    public int getRequestedParallelism() {
        return requestedParallelism;
    }
    
    public String getCustomPartitioner() {
    	return customPartitioner;
    }

    public void setSplitter(boolean spl) {
        splitter = spl;
    }

    public boolean isSplitter() {
        return splitter;
    }
    
    public boolean getUseSecondaryKey() {
        return useSecondaryKey;
    }
    
    public void setUseSecondaryKey(boolean useSecondaryKey) {
        this.useSecondaryKey = useSecondaryKey;
    }

    protected boolean usingTypedComparator() {
        return usingTypedComparator;
    }

    protected void useTypedComparator(boolean useTypedComparator) {
        this.usingTypedComparator = useTypedComparator;
    }
    
    protected void noCombineSmallSplits() {
        combineSmallSplits = false;
    }
    
    public boolean combineSmallSplits() {
        return combineSmallSplits;
    }
    
    /**
     * 
     * @param otherOP
     * @return whether this op is equivalent to the otherOp or not 
     * @author iman
     */
  public boolean isEquivalent(MapReduceOper otherOP) {
		if(this.getClass().equals(otherOP.getClass())){
			//the two operators are of the same class
			if(equivalentPlans(mapPlan,otherOP.mapPlan) && equivalentPlans(reducePlan,otherOP.reducePlan) && equivalentPlans(combinePlan,otherOP.combinePlan)){
				return true;
			}
		}
		return false;
	}
	
	/**
	 * @author iman
     */
  public boolean equivalentPlans(PhysicalPlan plan1, PhysicalPlan plan2){
		if(plan1==null && plan2 == null){
			return true;
		}
		if(plan1!=null && plan2!=null && plan1.isEquivalent(plan2)){
			return true;
		}
		return false;
	}

	/**
	 * @author iman
	 * @throws CloneNotSupportedException 
     */
  public MapReduceOper getPlanRecplacedWithView(MapReduceOper otherOP, PigContext pigContext) throws PlanException, VisitorException, CloneNotSupportedException {
		if(this.getClass().equals(otherOP.getClass())){
			//the two operators are of the same class
			if((otherOP.reducePlan!=null && reducePlan!=null) && (!otherOP.reducePlan.isEmpty() && !reducePlan.isEmpty())){
				if(equivalentPlans(mapPlan,otherOP.mapPlan) /*&& equivalentPlans(combinePlan,otherOP.combinePlan) && equivalentPlans(combinePlan,otherOP.combinePlan)*/){
					//make sure the map and combine plans are equivalent
					//check that the reduce plan of the otherOP is equivalent with a subset of this reduce plan
					PhysicalOperator matchedOperator = reducePlan.getPlanRecplacedWithView(otherOP.reducePlan);
					//replace the subplan rooted by matched opertaor in the reduce plan by the store in otherOP.reducePlan
					//I am assuming that there is only one store in the other plan
					if(matchedOperator!=null){
						//check if the successors of this plan is actually a store operator
						/*List<PhysicalOperator> matchedOperatorSuccs = reducePlan.getSuccessors(matchedOperator);
						boolean allsucStores=true;
						for(PhysicalOperator suc:matchedOperatorSuccs){
							if(! (suc instanceof POStore)){
								//found a successor of the matched operator that is not a store 
								allsucStores=false;
								break;
							}
						}
						if(allsucStores){
							//this job can be replaced by the view after changing the store location
							return otherOP;
						}else{
							//we still need to run this job
							reducePlan.replaceOperatorWithLoad(matchedOperator,PlanHelper.getStores(otherOP.reducePlan).get(0),pigContext);
							//make the map plan null
							mapPlan=new PhysicalPlan();
							return this;
						}*/
						
						List<POStore> otherPlanStores=PlanHelper.getStores(otherOP.reducePlan);
						//List<PhysicalOperator> otherPlanStoresPred=new ArrayList<PhysicalOperator>();
						//for(POStore otherPlanStore:otherPlanStores){
							//otherPlanStoresPred.addAll(otherOP.mapPlan.getPredecessors(otherPlanStore));
						//}
						//boolean allsucStores=true;
						//check if the matchedOperator is equivalent to any of the predecessors... if yes,
						//then we can conclude that it is safe to replace the  matched operator in
						//the new plan with a load from the store of the shared plan
						POStore otherPlanStoreToBeused=null;
						for(POStore otherPlanStore:otherPlanStores){
							List<PhysicalOperator> otherPlanStoresPred=otherOP.reducePlan.getPredecessors(otherPlanStore);
							if(otherPlanStoresPred.size()==1 && otherPlanStoresPred.get(0).isEquivalent(matchedOperator)){
								otherPlanStoreToBeused=otherPlanStore;
								break;
							}
						}
						if(otherPlanStoreToBeused!=null){
							//check if this plan has only stores after the matched operator
							boolean allsucStores=true;
							List<PhysicalOperator> matchedOperatorSuccs = reducePlan.getSuccessors(matchedOperator);
							for(PhysicalOperator suc:matchedOperatorSuccs){
								if(! (suc instanceof POStore)){
									//found a successor of the matched operator that is not a store 
									allsucStores=false;
									break;
								}
							}
							if(allsucStores ){
								//this job can be replaced by the view after changing the store location
								return otherOP;
							}else{
								//the shared plan is not completely subsumed in this plan
								//we replace the last matched operator with a load from the shared plan o/p location and 
								//we still need to run this job
								reducePlan.replaceOperatorWithLoad(matchedOperator,otherPlanStoreToBeused,pigContext);
								
								//since the matching is up to the reducer, and now the mapper is empty.. 
								//we need to move the reducer to the mapper and delete the combiner if it exists
								
								mapPlan.emptyPlan();
								mapPlan=reducePlan.clone();//new PhysicalPlan(reducePlan);
								combinePlan.emptyPlan();
								reducePlan.emptyPlan();
								
								return this;
							}
						}else{
							//the matched Plan is not fully subsumed in this plan,,, we do not consider this case
							System.out.println("the matched Plan is not fully subsumed in this plan,,, we do not consider this case");
							return null;
						}
					}else{
						//the map plans are equivalent, then the sharing goes as far as the map plan
						//in this case we need to change the loads in the reduce plan into load from the view location 
						//instead of the map temp location
						//TODO
						return null;
					}
				}else{
					//check if the sharing exist on the level of the map plan
					//check that the map  plan of the otherOP is equivalent with a subset of this map plan
					PhysicalOperator matchedOperator = mapPlan.getPlanRecplacedWithView(otherOP.mapPlan);
					//replace the subplan rooted by matched opertaor in the reduce plan by the store in otherOP.reducePlan
					//I am assuming that there is only one store in the other plan
					if(matchedOperator!=null){
						//mapPlan.replaceOperatorWithLoad(matchedOperator,PlanHelper.getStores(otherOP.mapPlan).get(0),pigContext);
						//return this;
						System.out.println("Matching went as far as the mapper but the reducer do not match, so we cannot use the plan in the repository");
						PrintStream ps=System.out;
						MROperPlan execPlan= new MROperPlan();
	                    execPlan.add(otherOP);
	                    MRPrinter printer = new MRPrinter(ps, execPlan);
	                    printer.setVerbose(true);
	                    try {
	        				printer.visit();
	        			} catch (VisitorException e) {
	        				System.out.println("Unable to print job plan");
	        			}
						return null;
					}else{
						return null;
					}
				}
			}else if(otherOP.reducePlan==null || otherOP.reducePlan.isEmpty()){
				// the reduce plan is null
				//check that the map  plan of the otherOP is equivalent with a subset of this map plan
				PhysicalOperator matchedOperator = mapPlan.getPlanRecplacedWithView(otherOP.mapPlan);
				//replace the subplan rooted by matched opertaor in the reduce plan by the store in otherOP.reducePlan
				//I am assuming that there is only one store in the other plan
				if(matchedOperator!=null){
					//check if the successors of this plan is actually a store operator
					/*List<PhysicalOperator> matchedOperatorSuccs = mapPlan.getSuccessors(matchedOperator);
					boolean allsucStores=true;
					for(PhysicalOperator suc:matchedOperatorSuccs){
						if(! (suc instanceof POStore)){
							//found a successor of the matched operator that is not a store 
							allsucStores=false;
							break;
						}
					}*/
					List<POStore> otherPlanStores=PlanHelper.getStores(otherOP.mapPlan);
					//List<PhysicalOperator> otherPlanStoresPred=new ArrayList<PhysicalOperator>();
					//for(POStore otherPlanStore:otherPlanStores){
						//otherPlanStoresPred.addAll(otherOP.mapPlan.getPredecessors(otherPlanStore));
					//}
					//boolean allsucStores=true;
					//check if the matchedOperator is equivalent to any of the predecessors... if yes,
					//then we can conclude that it is safe to replace the  matched operator in
					//the new plan with a load from the store of the shared plan
					POStore otherPlanStoreToBeused=null;
					for(POStore otherPlanStore:otherPlanStores){
						List<PhysicalOperator> otherPlanStoresPred=otherOP.mapPlan.getPredecessors(otherPlanStore);
						if(otherPlanStoresPred.size()==1 && otherPlanStoresPred.get(0).isEquivalent(matchedOperator)){
							otherPlanStoreToBeused=otherPlanStore;
							break;
						}
					}
					if(otherPlanStoreToBeused!=null){
						//check if this plan has only stores after the matched operator
						boolean allsucStores=true;
						List<PhysicalOperator> matchedOperatorSuccs = mapPlan.getSuccessors(matchedOperator);
						for(PhysicalOperator suc:matchedOperatorSuccs){
							if(! (suc instanceof POStore)){
								//found a successor of the matched operator that is not a store 
								allsucStores=false;
								break;
							}
						}
						if(allsucStores && reducePlan.isEmpty()){
							//this job can be replaced by the view after changing the store location
							return otherOP;
						}else{
							//the shared plan is not completely subsumed in this plan
							//we replace the last matched operator with a load from the shared plan o/p location and 
							//we still need to run this job
							mapPlan.replaceOperatorWithLoad(matchedOperator,otherPlanStoreToBeused,pigContext);
							return this;
						}
					}else{
						//the matched Plan is not fully subsumed in this plan,,, we do not consider this case
						System.out.println("the matched Plan is not fully subsumed in this plan,,, we do not consider this case");
						return null;
					}
					/*if(allsucStores && reducePlan.isEmpty()){
						//this job can be replaced by the view after changing the store location
						return otherOP;
					}else{
						//we still need to run this job
						mapPlan.replaceOperatorWithLoad(matchedOperator,PlanHelper.getStores(otherOP.mapPlan).get(0),pigContext);
						return this;
					}*/
				}else{
					return null;
				}
			}
		}
		return null;
	}

	/**
	 * @author iman
     */
  	public boolean  updateLoadOperator(MapReduceOper replaceablePlan,MapReduceOper sharedMRPlan) throws VisitorException, PlanException {
		//get the loads of this plan
		List<POLoad> planLoads = PlanHelper.getLoads(mapPlan);
		
		if(planLoads==null || planLoads.isEmpty()){
			//no loads in this plan
			return false;
		}
		System.out.println("To replace job input with the shared plan job output");
		
		POStore replaceablePlanStore=getStore(replaceablePlan);
		System.out.println("the old plan load location: "+replaceablePlanStore.getSFile().getFileName());
		if(replaceablePlanStore==null){
			//undecidable view replacement, then exit
			return false;
		}
		
		POStore sharedPlanStore=getStore(sharedMRPlan);
		System.out.println("the new plan load location: "+sharedPlanStore.getSFile().getFileName());
		if(sharedPlanStore==null){
			//undecidable view replacement, then exit
			return false;
		}
		
		//check if the load matches with the replacablePlan store
		for(POLoad load:planLoads){
			if(load.getLFile().getFileName().equals(replaceablePlanStore.getSFile().getFileName())){
				//the load and store match then do the merging of the plans
				PhysicalPlan replaceablePlanPart=null;
				if(replaceablePlan.reducePlan.isEmpty()){
					replaceablePlanPart=replaceablePlan.mapPlan;
				}else{
					replaceablePlanPart=replaceablePlan.reducePlan;
				}
				//mapPlan.replaceLoadWithOperatorsFromPlan(load,replaceablePlanStore,replaceablePlanPart);
				load.getLFile().setFileName(sharedPlanStore.getSFile().getFileName());
				return true;
			}
		}
		
		return false;
	}

	/**
	 * @author iman
     */
  	public POStore getStore(MapReduceOper mrplan) throws VisitorException {
		POStore store=null;
		PhysicalPlan planWithFinalStore=null;
		if(mrplan.reducePlan!=null && !mrplan.reducePlan.isEmpty()){
			planWithFinalStore=mrplan.reducePlan;
		}else{
			planWithFinalStore=mrplan.mapPlan;
		}
		//get the list of stores from the plan
		List<POStore> stores = PlanHelper.getStores(planWithFinalStore);
		if(stores==null || stores.isEmpty()){
			//no stores available
			return null;
		}else{
			//check if there is one store, else ambiguous result
			if(stores.size()>1){
				return null;
			}else{
				return stores.get(0);
			}
		}
		
	}

  	public static List<POStore> getStores(MapReduceOper mrplan) throws VisitorException{
  		POStore store=null;
		PhysicalPlan planWithFinalStore=null;
		if(mrplan.reducePlan!=null && !mrplan.reducePlan.isEmpty()){
			planWithFinalStore=mrplan.reducePlan;
		}else{
			planWithFinalStore=mrplan.mapPlan;
		}
		//get the list of stores from the plan
		List<POStore> stores = PlanHelper.getStores(planWithFinalStore);
		if(stores==null || stores.isEmpty()){
			//no stores available
			return null;
		}else{
			return stores;
			
		}
  	}
  	
  	public static List<String> getStoreLocs(MapReduceOper mrplan) throws VisitorException{
  		List<POStore> stores= getStores(mrplan);
  		List<String> storeLocs=new ArrayList<String>();
  		if(stores!=null){
	  		for(POStore store:stores){
	  			String storeLoc=store.getSFile().getFileName();
	  			storeLocs.add(storeLoc);
	  		}
  		}
  		return storeLocs;
  	}
  	/**
  	 * discover sub plans to share by adding a split and store tmp data after a filter or foreach
  	 * that makes substantial change
  	 * @pre note that this plan is after making any possible rewriting, so discovering other possible
  	 * shared plans will not overlap with any existing shared plans!
  	 * @return
  	 * @author iman
  	 * @param plan 
  	 * @throws CloneNotSupportedException 
  	 * @throws PlanException 
  	 * @throws VisitorException 
  	 */
	public List<MapReduceOper> discoverUsefulSubplans(PigContext pigContext, Configuration conf,MROperPlan mrplan,List <MapReduceOper> newMRRootPlans) throws VisitorException, PlanException, CloneNotSupportedException {
		List<MapReduceOper> subPlansToShare=new ArrayList<MapReduceOper>();
		List <Pair<PhysicalPlan, PhysicalPlan>> newMapperRootPlans=new ArrayList<Pair<PhysicalPlan, PhysicalPlan>>();
		//List <MapReduceOper> newMRRootPlans=new ArrayList<MapReduceOper>();
		
		//get stores of this plan
		List<POStore> stores=null;
		if(!reducePlan.isEmpty()){
			stores=PlanHelper.getStores(reducePlan);
		}else if(!mapPlan.isEmpty()){
			stores=PlanHelper.getStores(mapPlan);
		}else{
			stores=new ArrayList<POStore>();
		}
		
		//get the reducer
		//if(reducePlan!=null && !reducePlan.isEmpty()){
			//Vector<PhysicalPlan> subMapPlansToShare = reducePlan.discoverUsefulSubplans(pigContext, conf,stores);
		//}
		//get the mapper
		if(!mapPlan.isEmpty()){
			
			Vector<PhysicalPlan> subMapPlansToShare = mapPlan.discoverUsefulSubplans(pigContext, conf,stores,newMapperRootPlans);
			//create MR plan for every discovered subplan
			for(PhysicalPlan newPlan:subMapPlansToShare){
				if(newPlan!=null&&!newPlan.isEmpty()){
					//fix any references to any operator before the split in the new current plan
					PhyPlanRefUpdater mapperRefUpdater=new PhyPlanRefUpdater(mapPlan,new ArrayList(newPlan.getKeys().values()),PlanHelper.getLoads(mapPlan).get(0));
					mapperRefUpdater.visit();
					if(reducePlan!=null && !reducePlan.isEmpty()){
						PhyPlanRefUpdater reducerRefUpdater=new PhyPlanRefUpdater(reducePlan,new ArrayList(newPlan.getKeys().values()),PlanHelper.getLoads(mapPlan).get(0));
						reducerRefUpdater.visit();
					}
					//create a new MR plan out of this mapper plan
					MapReduceOper newMRPlan=new MapReduceOper(new OperatorKey(mKey.scope, NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)));
					if(newMRPlan!=null){
						newMRPlan.mapPlan=newPlan;
						subPlansToShare.add(newMRPlan);
						//newMRRootPlans.add(newMRPlan);
					}
				}
			}
			
			//create m/r plans for the new mapper root plans
			for(Pair<PhysicalPlan, PhysicalPlan> newPlanPair:newMapperRootPlans){
				PhysicalPlan newSplitterPlan=newPlanPair.first;
				PhysicalPlan newLoadStorePlan=newPlanPair.second;
				
				//create the MR splitter op  an add it to the MR plan
				MapReduceOper newMRPlan=new MapReduceOper(new OperatorKey(mKey.scope, NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)));
				if(newSplitterPlan!=null && !newSplitterPlan.isEmpty()){
					
					
					if(newMRPlan!=null){
						newMRPlan.mapPlan=newSplitterPlan;
						newMRPlan.setSplitter(true);
						newMRRootPlans.add(newMRPlan);
						
						//connect the m/r plans with this m/r plan
						mrplan.add(newMRPlan);
						mrplan.connect(newMRPlan, this);
					}
				}
				boolean isUseDiscovePlanHeuristics=conf.getBoolean(DISCOVER_NEWPLANS_HEURISTICS, false);	
				if(newSplitterPlan!=null && !newSplitterPlan.isEmpty() && newLoadStorePlan!=null && !newLoadStorePlan.isEmpty()){	
					//create the MR loadStore op and it it to the MR Plan
					MapReduceOper newMRLoadStore=new MapReduceOper(new OperatorKey(mKey.scope, NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)));
					if(newLoadStorePlan!=null && newMapperRootPlans.size()<=1 && isUseDiscovePlanHeuristics){
						newMRLoadStore.mapPlan=newLoadStorePlan;
						//newMRLoadStore.setSplitter(true);
						//newMRRootPlans.add(newMRLoadStore);
						
						//connect the m/r plans with this m/r plan
						mrplan.add(newMRLoadStore);
						mrplan.connect(newMRPlan, newMRLoadStore);
					}else{
						//replace the tmpStorage with the sharedStorage from newLoadStorePlan
						mapPlan.replaceTmpLoadWithSharedStorage(newSplitterPlan,newLoadStorePlan);
					}
					
				}
			}
			
		}
		
		//create m/r plans for the new mapper root plans
		/*for(Pair<PhysicalPlan, PhysicalPlan> newPlanPair:newMapperRootPlans){
			PhysicalPlan newSplitterPlan=newPlanPair.first;
			PhysicalPlan newLoadStorePlan=newPlanPair.second;
			
			if(newSplitterPlan!=null && !newSplitterPlan.isEmpty() && newLoadStorePlan!=null && !newLoadStorePlan.isEmpty()){
				//create the MR splitter op  an add it to the MR plan
				MapReduceOper newMRPlan=new MapReduceOper(new OperatorKey(mKey.scope, NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)));
				if(newMRPlan!=null){
					newMRPlan.mapPlan=newSplitterPlan;
					newMRPlan.setSplitter(true);
					//newMRRootPlans.add(newMRPlan);
				}
				//connect the m/r plans with this m/r plan
				mrplan.add(newMRPlan);newMapperRootPlans
				mrplan.connect(newMRPlan, this);
				
				//create the MR loadStore op and it it to the MR Plan
				MapReduceOper newMRLoadStore=new MapReduceOper(new OperatorKey(mKey.scope, NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)));
				if(newMRLoadStore!=null){
					newMRLoadStore.mapPlan=newLoadStorePlan;
					//newMRLoadStore.setSplitter(true);
					//newMRRootPlans.add(newMRLoadStore);
				}
				//connect the m/r plans with this m/r plan
				mrplan.add(newMRLoadStore);
				mrplan.connect(newMRPlan, newMRLoadStore);
			}
		}*/
		
		
		return subPlansToShare;
	}
  	
	public void discoverUsefulSubplans(PigContext pigContext, Configuration conf, MROperPlan mrplan) throws VisitorException, PlanException, CloneNotSupportedException{
		
		//get stores of this plan
		List<POStore> stores=null;
		if(!reducePlan.isEmpty()){
			stores=PlanHelper.getStores(reducePlan);
		}else if(!mapPlan.isEmpty()){
			stores=PlanHelper.getStores(mapPlan);
		}else{
			stores=new ArrayList<POStore>();
		}
		
		if(!mapPlan.isEmpty()){
			//List<POLoad> loads=PlanHelper.getLoads(mapPlan);
			
			for(;;){
				
				
				List<MapReduceOper> mrPreds = mrplan.getPredecessors(this);
				
				//List <MapReduceOper> newMRRootPlans=new ArrayList();
				List <Pair<PhysicalPlan, PhysicalPlan>> newMapperRootPlans=new ArrayList<Pair<PhysicalPlan, PhysicalPlan>>();
				mapPlan.discoverUsefulSubplans(pigContext, conf,stores,  newMapperRootPlans);
				
				//create m/r plans for the new mapper root plans
				for(Pair<PhysicalPlan, PhysicalPlan> newPlanPair:newMapperRootPlans){
					PhysicalPlan newSplitterPlan=newPlanPair.first;
					PhysicalPlan newLoadStorePlan=newPlanPair.second;
					
					if(newSplitterPlan!=null && !newSplitterPlan.isEmpty()){
						//create the MR splitter op  an add it to the MR plan
						MapReduceOper newMRPlan=new MapReduceOper(new OperatorKey(mKey.scope, NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)));
						if(newMRPlan!=null){
							newMRPlan.mapPlan=newSplitterPlan;
							newMRPlan.setSplitter(true);
							//newMRRootPlans.add(newMRPlan);
							
							//add the new m/r plan
							mrplan.add(newMRPlan);
							if(newLoadStorePlan==null){
								//List<MapReduceOper> mrPreds = mrplan.getPredecessors(this);
								for(MapReduceOper pred:mrPreds){
									mrplan.insertBetween(pred, newMRPlan, this);
								}
							}else{
								//List<MapReduceOper> mrPreds = mrplan.getPredecessors(this);
								if(mrPreds==null || mrPreds.isEmpty()){
									//connect the m/r plans with this m/r plan
									mrplan.connect(newMRPlan, this);
								}else{
									for(MapReduceOper pred:mrPreds){
										mrplan.insertBetween(pred, newMRPlan, this);
									}
								}
							}
						}
					}
					
					if(newSplitterPlan!=null && !newSplitterPlan.isEmpty() && newLoadStorePlan!=null && !newLoadStorePlan.isEmpty()){
						
						//create the MR loadStore op and it it to the MR Plan
						MapReduceOper newMRLoadStore=new MapReduceOper(new OperatorKey(mKey.scope, NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)));
						if(newLoadStorePlan!=null && newMapperRootPlans.size()<=1){
							//newMRLoadStore.mapPlan=newLoadStorePlan;
							//newMRLoadStore.setSplitter(true);
							//newMRRootPlans.add(newMRLoadStore);
							
							//connect the m/r plans with this m/r plan
							//mrplan.add(newMRLoadStore);
							//mrplan.connect(newMRPlan, newMRLoadStore);
							mapPlan.replaceTmpLoadWithSharedStorage(newSplitterPlan,newLoadStorePlan);
						}else{
							//replace the tmpStorage with the sharedStorage from newLoadStorePlan
							mapPlan.replaceTmpLoadWithSharedStorage(newSplitterPlan,newLoadStorePlan);
						}
						
					}
				}
				
				if(newMapperRootPlans.size()==0){
					break;
				}
			}
		}
	}

	public void discoverUsefulSubplansReducer(PigContext pigContext, Configuration conf, MROperPlan mrplan) throws VisitorException, PlanException, CloneNotSupportedException{
		
		//get stores of this plan
		List<POStore> stores=null;
		if(!reducePlan.isEmpty()){
			stores=PlanHelper.getStores(reducePlan);
		}else if(!mapPlan.isEmpty()){
			stores=PlanHelper.getStores(mapPlan);
		}else{
			stores=new ArrayList<POStore>();
		}
		
		if(!this.reducePlan.isEmpty()){
			
			//for(;;){
				List<MapReduceOper> mrSuccs = mrplan.getSuccessors(this);
				List <PhysicalPlan> newMapperRootPlans=new ArrayList<PhysicalPlan>();
				reducePlan.discoverUsefulSubplansReducer(pigContext, conf,stores,  newMapperRootPlans);
				
				//create m/r plans for the new mapper root plans
				for(PhysicalPlan newSplitterPlan:newMapperRootPlans){
					
					
					if(newSplitterPlan!=null && !newSplitterPlan.isEmpty()){
						//create the MR splitter op  an add it to the MR plan
						MapReduceOper newMRPlan=new MapReduceOper(new OperatorKey(mKey.scope, NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)));
						if(newMRPlan!=null){
							newMRPlan.mapPlan=newSplitterPlan;
							/*newMRPlan.reducePlan=newSplitterPlan;
							List<PhysicalOperator> loads = new ArrayList<PhysicalOperator>(newMRPlan.reducePlan.getRoots());
							if(loads!=null){
								for(PhysicalOperator op:loads){
									newMRPlan.mapPlan.add(op);
									newMRPlan.reducePlan.remove(op);
								}
							}*/
							//newMRPlan.setSplitter(true);
							//newMRRootPlans.add(newMRPlan);
							
							//add the new m/r plan
							mrplan.add(newMRPlan);
							
							if(mrSuccs==null || mrSuccs.isEmpty()){
								//connect the m/r plans with this m/r plan
								mrplan.connect(this, newMRPlan);
							}else{
								for(MapReduceOper succ:mrSuccs){
									mrplan.insertBetween(this, newMRPlan, succ);
								}
							}
						}
					}
				}
				//if(newMapperRootPlans.size()==0){
					//break;
				//}
			//}
		}
	}
	/**
  	 * discover sub plans to share by adding a split and store tmp data after a filter or foreach
  	 * that makes substantial change
  	 * @pre note that this plan is after making any possible rewriting, so discovering other possible
  	 * shared plans will not overlap with any existing shared plans!
  	 * @return
  	 * @author iman
  	 * @param plan 
  	 * @throws CloneNotSupportedException 
  	 * @throws PlanException 
  	 * @throws VisitorException 
  	 */
	public Vector<MapReduceOper> discoverUsefulSubplans(PigContext pigContext) throws VisitorException, PlanException, CloneNotSupportedException {
		Vector<MapReduceOper> subPlansToShare=new Vector<MapReduceOper>();
		//get stores of this plan
		List<POStore> stores=null;
		if(!reducePlan.isEmpty()){
			stores=PlanHelper.getStores(reducePlan);
		}else if(!mapPlan.isEmpty()){
			stores=PlanHelper.getStores(mapPlan);
		}else{
			stores=new ArrayList<POStore>();
		}
		//get the mapper
		if(!mapPlan.isEmpty()){
			Vector<PhysicalPlan> subMapPlansToShare = mapPlan.discoverUsefulSubplans(pigContext,stores);
			//create MR plan for every discovered subplan
			for(PhysicalPlan newPlan:subMapPlansToShare){
				if(newPlan!=null&&!newPlan.isEmpty()){
					MapReduceOper newMRPlan=new MapReduceOper(new OperatorKey(mKey.scope, NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)));
					if(newMRPlan!=null){
						newMRPlan.mapPlan=newPlan;
						subPlansToShare.add(newMRPlan);
					}
				}
			}
		}
		return subPlansToShare;
	}

	public boolean hasTempStore() {
		return false;
	}

	public Pair<String,String> replaceTempStore() throws VisitorException {
		List<POStore> stores=null;
		if(!reducePlan.isEmpty()){
			stores=PlanHelper.getStores(reducePlan);
		}else if(!mapPlan.isEmpty()){
			stores=PlanHelper.getStores(mapPlan);
		}
		
		POStore tmpStoreToReplace=null;
		for(POStore store:stores){
			if(store.isTmpStore()){
				tmpStoreToReplace=store;
				break;
			}
		}
		if(tmpStoreToReplace!=null){
			//create a shared storage location
			String newSharedStorageLocation=PhysicalPlan.getNewSharedStorageLocation();
			//replace the tmp store location with the shared one
			FileSpec sfile = tmpStoreToReplace.getSFile();
			String oldTmpFileName=sfile.getFileName();
			sfile.setFileName(newSharedStorageLocation);
			//set the store to be not-tmp
			tmpStoreToReplace.setIsTmpStore(false);
			//return the new store location
			Pair<String, String> replaceInfo=new Pair<String, String>(oldTmpFileName,newSharedStorageLocation);
			
			return replaceInfo;
		}
		return null;
	}
  	
  	
}
