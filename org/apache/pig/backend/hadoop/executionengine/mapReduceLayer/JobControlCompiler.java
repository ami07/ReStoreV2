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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.RunJar;
import org.apache.pig.ComparisonFunc;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.SecondaryKeyPartitioner;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.SkewedPartitioner;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.WeightedRangePartitioner;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MRPrinter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeCogroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.NullableBytesWritable;
import org.apache.pig.impl.io.NullableDoubleWritable;
import org.apache.pig.impl.io.NullableFloatWritable;
import org.apache.pig.impl.io.NullableIntWritable;
import org.apache.pig.impl.io.NullableLongWritable;
import org.apache.pig.impl.io.NullablePartitionWritable;
import org.apache.pig.impl.io.NullableText;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.UriUtil;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStatsUtil;
import org.apache.pig.tools.pigstats.ScriptState;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;


/**
 * This is compiler class that takes an MROperPlan and converts
 * it into a JobControl object with the relevant dependency info
 * maintained. The JobControl Object is made up of Jobs each of
 * which has a JobConf. The MapReduceOper corresponds to a Job
 * and the getJobCong method returns the JobConf that is configured
 * as per the MapReduceOper
 *
 * <h2>Comparator Design</h2>
 * <p>
 * A few words on how comparators are chosen.  In almost all cases we use raw
 * comparators (the one exception being when the user provides a comparison
 * function for order by).  For order by queries the PigTYPERawComparator
 * functions are used, where TYPE is Int, Long, etc.  These comparators are
 * null aware and asc/desc aware.  The first byte of each of the
 * NullableTYPEWritable classes contains info on whether the value is null.
 * Asc/desc is written as an array into the JobConf with the key pig.sortOrder
 * so that it can be read by each of the comparators as part of their 
 * setConf call.
 * <p>
 * For non-order by queries, PigTYPEWritableComparator classes are used.
 * These are all just type specific instances of WritableComparator.
 *
 */
public class JobControlCompiler{
    MROperPlan plan;
    Configuration conf;
    PigContext pigContext;
    long jobIDTag=-1;
    
    private static final Log log = LogFactory.getLog(JobControlCompiler.class);
    
    public static final String LOG_DIR = "_logs";

    public static final String END_OF_INP_IN_MAP = "pig.invoke.close.in.map";
    
    //@iman 
    public static final String TMPFILES_JOB_DFN = "tmpfiles.job.dfn";
    
    /**
     * We will serialize the POStore(s) present in map and reduce in lists in
     * the Hadoop Conf. In the case of Multi stores, we could deduce these from
     * the map plan and reduce plan but in the case of single store, we remove
     * the POStore from the plan - in either case, we serialize the POStore(s)
     * so that PigOutputFormat and PigOutputCommiter can get the POStore(s) in
     * the same way irrespective of whether it is multi store or single store.
     */
    public static final String PIG_MAP_STORES = "pig.map.stores";
    public static final String PIG_REDUCE_STORES = "pig.reduce.stores";
    
    //@iman
    public static final String DEBUGINFO_DETAILED = "debuginfo.printplans";
    public static final String DISCOVER_NEWPLANS = "sharing.discoverPlans";
    private static final String OPTIMIZE_BY_SHARING_PLANS = "sharing.plan.reuseoutputs";
    public static final String DISCOVER_NEWPLANS_HEURISTICS = "sharing.useHeuristics.discoverPlans";
    public static final String DISCOVER_NEWPLANS_HEURISTICS_REDUCER = "sharing.useHeuristics.discoverPlans.reducer";
    
    // A mapping of job to pair of store locations and tmp locations for that job
    private Map<Job, Pair<List<POStore>, Path>> jobStoreMap;
    
    private Map<Job, MapReduceOper> jobMroMap;
    
    //@iman: list of plans to be shared
    //@amiTOD private Set<SharedMapReducePlan> sharedPlans;
    private Vector<SharedMapReducePlan> sharedPlans;
    private Map<String,Pair<SharedMapReducePlan,Vector<SharedMapReducePlan>>> candidatePlansToShare;
	private boolean isOptimizeBySharing;
	private boolean isOptimizeBySharingWholePlan;
	private boolean isMoreDebugInfo;
	private boolean isDiscoverNewPlansOn;
	private boolean isUseDiscovePlanHeuristics;
	private boolean isUseDiscovePlanHeuristicsReducer;
    private SharedMapReducePlan dependingCandidateSharedPlan=null;
	private boolean firstIter;
	private Map<Job,Pair<String,String>>  jobSharedStoreMap;
    
    public JobControlCompiler(PigContext pigContext, Configuration conf, boolean isOptimizeBySharing) throws IOException {
        this.pigContext = pigContext;
        this.conf = conf;
        jobStoreMap = new HashMap<Job, Pair<List<POStore>, Path>>();
        jobSharedStoreMap = new HashMap<Job, Pair<String,String>>();
        jobMroMap = new HashMap<Job, MapReduceOper>();
        //@iman:initialize the struct containing the set of existing materialized views
        candidatePlansToShare=new HashMap<String,Pair<SharedMapReducePlan,Vector<SharedMapReducePlan>>>();
        this.isOptimizeBySharing=isOptimizeBySharing;
        this.isOptimizeBySharingWholePlan=conf.getBoolean(OPTIMIZE_BY_SHARING_PLANS, false);
        this.isMoreDebugInfo=conf.getBoolean(DEBUGINFO_DETAILED, false);
        this.isDiscoverNewPlansOn=conf.getBoolean(DISCOVER_NEWPLANS, false);
        this.isUseDiscovePlanHeuristics=conf.getBoolean(DISCOVER_NEWPLANS_HEURISTICS, false);
        this.isUseDiscovePlanHeuristicsReducer=conf.getBoolean(DISCOVER_NEWPLANS_HEURISTICS_REDUCER, false);
        
        //@iman:load the m/r views
        if(this.isOptimizeBySharing){
	        try {
				loadJobDfnForTmpFiles();
			} catch (ClassNotFoundException e) {
				log.error("Unable to deserialize the shared plans set", e);
			}catch (IOException e) {
				log.error("Unable to open the stored shared plans listing", e);
			}
        }
        //@iman indicates that the next iteration by compile is the first and it will do the leaves of the workflow
        firstIter=true;
    }

    /**
     * Load the table with jobs and their tmp file
     * @throws IOException 
     * @throws ClassNotFoundException 
	 * @author iman
     */
    private void loadJobDfnForTmpFiles() throws IOException, ClassNotFoundException {
    	if(!this.isOptimizeBySharing){
    		return;
    	}
		String tmpFileJobDefnLocation = conf.get(TMPFILES_JOB_DFN);
		if(tmpFileJobDefnLocation==null){
			tmpFileJobDefnLocation="/home/iman/sharedPigPlans.plans";
		}
		//load the job definitions
		File file=new File(tmpFileJobDefnLocation);
		if(file.exists()){
			FileInputStream fis = new FileInputStream(file);
	        ObjectInputStream ois = new ObjectInputStream(fis);
	        sharedPlans=(Vector<SharedMapReducePlan>) ois.readObject();
	        ois.close();
		}else{
			//create a new empty hashset
			sharedPlans = new Vector<SharedMapReducePlan>();
		}
	}
	
    /**
     * Returns all store locations of a previously compiled job
     */
    public List<POStore> getStores(Job job) {
        Pair<List<POStore>, Path> pair = jobStoreMap.get(job);
        if (pair != null && pair.first != null) {
            return pair.first;
        } else {
            return new ArrayList<POStore>();
        }
    }

    /**
     * Resets the state
     */
    public void reset() {
        jobStoreMap = new HashMap<Job, Pair<List<POStore>, Path>>();
        jobSharedStoreMap = new HashMap<Job, Pair<String,String>>();
        jobMroMap = new HashMap<Job, MapReduceOper>();
        UDFContext.getUDFContext().reset();
        firstIter=true;
    }

    /**
     * dump shared plans discovered in this query
	 * @author iman
     */
    public void dumpSharedPlans(){
    	if(!this.isOptimizeBySharing){
    		return;
    	}
    	//print the existing plans
    	if(this.isMoreDebugInfo){
	    	System.out.println("shared plans: ");
	    	for(SharedMapReducePlan sharedPlan: sharedPlans){
	    		System.out.println("shared plan: ");
	    		PrintStream ps=System.out;
	    		boolean verbose = true;
	            ps.println("#--------------------------------------------------");
	            ps.println("# shared plan:                             ");
	            ps.println("#--------------------------------------------------");
	            MROperPlan sharedMRPlan= new MROperPlan();
	            sharedMRPlan.add(sharedPlan.getMRPlan());
	            MRPrinter printer = new MRPrinter(ps, sharedMRPlan);
	            printer.setVerbose(verbose);
	            try {
					printer.visit();
				} catch (VisitorException e) {
					log.warn("Unable to print shared plan", e);
				}
				ps.println("plan stats:");
				ps.println("bytes read: " + sharedPlan.getHdfsBytesRead()+" bytes written: "+sharedPlan.getHdfsBytesWritten()+" read/write= "+sharedPlan.getReadWriteRatio());
				ps.println("avg time taken by this job "+sharedPlan.getAvgPlanTime());
				System.out.println();
	    	}
    	}
    	
    	String tmpFileJobDefnLocation = conf.get(TMPFILES_JOB_DFN);
    	if(tmpFileJobDefnLocation==null){
			tmpFileJobDefnLocation="/home/iman/sharedPigPlans.plans";
		}
		//load the job definitions
		File file=new File(tmpFileJobDefnLocation);
		FileOutputStream fos;
		ObjectOutputStream oos;
		if(file.exists()){
			//replace the object that already exist in the stream
			try {
				file.createNewFile();
				fos = new FileOutputStream(file);
				oos = new ObjectOutputStream(fos);
				oos.writeObject(sharedPlans);
				oos.close();
			} catch (FileNotFoundException e) {
				log.error("Could not create/find the file of shared plans", e);
			} catch (IOException e) {
				log.error("Unable to write/serialize shared plans set", e);
			}
		}else{
			//create a new file and write the shared plans to it
			
			try {
				file.createNewFile();
				fos = new FileOutputStream(file);
				oos = new ObjectOutputStream(fos);
				oos.writeObject(sharedPlans);
				oos.close();
			} catch (FileNotFoundException e) {
				log.error("Could not create/find the file of shared plans", e);
			} catch (IOException e) {
				log.error("Unable to write/serialize shared plans set", e);
			}
	        

		}
    }
    
    Map<Job, MapReduceOper> getJobMroMap() {
        return Collections.unmodifiableMap(jobMroMap);
    }
    
    public void copyResultsToSharedLocation(List<Job> completedJobs) throws IOException{
    	for (Job job: completedJobs) {
    		Pair<String, String> locationCopyInfo=jobSharedStoreMap.get(job);
    		if(locationCopyInfo!=null){
    			String oldLocation=locationCopyInfo.first;
    			String newLocation=locationCopyInfo.second;
    			//cory from the temp loc to the shared one
    			copyResultLocations(oldLocation,newLocation);
    		}
    	}
    }
    /**
     * Moves all the results of a collection of MR jobs to the final
     * output directory. Some of the results may have been put into a
     * temp location to work around restrictions with multiple output
     * from a single map reduce job.
     *
     * This method should always be called after the job execution
     * completes.
     */
    public void moveResults(List<Job> completedJobs) throws IOException {
        for (Job job: completedJobs) {
            Pair<List<POStore>, Path> pair = jobStoreMap.get(job);
            if (pair != null && pair.second != null) {
                Path tmp = pair.second;
                Path abs = new Path(tmp, "abs");
                Path rel = new Path(tmp, "rel");
                FileSystem fs = tmp.getFileSystem(conf);

                if (fs.exists(abs)) {
                    moveResults(abs, abs.toUri().getPath(), fs);
                }
                
                if (fs.exists(rel)) {        
                    moveResults(rel, rel.toUri().getPath()+"/", fs);
                }
            }
        }
    }

    /**
     * Walks the temporary directory structure to move (rename) files
     * to their final location.
     */
    private void moveResults(Path p, String rem, FileSystem fs) throws IOException {
        for (FileStatus fstat: fs.listStatus(p)) {
            Path src = fstat.getPath();
            if (fstat.isDir()) {
                log.info("mkdir: "+src);
                fs.mkdirs(removePart(src, rem));
                moveResults(fstat.getPath(), rem, fs);
            } else {
                Path dst = removePart(src, rem);
                log.info("mv: "+src+" "+dst);
                fs.rename(src,dst);
            }
        }
    }

    private Path removePart(Path src, String part) {
        URI uri = src.toUri();
        String pathStr = uri.getPath().replace(part, "");
        return new Path(pathStr);
    }
    
    
    /**
     * Compiles all jobs that have no dependencies removes them from
     * the plan and returns. Should be called with the same plan until
     * exhausted. 
     * @param plan - The MROperPlan to be compiled
     * @param grpName - The name given to the JobControl
     * @return JobControl object - null if no more jobs in plan
     * @throws JobCreationException
     */
    public JobControl compile(MROperPlan plan, String grpName) throws JobCreationException{
        // Assert plan.size() != 0
        this.plan = plan;

        JobControl jobCtrl = new JobControl(grpName);
        

        try {
            List<MapReduceOper> roots = new LinkedList<MapReduceOper>();
            roots.addAll(plan.getRoots());
            for (MapReduceOper mro: roots) {
                if(mro instanceof NativeMapReduceOper) {
                    return null;
                }
                
                //@iman
                boolean noExecForMRO=false;
            	MapReduceOper mroToShare=mro.clone();
            	long sharedPlanBytesRead=0;
				long sharedPlanAvgTime=0;
				JobStats jsForMRO=PigStatsUtil.getJobForMRO(mro);
            	//SharedMapReducePlan bestSharedPlanSoFar=null;
            	//check plan equivalence with shared plans
				if(this.isOptimizeBySharing){
	            	for(SharedMapReducePlan sharedPlan: sharedPlans){
	            		MapReduceOper sharedMRPlan=sharedPlan.getMRPlan();
	            		MapReduceOper planReplacedWithView=null;
	            		if(sharedMRPlan.isEquivalent(mro)){
	        				System.out.println("Found an equivalent view to this plan");
	        				//get the successor MR plan for mro
	        				List<MapReduceOper> successorMROs = this.plan.getSuccessors(mro);
	        				if(successorMROs==null|| successorMROs.size()<=0){
	        					//there are no successors to this plan, we only need to have a special condition to copy the 
	        					//o/p to the new location and terminate
	        					//TODO
	        					log.info("Found an exact match view");
	        					//check if the output of this job currently exist
	        					
	        				}else{
		        				//update successor MR plans with the new load replacing the old tmp one
		        				for (MapReduceOper successorMRO: successorMROs) {
		        					successorMRO.updateLoadOperator(mro,sharedMRPlan);
		        				}
	        				}
	        				//@iman remove this plan from the list of plans
	        				//plan.trimBelow(mro);
	        	            this.plan.remove(mro);
	        	            
	        				noExecForMRO=true;
	        				//@iman set the first iter to be false as at least one iter has been performed
	        		        firstIter=false;
	        				//bestSharedPlanSoFar=null; //make sure it is null, so we will not execute that parts that handles shared plans again
	        				break;
	        			}else{
	        				//the plan is not equivalent with the view, check if it is subset of it
	        				planReplacedWithView=mro.getPlanRecplacedWithView(sharedMRPlan,pigContext);
	        				if(planReplacedWithView!=null ){//&& (bestSharedPlanSoFar==null || sharedPlan.isBetterPlan(bestSharedPlanSoFar))
	        					System.out.println("Found an view that is a subset of this plan");
	        					//get the successor MR plan for mro
	            				List<MapReduceOper> successorMROs = this.plan.getSuccessors(mro);
	            				if(planReplacedWithView==sharedMRPlan){
	            					//the new plan is the same as the shared plan after changing the store location
	            					if(successorMROs==null|| successorMROs.size()<=0){
	            						// only copy the files in the old store location to the new one
	            						log.info("To copy the files from the old location to the new one");
	            						POStore sharedPlanStoreLoc=sharedMRPlan.getStore(sharedMRPlan);//getPlanFinalStoreLocation(sharedMRPlan);
	            						POStore newPlanStoreLoc=mro.getStore(mro);//getPlanFinalStoreLocation(mro);
	            						copyResultLocations(sharedPlanStoreLoc,newPlanStoreLoc);
	            						//@iman remove this plan from the list of plans
	                    				//plan.trimBelow(mro);
	                    	            this.plan.remove(mro);
	            						//set flag that we will not exec mro
		        						noExecForMRO=true;
		        						//@iman set the first iter to be false as at least one iter has been performed
		        				        firstIter=false;
	            					}else{
	            						log.info("To update successor MR plans by merging the new mro into their maps");
		    	        				//update successor MR plans by merging the new mro into their maps
		    	        				for (int i=0;i<successorMROs.size();i++) {
		    	        					MapReduceOper successorMRO= successorMROs.get(i);
		    	        					//TODO replace the load operator with the location of store operator from shared plan
		    	        					if(successorMRO.updateLoadOperator(mro,sharedMRPlan)==true){
		    	        						//@iman remove this plan from the list of plans
			                    				//plan.trimBelow(mro);
			                    	            this.plan.remove(mro);
		    	        						//set flag that we will not exec mro
		    	        						noExecForMRO=true;
		    	        						//@iman set the first iter to be false as at least one iter has been performed
		    	        				        firstIter=false;
		    	        					}
		    	        				}
		    	        				
		            				}
	            					break;
	            				}else{
	            					log.info("the differences in the shared plan and the new plan are more than store location ");
	            					log.info("the shared plan is subsumed in this plan");
	            					log.info("therefore, we replace the last shared op with a load from the store loc of the shared plan");
	            					log.info("the new rewritten plan is executed and nothing will be changed for subsecuent MR plans");
	            					//store info about sharedPlan bytes read and execution time
	            					sharedPlanBytesRead=sharedPlan.getHdfsBytesRead();
	            					sharedPlanAvgTime=sharedPlan.getAvgPlanTime();
	            					//the differences in the shared plan and the new plan are more than store location
	            					
		            				/*if(successorMROs==null|| successorMROs.size()<=0){
		            					//mro will remain as it is, except that we will need to adjust the operators
		            					//to move them to the map instead of reduce plan
		            					//TODO
		            				}else{
		    	        				//update successor MR plans by merging the new mro into their maps
		    	        				for (MapReduceOper successorMRO: successorMROs) {
		    	        					if(successorMRO.updateLoadOperator(mro,sharedMRPlan)==true){
		    	        						//set flag that we will not exec mro
		    	        						//TODO look at this and make sure that this is intended to be that way!
		    	        						//noExecForMRO=true;
		    	        					}
		    	        				}
		    	        				
		            				}*/
	            				}
	            				//print the shared plan that is used for rewritting the query
	            				if(this.isMoreDebugInfo){
		            				boolean verbose = true;
		                    		PrintStream ps=System.out;
		                            ps.println("#--------------------------------------------------");
		                            ps.println("# The plan that we will use to rewrite the query:  ");
		                            ps.println("#--------------------------------------------------");
		                            MROperPlan execPlan= new MROperPlan();
		                            execPlan.add(sharedMRPlan);
		                            MRPrinter printer = new MRPrinter(ps, execPlan);
		                            printer.setVerbose(verbose);
		                            try {
		                				printer.visit();
		                			} catch (VisitorException e) {
		                				log.warn("Unable to print shared plan", e);
		                			}
	            				}
		        				//break;
	        				}//end if the shared plan is subset of the plan i am currently executing
	        			}
	            	}//end for every shared plan
				}
    			
            	//if(bestSharedPlanSoFar!=null){
            		
            	//}
            	
            	if(!noExecForMRO){
	            	//ami: the current plan did not match with any of the existing shared plans
            		//add the plan to the jobs to be executed, and
            		//add this plan as a candidate to be shared in the future
	                //String jobID=jobCtrl.addJob(getJob(mro, conf, pigContext));
            		
            		
            		if(isMoreDebugInfo){
            			boolean verbose = true;
                		PrintStream ps=System.out;
	                    ps.println("#--------------------------------------------------");
	                    ps.println("# To run a job for plan:                           ");
	                    ps.println("#--------------------------------------------------");
	                    MROperPlan execPlan= new MROperPlan();
	                    execPlan.add(mro);
	                    MRPrinter printer = new MRPrinter(ps, execPlan);
	                    printer.setVerbose(verbose);
	                    try {
	        				printer.visit();
	        			} catch (VisitorException e) {
	        				log.warn("Unable to print job plan", e);
	        			}
            		}
            		
            		Vector<SharedMapReducePlan> candidateSubPlansToShare=new Vector<SharedMapReducePlan>();
            		List <MapReduceOper> newMRRootPlans=new ArrayList<MapReduceOper>();
            		List<MapReduceOper> subPlansToShare=new ArrayList<MapReduceOper>();
            		if(isDiscoverNewPlansOn){
	            		//@iman discover new potential useful subplans that we can share
	        			subPlansToShare=mro.discoverUsefulSubplans(pigContext, conf, this.plan,newMRRootPlans);
	        			//candidateSubPlansToShare=new Vector<SharedMapReducePlan>(subPlansToShare.size());
	        			/*for(MapReduceOper subPlan:subPlansToShare){
	        				//get the POStore of the discovered shared plan
	        				List<POStore> planStores = PlanHelper.getStores(subPlan.mapPlan);
	        				POStore planStore=planStores.get(0);
	        				String storeLocation=planStore.getSFile().getFileName();
	        				log.debug("To create a shared plan for the tmp storage "+storeLocation);
	        				SharedMapReducePlan candidateSharedsubPlan=new SharedMapReducePlan(subPlan,planStore,storeLocation);
	        				candidateSubPlansToShare.add(candidateSharedsubPlan);
	        				
	        			}*/
            		}
        			
        			if(isMoreDebugInfo){
        				boolean verbose = true;
                		PrintStream ps=System.out;
	        			ps.println("#--------------------------------------------------");
	                    ps.println("# Job plan after inserting extra stores:                ");
	                    ps.println("#--------------------------------------------------");
	                    MROperPlan execPlan2= new MROperPlan();
	                    execPlan2.add(mro);
	                    MRPrinter printer2 = new MRPrinter(ps, this.plan);
	                    printer2.setVerbose(verbose);
	                    try {
	        				printer2.visit();
	        			} catch (VisitorException e) {
	        				log.warn("Unable to print job plan", e);
	        			}
        			}
        			
        			//String jobID="";
        			if(subPlansToShare.isEmpty()){
        				
        				if(isMoreDebugInfo){
            				boolean verbose = true;
                    		PrintStream ps=System.out;
    	        			ps.println("#--------------------------------------------------");
    	        			ps.println("# No Discoverd Plans to share                      ");
    	                    ps.println("# The mapreduce operator to be exec                ");
    	                    ps.println("#--------------------------------------------------");
    	                    MROperPlan execPlan2= new MROperPlan();
    	                    execPlan2.add(mro);
    	                    MRPrinter printer2 = new MRPrinter(ps, execPlan2);
    	                    printer2.setVerbose(verbose);
    	                    try {
    	        				printer2.visit();
    	        			} catch (VisitorException e) {
    	        				log.warn("Unable to print job plan", e);
    	        			}
            			}
        				
	            		Job job = getJob(mro, conf, pigContext);
	                    jobMroMap.put(job, mro);
	                    String jobID=jobCtrl.addJob(job);
	                    jobIDTag++;
	                    jobID+="_"+jobIDTag;
	                    //update jobID of the job
	                    job.setJobID(jobID);
	                    
	                    if(isOptimizeBySharing && isOptimizeBySharingWholePlan && (firstIter || ! isUseDiscovePlanHeuristics) && !hasFinalStore(mroToShare)){
			                //ami: add mro to candidate plans to share
			                //SharedMapReducePlan candidateSharedPlan=new SharedMapReducePlan(mroToShare,null,null);
			                //sharedPlans.add(candidateSharedPlan);
			                //candidatePlansToShare.put(jobID, mro);
			                SharedMapReducePlan candidateSharedPlan=null;
			                if(dependingCandidateSharedPlan!=null && hasSameStoreLoc(dependingCandidateSharedPlan.getMRPlan(), mro)){
			                	candidateSharedPlan=new SharedMapReducePlan(dependingCandidateSharedPlan);
			                	dependingCandidateSharedPlan=null;
			                	//get the shared plan
			                	MapReduceOper planToShare = candidateSharedPlan.getMRPlan();
			                	//replace mrToShare temp store with a shared store
			                	Pair<String,String> newStoreLoc=planToShare.replaceTempStore();
			                	if(newStoreLoc!=null){
			                		//keep info about the job, new shared store location
			                		jobSharedStoreMap.put(job,newStoreLoc);
			                	}
			                }else{
			                	//replace mrToShare temp store with a shared store
		                		Pair<String,String> newStoreLocPair=mroToShare.replaceTempStore();
			                	if(newStoreLocPair!=null){
			                		//keep info about the job, new shared store location
			                		jobSharedStoreMap.put(job,newStoreLocPair);
			                	}
				                candidateSharedPlan=new SharedMapReducePlan(mroToShare,null,null);
				                //update the new shared plan original input bytes and time based on the shared plan info
				                candidateSharedPlan.setHdfsBytesRead(sharedPlanBytesRead);
				                candidateSharedPlan.setAvgPlanTime(sharedPlanAvgTime);
				                candidateSharedPlan.setDiscoveredPlan(false);
			                }
			                //create a pair for the main plan and its proposed subplans
			                Pair<SharedMapReducePlan,Vector<SharedMapReducePlan>> planAndSubPlans=new Pair<SharedMapReducePlan,Vector<SharedMapReducePlan>>(candidateSharedPlan,candidateSubPlansToShare);
			                candidatePlansToShare.put(jobID,planAndSubPlans);
	                    }
	                    
	                    //@iman set the first iter to be false as at least one iter has been performed
	                    firstIter=false;
        			}else{
        				//handle the case with discovered new plans
        				//merge the new plan with other existing plans
        				/*for(MapReduceOper newRootPlan:newMRRootPlans){
        					//create a MROperPlan from the newRootPlan
        					MROperPlan newOperPlan=new MROperPlan();
        					newOperPlan.add(newRootPlan);
        					plan.mergeSharedPlan(newOperPlan);
        				}*/
        				//compile the new MR plan for the query
        				boolean isMultiQuery = 
        		            "true".equalsIgnoreCase(pigContext.getProperties().getProperty("opt.multiquery","true"));
        		        int planSizeBeforeOpt=this.plan.size();
        		        if (isMultiQuery && !this.isUseDiscovePlanHeuristicsReducer) {
        		            // reduces the number of MROpers in the MR plan generated 
        		            // by multi-query (multi-store) script.
        		            MultiQueryOptimizer mqOptimizer = new MultiQueryOptimizer(this.plan);
        		            mqOptimizer.visit();
        		            // removes unnecessary stores (as can happen with splits in
        		            // some cases.). This has to run after the MultiQuery and
        		            // NoopFilterRemover.
        		            NoopStoreRemover sRem = new NoopStoreRemover(this.plan);
        		            sRem.visit();
        		        }
        		        int planSizeAfterOpt=this.plan.size();
        		        //print mro after optimization
        		        if(isMoreDebugInfo){
            				boolean verbose = true;
                    		PrintStream ps=System.out;
    	        			ps.println("#--------------------------------------------------");
    	                    ps.println("# MRO after inserting extra stores and then multiquery opt of splits!");
    	                    ps.println("#--------------------------------------------------");
    	                    //MROperPlan execPlan2= new MROperPlan();
    	                    //execPlan2.add(mro);
    	                    MRPrinter printer = new MRPrinter(ps, this.plan);
    	                    //MRPrinter printer2 = new MRPrinter(ps, execPlan2);
    	                    printer.setVerbose(verbose);
    	                    try {
    	        				printer.visit();
    	        			} catch (VisitorException e) {
    	        				log.warn("Unable to print job plan", e);
    	        			}
            			}
        		        
        		        //if the MultiQueryOptimizer reduced the plan size
        		        if(planSizeAfterOpt<planSizeBeforeOpt){
        		        	//the discovered MR plans have been merged with the pred mro plan
        		        	if(planSizeBeforeOpt-planSizeAfterOpt>=subPlansToShare.size()){
        		        		//all discovered plans where merged
        		        		
        		        		//find the new discovered plan
        		        		List<MapReduceOper> newRoots = new LinkedList<MapReduceOper>();
        		        		newRoots.addAll(plan.getRoots());
        		        		
        		        		MapReduceOper mergedmro=mro;
        		        		
        		        		for (MapReduceOper newmro: newRoots) {
        		        			if(!roots.contains(newmro)){
        		        				//this the new mro that was merged in the previous step
        		        				mergedmro=newmro;
        		        				break;
        		        			}
        		        		}
        		                
        		        		if(isMoreDebugInfo){
                    				boolean verbose = true;
                            		PrintStream ps=System.out;
                            		
                            		ps.println("#--------------------------------------------------");
            	        			ps.println("#Discoverd Plans are merged with the mro           ");
            	                    ps.println("# The mapreduce operator mro that might have changed because of optimization");
            	                    ps.println("#--------------------------------------------------");
            	                    MROperPlan execPlan3= new MROperPlan();
            	                    execPlan3.add(mro);
            	                    MRPrinter printer3 = new MRPrinter(ps, execPlan3);
            	                    printer3.setVerbose(verbose);
            	                    try {
            	        				printer3.visit();
            	        			} catch (VisitorException e) {
            	        				log.warn("Unable to print job plan", e);
            	        			}
            	        			
            	        			
            	        			ps.println("#--------------------------------------------------");
            	        			ps.println("#Discoverd Plans are merged with the mro           ");
            	                    ps.println("# The mapreduce operator to be exec                ");
            	                    ps.println("#--------------------------------------------------");
            	                    MROperPlan execPlan2= new MROperPlan();
            	                    execPlan2.add(mergedmro);
            	                    MRPrinter printer2 = new MRPrinter(ps, execPlan2);
            	                    printer2.setVerbose(verbose);
            	                    try {
            	        				printer2.visit();
            	        			} catch (VisitorException e) {
            	        				log.warn("Unable to print job plan", e);
            	        			}
                    			}
        		        		
        		        		//clone the new mro with the discovered plans merged to it
        		        		//mroToShare=mergedmro.clone();
        		        		
        		        		//update the pig stats utility for new merged job
            					// start collecting statistics
        		        		if(mergedmro!=mro){
        		        			//find the successors of the new mergedmro
        		        			List<MapReduceOper> mroSuccs = this.plan.getSuccessors(mergedmro);
        		        			//PigStatsUtil.updateCollection(this, mergedmro, mroSuccs);
        		        			PigStatsUtil.updateCollection(this, mro, mergedmro, mroSuccs);
        		        			/*if(!this.plan.contains(mro)){
        		        				log.debug("the mro operator does not exist any more in the plan, so remove it from stats");
        		        				//if mro is no longer part of the MR plan, then delete it
	        		        			if(jsForMRO!=null){
	        		        				PigStatsUtil.updateCollectionRemoveJob(this, jsForMRO);
	        		        			}
        		        			}else{
        		        				log.debug("the mro operator is still in the plan, so keep collecting stats for it");
        		        			}*/
        		        		}
            			        
            			        
        		        		//run the job for the new mro
        		        		Job job = getJob(mergedmro, conf, pigContext);
        	                    jobMroMap.put(job, mergedmro);
        	                    String jobID=jobCtrl.addJob(job);
        	                    jobIDTag++;
        	                    jobID+="_"+jobIDTag;
        	                    //update jobID of the job
        	                    job.setJobID(jobID);
        		        		
        	                    
        	                    candidateSubPlansToShare=new Vector<SharedMapReducePlan>(subPlansToShare.size());
        	        			for(MapReduceOper subPlan:subPlansToShare){
        	        				//get the POStore of the discovered shared plan
        	        				List<POStore> planStores = PlanHelper.getStores(subPlan.mapPlan);
        	        				POStore planStore=planStores.get(0);
        	        				String storeLocation=planStore.getSFile().getFileName();
        	        				log.debug("To create a shared plan for the tmp storage "+storeLocation);
        	        				SharedMapReducePlan candidateSharedsubPlan=new SharedMapReducePlan(subPlan,planStore,storeLocation);
        	        				candidateSharedsubPlan.setDiscoveredPlan(true);
        	        				candidateSubPlansToShare.add(candidateSharedsubPlan);
        	        				
        	        			}
        	                    
        	        			Pair<String,String> newStoreLocPair=mroToShare.replaceTempStore();
			                	if(newStoreLocPair!=null){
			                		//because mroToShare is different from the actually executed mergedmro
			                		/*POStore newPlanStore=mergedmro.getStore(mergedmro);
			                		if(newPlanStore!=null){
			                			String jobPlanStoreLoc=newPlanStore.getSFile().getFileName();
			                			if(!jobPlanStoreLoc.equals(newStoreLocPair.first)){
			                				newStoreLocPair.first=jobPlanStoreLoc;
			                			}
			                		}*/
			                		//keep info about the job, new shared store location
			                		jobSharedStoreMap.put(job,newStoreLocPair);
			                	}
        		        		//ami: add mro to candidate plans to share
        		                //SharedMapReducePlan candidateSharedPlan=new SharedMapReducePlan(mroToShare,null,null);
        		                //sharedPlans.add(candidateSharedPlan);
        		                //candidatePlansToShare.put(jobID, mro);
        		                SharedMapReducePlan candidateSharedPlan=new SharedMapReducePlan(mroToShare,null,null);
        		                //update the new shared plan original input bytes and time based on the shared plan info
        		                candidateSharedPlan.setHdfsBytesRead(sharedPlanBytesRead);
        		                candidateSharedPlan.setAvgPlanTime(sharedPlanAvgTime);
        		                candidateSharedPlan.setDiscoveredPlan(false);
        		                //create a pair for the main plan and its proposed subplans
        		                Pair<SharedMapReducePlan,Vector<SharedMapReducePlan>> planAndSubPlans=new Pair<SharedMapReducePlan,Vector<SharedMapReducePlan>>(candidateSharedPlan,candidateSubPlansToShare);
        		                candidatePlansToShare.put(jobID,planAndSubPlans);
        		        		
        		        	}{
        		        		//some of the discovered plans were not merged
        		        		//TODO
        		        	}
        		        	//@iman set the first iter to be false as at least one iter has been performed
        		            firstIter=false;
        		        }else{
        		        	//in this case we will run the plans that are discovered first and then
        		        	//continue
        		        	
        		        	//if we keep whole plans
			                if(isOptimizeBySharingWholePlan && (firstIter || !isUseDiscovePlanHeuristics) &&  !hasFinalStore(mroToShare)){
			                	dependingCandidateSharedPlan=new SharedMapReducePlan(mroToShare,null,null);
			                	//dependingCandidateSharedPlan.setJobForInputInfo(jobID);
			                }
			                
        		        	//new root plans are created, then add job for them instead of the actual mro
            				//that is now postponed until the next pass
            				for(int i=0; i<newMRRootPlans.size();i++){
            					MapReduceOper newRootPlan=newMRRootPlans.get(i);
            					if(isMoreDebugInfo){
                    				boolean verbose = true;
                            		PrintStream ps=System.out;
            	        			ps.println("#--------------------------------------------------");
            	        			ps.println("# To execute Discoverd Plans to share              ");
            	                    ps.println("# The mapreduce operator to be exec                ");
            	                    ps.println("#--------------------------------------------------");
            	                    MROperPlan execPlan2= new MROperPlan();
            	                    execPlan2.add(newRootPlan);
            	                    MRPrinter printer2 = new MRPrinter(ps, execPlan2);
            	                    printer2.setVerbose(verbose);
            	                    try {
            	        				printer2.visit();
            	        			} catch (VisitorException e) {
            	        				log.warn("Unable to print job plan", e);
            	        			}
                    			}
            					
            					//get a copy of the mr plan to share
            					MapReduceOper newRootPlanToShare=subPlansToShare.get(i).clone();
            					//MapReduceOper newRootPlanToShare=newRootPlan.clone();
            					
            					//if(plan.mergeSharedPlan(null));
            					//update the pig stats utility for new discovered jobs
            					// start collecting statistics
            			        PigStatsUtil.updateCollection(this, newRootPlan, mro); 
            			        
            					Job job = getJob(newRootPlan, conf, pigContext);
        	                    jobMroMap.put(job, newRootPlan);
        	                    String jobID=jobCtrl.addJob(job);
        	                    jobIDTag++;
        	                    jobID+="_"+jobIDTag;
        	                    //update jobID of the job
        	                    job.setJobID(jobID);
        	                    
        	                    if(isOptimizeBySharing){
        			                //ami: add mro to candidate plans to share
        			                //SharedMapReducePlan candidateSharedPlan=new SharedMapReducePlan(mroToShare,null,null);
        			                //sharedPlans.add(candidateSharedPlan);
        			                //candidatePlansToShare.put(jobID, mro);
        			                SharedMapReducePlan candidateSharedPlan=new SharedMapReducePlan(newRootPlanToShare,null,null);
        			                //update the new shared plan original input bytes and time based on the shared plan info
        			                candidateSharedPlan.setHdfsBytesRead(sharedPlanBytesRead);
        			                candidateSharedPlan.setAvgPlanTime(sharedPlanAvgTime);
        			                candidateSharedPlan.setDiscoveredPlan(true);
        			                //create a pair for the main plan and its proposed subplans
        			                Pair<SharedMapReducePlan,Vector<SharedMapReducePlan>> planAndSubPlans=new Pair<SharedMapReducePlan,Vector<SharedMapReducePlan>>(candidateSharedPlan,candidateSubPlansToShare);
        			                candidatePlansToShare.put(jobID,planAndSubPlans);
        			                
        			                //if we keep whole plans
        			                if(isOptimizeBySharingWholePlan && (firstIter || !isUseDiscovePlanHeuristics) && ! hasFinalStore(mroToShare)){
        			                	//dependingCandidateSharedPlan=new SharedMapReducePlan(mroToShare,null,null);
        			                	if(hasSameStoreLoc(dependingCandidateSharedPlan.getMRPlan(),newRootPlan )){
        			                		dependingCandidateSharedPlan.setJobForInputInfo(jobID);
        			                	}
        			                }
        	                    }
            				}
        		        	
        		        }
        		        
        		        //added temporarly ofr testing
        		        /*Job job = getJob(mro, conf, pigContext);
	                    jobMroMap.put(job, mro);
	                    String jobID=jobCtrl.addJob(job);
	                    jobIDTag++;
	                    jobID+="_"+jobIDTag;
	                    //update jobID of the job
	                    job.setJobID(jobID);*/
	                    
        				
        			}
                    /*if(isOptimizeBySharing){
		                //ami: add mro to candidate plans to share
		                //SharedMapReducePlan candidateSharedPlan=new SharedMapReducePlan(mroToShare,null,null);
		                //sharedPlans.add(candidateSharedPlan);
		                //candidatePlansToShare.put(jobID, mro);
		                SharedMapReducePlan candidateSharedPlan=new SharedMapReducePlan(mroToShare,null,null);
		                //update the new shared plan original input bytes and time based on the shared plan info
		                candidateSharedPlan.setHdfsBytesRead(sharedPlanBytesRead);
		                candidateSharedPlan.setAvgPlanTime(sharedPlanAvgTime);
		                //create a pair for the main plan and its proposed subplans
		                Pair<SharedMapReducePlan,Vector<SharedMapReducePlan>> planAndSubPlans=new Pair<SharedMapReducePlan,Vector<SharedMapReducePlan>>(candidateSharedPlan,candidateSubPlansToShare);
		                candidatePlansToShare.put(jobID,planAndSubPlans);
                    }*/
            	}
    			
    			
                //Job job = getJob(mro, conf, pigContext);
                //jobMroMap.put(job, mro);
                //jobCtrl.addJob(job);
            }
        } catch (JobCreationException jce) {
        	throw jce;
        } catch(Exception e) {
            int errCode = 2017;
            String msg = "Internal error creating job configuration.";
            throw new JobCreationException(msg, errCode, PigException.BUG, e);
        }

        //@iman 
        //because of the changes I made, we might reach this point with no jobs added to the jobctrl, 
        //for this case I need to 
        //I do not think there is a need for this step!!!!
        //if(jobCtrl.getWaitingJobs().size()==0){
        	//no job added to job ctrl, then try to execute it one more time
        	//return compile(plan, grpName);
        //}
        
        //@iman set the first iter to be false as at least one iter has been performed
        //firstIter=false;
        
        return jobCtrl;
    }
    
    private boolean hasSameStoreLoc(MapReduceOper firstPlan, MapReduceOper secondPlan) throws VisitorException {
    	//get stores of first plan
    	List<String> firstPlanStores = MapReduceOper.getStoreLocs(firstPlan);
    	List<String> secondPlanStores = MapReduceOper.getStoreLocs(secondPlan);
    	
    	for(String storeInFirstPlan:firstPlanStores){
    		if(!secondPlanStores.contains(storeInFirstPlan)){
    			return false;
    		}
    	}
		return true;
	}

	private boolean rewriteQueryUsingSharedPlans(MROperPlan plan, MapReduceOper mro, Vector<SharedMapReducePlan>sharedPlans) throws CloneNotSupportedException, IOException{
    	 boolean noExecForMRO=false;
    	 
    	for(SharedMapReducePlan sharedPlan: sharedPlans){
    		MapReduceOper sharedMRPlan=sharedPlan.getMRPlan();
    		MapReduceOper planReplacedWithView=null;
    		if(sharedMRPlan.isEquivalent(mro)){
				System.out.println("Found an equivalent view to this plan");
				//get the successor MR plan for mro
				List<MapReduceOper> successorMROs = plan.getSuccessors(mro);
				if(successorMROs==null|| successorMROs.size()<=0){
					//there are no successors to this plan, we only need to have a special condition to copy the 
					//o/p to the new location and terminate
					//TODO
					log.info("Found an exact match view");
					//check if the output of this job currently exist
					
				}else{
    				//update successor MR plans with the new load replacing the old tmp one
    				for (MapReduceOper successorMRO: successorMROs) {
    					successorMRO.updateLoadOperator(mro,sharedMRPlan);
    				}
				}
				//@iman remove this plan from the list of plans
				//plan.trimBelow(mro);
	            plan.remove(mro);
	            
				noExecForMRO=true;
				//@iman set the first iter to be false as at least one iter has been performed
		        firstIter=false;
				//bestSharedPlanSoFar=null; //make sure it is null, so we will not execute that parts that handles shared plans again
				break;
			}else{
				//the plan is not equivalent with the view, check if it is subset of it
				planReplacedWithView=mro.getPlanRecplacedWithView(sharedMRPlan,pigContext);
				if(planReplacedWithView!=null ){//&& (bestSharedPlanSoFar==null || sharedPlan.isBetterPlan(bestSharedPlanSoFar))
					System.out.println("Found an view that is a subset of this plan");
					//get the successor MR plan for mro
    				List<MapReduceOper> successorMROs = plan.getSuccessors(mro);
    				if(planReplacedWithView==null){
    					continue;
    				}else if(planReplacedWithView==sharedMRPlan){
    					//the new plan is the same as the shared plan after changing the store location
    					if(successorMROs==null|| successorMROs.size()<=0){
    						// only copy the files in the old store location to the new one
    						log.info("To copy the files from the old location to the new one");
    						POStore sharedPlanStoreLoc=sharedMRPlan.getStore(sharedMRPlan);//getPlanFinalStoreLocation(sharedMRPlan);
    						POStore newPlanStoreLoc=mro.getStore(mro);//getPlanFinalStoreLocation(mro);
    						copyResultLocations(sharedPlanStoreLoc,newPlanStoreLoc);
    						//@iman remove this plan from the list of plans
            				//plan.trimBelow(mro);
            	            plan.remove(mro);
    						//set flag that we will not exec mro
    						noExecForMRO=true;
    						//@iman set the first iter to be false as at least one iter has been performed
    				        firstIter=false;
    					}else{
    						log.info("To update successor MR plans by merging the new mro into their maps");
	        				//update successor MR plans by merging the new mro into their maps
	        				for (int i=0;i<successorMROs.size();i++) {
	        					MapReduceOper successorMRO= successorMROs.get(i);
	        					//TODO replace the load operator with the location of store operator from shared plan
	        					if(successorMRO.updateLoadOperator(mro,sharedMRPlan)==true){
	        						//@iman remove this plan from the list of plans
                    				//plan.trimBelow(mro);
                    	            plan.remove(mro);
	        						//set flag that we will not exec mro
	        						noExecForMRO=true;
	        						//@iman set the first iter to be false as at least one iter has been performed
	        				        firstIter=false;
	        					}
	        				}
	        				
        				}
    					break;
    				}else{
    					log.info("the differences in the shared plan and the new plan are more than store location ");
    					log.info("the shared plan is subsumed in this plan");
    					log.info("therefore, we replace the last shared op with a load from the store loc of the shared plan");
    					log.info("the new rewritten plan is executed and nothing will be changed for subsecuent MR plans");
    					//store info about sharedPlan bytes read and execution time
    					//sharedPlanBytesRead=sharedPlan.getHdfsBytesRead();
    					//sharedPlanAvgTime=sharedPlan.getAvgPlanTime();
    					//the differences in the shared plan and the new plan are more than store location
    					//print the shared plan that is used for rewritting the query
        				if(this.isMoreDebugInfo){
            				boolean verbose = true;
                    		PrintStream ps=System.out;
                            ps.println("#--------------------------------------------------");
                            ps.println("# The plan that we will use to rewrite the query:  ");
                            ps.println("#--------------------------------------------------");
                            MROperPlan execPlan= new MROperPlan();
                            execPlan.add(sharedMRPlan);
                            MRPrinter printer = new MRPrinter(ps, execPlan);
                            printer.setVerbose(verbose);
                            try {
                				printer.visit();
                			} catch (VisitorException e) {
                				log.warn("Unable to print shared plan", e);
                			}
        				}
        				//we need to start matching from the begining
    					Vector<SharedMapReducePlan>sharedPlansCopy=new Vector<SharedMapReducePlan>(sharedPlans);
    					sharedPlansCopy.remove(sharedPlan);
    					return rewriteQueryUsingSharedPlans(plan, mro, sharedPlansCopy);
    				}
    				
    				//break;
				}//end if the shared plan is subset of the plan i am currently executing
			}
    	}//end for every shared plan
    	return noExecForMRO;
    }
    
    private boolean hasFinalStore(MapReduceOper mroToShare) throws VisitorException {
    	if(isUseDiscovePlanHeuristics){
    		return false;
    	}
    	List<POStore> planStores = MapReduceOper.getStores(mroToShare);
    	if(planStores==null || planStores.isEmpty()){
    		return false;
    	}else{
    		for(POStore planStore:planStores){
    			String storeFile=planStore.getSFile().getFileName();
    			if(storeFile.contains("_out")){
    				return true;
    			}
    		}
    	}
    	return false;
	}

	/*private String getPlanFinalStoreLocation(MROperPlan mrPlan) {
    	String finalStoreLocation=null;
    	List<POStore> planStores = mrPlan.getStores();
    	if(planStores.size()>0){
    		//TODO I am assuming that I have only one store in the plan!!
    		MapReduceOper planStore=planStores.get(0);
    		if(planStore instanceof POStore){
    			
    		}
    	}
		return finalStoreLocation;
	}*/

	private void copyResultLocations(POStore oldPlanStore, POStore newPlanStore) throws IOException {
		//get the uri of the store location of old plan
		String oldPlanStoreLoc=oldPlanStore.getSFile().getFileName();
		Path oldPath=new Path(oldPlanStoreLoc);
		FileSystem oldFS = oldPath.getFileSystem(conf);
		//get the uri for the store location of the new plan
		String newPlanStoreLoc=newPlanStore.getSFile().getFileName();
		Path newPath=new Path(newPlanStoreLoc);
		FileSystem newFS = newPath.getFileSystem(conf);
		//copy the data at the old location to the new location
		log.info("To copy files from: "+oldPlanStoreLoc+" to: "+newPlanStoreLoc);
		FileUtil.copy(oldFS,oldPath,newFS,newPath,false,conf);
	}
	
	private void copyResultLocations(String oldPlanStoreLoc, String newPlanStoreLoc) throws IOException {
		//get the uri of the store location of old plan
		//String oldPlanStoreLoc=oldPlanStore.getSFile().getFileName();
		Path oldPath=new Path(oldPlanStoreLoc);
		FileSystem oldFS = oldPath.getFileSystem(conf);
		//get the uri for the store location of the new plan
		//String newPlanStoreLoc=newPlanStore.getSFile().getFileName();
		Path newPath=new Path(newPlanStoreLoc);
		FileSystem newFS = newPath.getFileSystem(conf);
		//copy the data at the old location to the new location
		log.info("To copy files from: "+oldPlanStoreLoc+" to: "+newPlanStoreLoc);
		FileUtil.copy(oldFS,oldPath,newFS,newPath,false,conf);
	}

	// Update Map-Reduce plan with the execution status of the jobs. If one job
    // completely fail (the job has only one store and that job fail), then we 
    // remove all its dependent jobs. This method will return the number of MapReduceOper
    // removed from the Map-Reduce plan
    public int updateMROpPlan(List<Job> completeFailedJobs)
    {
        int sizeBefore = plan.size();
        for (Job job : completeFailedJobs)  // remove all subsequent jobs
        {
            MapReduceOper mrOper = jobMroMap.get(job); 
            plan.trimBelow(mrOper);
            plan.remove(mrOper);
        }

        // Remove successful jobs from jobMroMap
        for (Job job : jobMroMap.keySet())
        {
            if (!completeFailedJobs.contains(job))
            {
                MapReduceOper mro = jobMroMap.get(job);
                plan.remove(mro);
            }
        }
        jobMroMap.clear();
        int sizeAfter = plan.size();
        return sizeBefore-sizeAfter;
    }
        
    /**
     * The method that creates the Job corresponding to a MapReduceOper.
     * The assumption is that
     * every MapReduceOper will have a load and a store. The JobConf removes
     * the load operator and serializes the input filespec so that PigInputFormat can
     * take over the creation of splits. It also removes the store operator
     * and serializes the output filespec so that PigOutputFormat can take over
     * record writing. The remaining portion of the map plan and reduce plans are
     * serialized and stored for the PigMapReduce or PigMapOnly objects to take over
     * the actual running of the plans.
     * The Mapper &amp; Reducer classes and the required key value formats are set.
     * Checks if this is a map only job and uses PigMapOnly class as the mapper
     * and uses PigMapReduce otherwise.
     * If it is a Map Reduce job, it is bound to have a package operator. Remove it from
     * the reduce plan and serializes it so that the PigMapReduce class can use it to package
     * the indexed tuples received by the reducer.
     * @param mro - The MapReduceOper for which the JobConf is required
     * @param conf - the Configuration object from which JobConf is built
     * @param pigContext - The PigContext passed on from execution engine
     * @return Job corresponding to mro
     * @throws JobCreationException
     */
    @SuppressWarnings({ "unchecked", "deprecation" })
    private Job getJob(MapReduceOper mro, Configuration config, PigContext pigContext) throws JobCreationException{
        org.apache.hadoop.mapreduce.Job nwJob = null;
        
        try{
            nwJob = new org.apache.hadoop.mapreduce.Job(config);        
        }catch(Exception e) {
            throw new JobCreationException(e);
        }
        
        Configuration conf = nwJob.getConfiguration();
        
        ArrayList<FileSpec> inp = new ArrayList<FileSpec>();
        ArrayList<List<OperatorKey>> inpTargets = new ArrayList<List<OperatorKey>>();
        ArrayList<String> inpSignatureLists = new ArrayList<String>();
        ArrayList<POStore> storeLocations = new ArrayList<POStore>();
        Path tmpLocation = null;
        
        // add settings for pig statistics
        String setScriptProp = conf.get(ScriptState.INSERT_ENABLED, "true");
        if (setScriptProp.equalsIgnoreCase("true")) {
            ScriptState ss = ScriptState.get();
            ss.addSettingsToConf(mro, conf);
        }
        
 
        conf.set("mapred.mapper.new-api", "true");
        conf.set("mapred.reducer.new-api", "true");
        
        String buffPercent = conf.get("mapred.job.reduce.markreset.buffer.percent");
        if (buffPercent == null || Double.parseDouble(buffPercent) <= 0) {
            log.info("mapred.job.reduce.markreset.buffer.percent is not set, set to default 0.3");
            conf.set("mapred.job.reduce.markreset.buffer.percent", "0.3");
        }else{
            log.info("mapred.job.reduce.markreset.buffer.percent is set to " + conf.get("mapred.job.reduce.markreset.buffer.percent"));
        }        
                
        try{        
            //Process the POLoads
            List<POLoad> lds = PlanHelper.getLoads(mro.mapPlan);
            
            if(lds!=null && lds.size()>0){
                for (POLoad ld : lds) {
                    
                    //Store the inp filespecs
                    inp.add(ld.getLFile());
                    
                    //Store the target operators for tuples read
                    //from this input
                    List<PhysicalOperator> ldSucs = mro.mapPlan.getSuccessors(ld);
                    List<OperatorKey> ldSucKeys = new ArrayList<OperatorKey>();
                    if(ldSucs!=null){
                        for (PhysicalOperator operator2 : ldSucs) {
                            ldSucKeys.add(operator2.getOperatorKey());
                        }
                    }
                    inpTargets.add(ldSucKeys);
                    inpSignatureLists.add(ld.getSignature());
                    //Remove the POLoad from the plan
                    mro.mapPlan.remove(ld);
                }
            }

            //Create the jar of all functions and classes required
            File submitJarFile = File.createTempFile("Job", ".jar");
            // ensure the job jar is deleted on exit
            submitJarFile.deleteOnExit();
            FileOutputStream fos = new FileOutputStream(submitJarFile);
            JarManager.createJar(fos, mro.UDFs, pigContext);
            
            //Start setting the JobConf properties
            conf.set("mapred.jar", submitJarFile.getPath());
            conf.set("pig.inputs", ObjectSerializer.serialize(inp));
            conf.set("pig.inpTargets", ObjectSerializer.serialize(inpTargets));
            conf.set("pig.inpSignatures", ObjectSerializer.serialize(inpSignatureLists));
            conf.set("pig.pigContext", ObjectSerializer.serialize(pigContext));
            conf.set("udf.import.list", ObjectSerializer.serialize(PigContext.getPackageImportList()));
            // this is for unit tests since some don't create PigServer
           
            // if user specified the job name using -D switch, Pig won't reset the name then.
            if (System.getProperty("mapred.job.name") == null && 
                    pigContext.getProperties().getProperty(PigContext.JOB_NAME) != null){
                nwJob.setJobName(pigContext.getProperties().getProperty(PigContext.JOB_NAME));                
            }
    
            if (pigContext.getProperties().getProperty(PigContext.JOB_PRIORITY) != null) {
                // If the job priority was set, attempt to get the corresponding enum value
                // and set the hadoop job priority.
                String jobPriority = pigContext.getProperties().getProperty(PigContext.JOB_PRIORITY).toUpperCase();
                try {
                  // Allow arbitrary case; the Hadoop job priorities are all upper case.
                  conf.set("mapred.job.priority", JobPriority.valueOf(jobPriority).toString());
                  
                } catch (IllegalArgumentException e) {
                  StringBuffer sb = new StringBuffer("The job priority must be one of [");
                  JobPriority[] priorities = JobPriority.values();
                  for (int i = 0; i < priorities.length; ++i) {
                    if (i > 0)  sb.append(", ");
                    sb.append(priorities[i]);
                  }
                  sb.append("].  You specified [" + jobPriority + "]");
                  throw new JobCreationException(sb.toString());
                }
            }

            // Setup the DistributedCache for this job
            setupDistributedCache(pigContext, nwJob.getConfiguration(), pigContext.getProperties(), 
                                  "pig.streaming.ship.files", true);
            setupDistributedCache(pigContext, nwJob.getConfiguration(), pigContext.getProperties(), 
                                  "pig.streaming.cache.files", false);

            nwJob.setInputFormatClass(PigInputFormat.class);
            
            //Process POStore and remove it from the plan
            LinkedList<POStore> mapStores = PlanHelper.getStores(mro.mapPlan);
            LinkedList<POStore> reduceStores = PlanHelper.getStores(mro.reducePlan);
            
            for (POStore st: mapStores) {
                storeLocations.add(st);
                StoreFuncInterface sFunc = st.getStoreFunc();
                sFunc.setStoreLocation(st.getSFile().getFileName(), nwJob);
            }

            for (POStore st: reduceStores) {
                storeLocations.add(st);
                StoreFuncInterface sFunc = st.getStoreFunc();
                sFunc.setStoreLocation(st.getSFile().getFileName(), nwJob);
            }

            // the OutputFormat we report to Hadoop is always PigOutputFormat
            nwJob.setOutputFormatClass(PigOutputFormat.class);
            
            if (mapStores.size() + reduceStores.size() == 1) { // single store case
                log.info("Setting up single store job");
                
                POStore st;
                if (reduceStores.isEmpty()) {
                    st = mapStores.get(0);
                    mro.mapPlan.remove(st);
                }
                else {
                    st = reduceStores.get(0);
                    mro.reducePlan.remove(st);
                }

                // set out filespecs
                String outputPath = st.getSFile().getFileName();
                FuncSpec outputFuncSpec = st.getSFile().getFuncSpec();
                
                conf.set("pig.streaming.log.dir", 
                            new Path(outputPath, LOG_DIR).toString());
                conf.set("pig.streaming.task.output.dir", outputPath);
            } 
           else { // multi store case
                log.info("Setting up multi store job");
                String tmpLocationStr =  FileLocalizer
                .getTemporaryPath(pigContext).toString();
                tmpLocation = new Path(tmpLocationStr);

                nwJob.setOutputFormatClass(PigOutputFormat.class);
                
                for (POStore sto: storeLocations) {
                    sto.setMultiStore(true);
                }
 
                conf.set("pig.streaming.log.dir", 
                            new Path(tmpLocation, LOG_DIR).toString());
                conf.set("pig.streaming.task.output.dir", tmpLocation.toString());
           }

            // store map key type
            // this is needed when the key is null to create
            // an appropriate NullableXXXWritable object
            conf.set("pig.map.keytype", ObjectSerializer.serialize(new byte[] { mro.mapKeyType }));

            // set parent plan in all operators in map and reduce plans
            // currently the parent plan is really used only when POStream is present in the plan
            new PhyPlanSetter(mro.mapPlan).visit();
            new PhyPlanSetter(mro.reducePlan).visit();
            
            // this call modifies the ReplFiles names of POFRJoin operators
            // within the MR plans, must be called before the plans are
            // serialized
            setupDistributedCacheForJoin(mro, pigContext, conf);

            POPackage pack = null;
            if(mro.reducePlan.isEmpty()){
                //MapOnly Job
                nwJob.setMapperClass(PigMapOnly.Map.class);
                nwJob.setNumReduceTasks(0);
                conf.set("pig.mapPlan", ObjectSerializer.serialize(mro.mapPlan));
                if(mro.isEndOfAllInputSetInMap()) {
                    // this is used in Map.close() to decide whether the
                    // pipeline needs to be rerun one more time in the close()
                    // The pipeline is rerun if there either was a stream or POMergeJoin
                    conf.set(END_OF_INP_IN_MAP, "true");
                }
            }
            else{
                //Map Reduce Job
                //Process the POPackage operator and remove it from the reduce plan
                if(!mro.combinePlan.isEmpty()){
                    POPackage combPack = (POPackage)mro.combinePlan.getRoots().get(0);
                    mro.combinePlan.remove(combPack);
                    nwJob.setCombinerClass(PigCombiner.Combine.class);
                    conf.set("pig.combinePlan", ObjectSerializer.serialize(mro.combinePlan));
                    conf.set("pig.combine.package", ObjectSerializer.serialize(combPack));
                } else if (mro.needsDistinctCombiner()) {
                    nwJob.setCombinerClass(DistinctCombiner.Combine.class);
                    log.info("Setting identity combiner class.");
                }
                pack = (POPackage)mro.reducePlan.getRoots().get(0);
                mro.reducePlan.remove(pack);
                nwJob.setMapperClass(PigMapReduce.Map.class);
                nwJob.setReducerClass(PigMapReduce.Reduce.class);
                
                // first check the PARALLE in query, then check the defaultParallel in PigContext, and last do estimation
                if (mro.requestedParallelism > 0)
                    nwJob.setNumReduceTasks(mro.requestedParallelism);
		else if (pigContext.defaultParallel > 0)
                    conf.set("mapred.reduce.tasks", ""+pigContext.defaultParallel);
                else
                    estimateNumberOfReducers(conf,lds);
                
                if (mro.customPartitioner != null)
                	nwJob.setPartitionerClass(PigContext.resolveClassName(mro.customPartitioner));

                conf.set("pig.mapPlan", ObjectSerializer.serialize(mro.mapPlan));
                if(mro.isEndOfAllInputSetInMap()) {
                    // this is used in Map.close() to decide whether the
                    // pipeline needs to be rerun one more time in the close()
                    // The pipeline is rerun only if there was a stream or merge-join.
                    conf.set(END_OF_INP_IN_MAP, "true");
                }
                conf.set("pig.reducePlan", ObjectSerializer.serialize(mro.reducePlan));
                if(mro.isEndOfAllInputSetInReduce()) {
                    // this is used in Map.close() to decide whether the
                    // pipeline needs to be rerun one more time in the close()
                    // The pipeline is rerun only if there was a stream
                    conf.set("pig.stream.in.reduce", "true");
                }
                conf.set("pig.reduce.package", ObjectSerializer.serialize(pack));
                conf.set("pig.reduce.key.type", Byte.toString(pack.getKeyType())); 
                
                if (mro.getUseSecondaryKey()) {
                    nwJob.setGroupingComparatorClass(PigSecondaryKeyGroupComparator.class);
                    nwJob.setPartitionerClass(SecondaryKeyPartitioner.class);
                    nwJob.setSortComparatorClass(PigSecondaryKeyComparator.class);
                    nwJob.setOutputKeyClass(NullableTuple.class);
                    conf.set("pig.secondarySortOrder",
                            ObjectSerializer.serialize(mro.getSecondarySortOrder()));

                }
                else
                {
                    Class<? extends WritableComparable> keyClass = HDataType.getWritableComparableTypes(pack.getKeyType()).getClass();
                    nwJob.setOutputKeyClass(keyClass);
                    selectComparator(mro, pack.getKeyType(), nwJob);
                }
                nwJob.setOutputValueClass(NullableTuple.class);
            }
        
            if(mro.isGlobalSort() || mro.isLimitAfterSort()){
                // Only set the quantiles file and sort partitioner if we're a
                // global sort, not for limit after sort.
                if (mro.isGlobalSort()) {
                    String symlink = addSingleFileToDistributedCache(
                            pigContext, conf, mro.getQuantFile(), "pigsample");
                    conf.set("pig.quantilesFile", symlink);
                    nwJob.setPartitionerClass(WeightedRangePartitioner.class);
                }
                
                if (mro.isUDFComparatorUsed) {  
                    boolean usercomparator = false;
                    for (String compFuncSpec : mro.UDFs) {
                        Class comparator = PigContext.resolveClassName(compFuncSpec);
                        if(ComparisonFunc.class.isAssignableFrom(comparator)) {
                            nwJob.setMapperClass(PigMapReduce.MapWithComparator.class);
                            nwJob.setReducerClass(PigMapReduce.ReduceWithComparator.class);
                            conf.set("pig.reduce.package", ObjectSerializer.serialize(pack));
                            conf.set("pig.usercomparator", "true");
                            nwJob.setOutputKeyClass(NullableTuple.class);
                            nwJob.setSortComparatorClass(comparator);
                            usercomparator = true;
                            break;
                        }
                    }
                    if (!usercomparator) {
                        String msg = "Internal error. Can't find the UDF comparator";
                        throw new IOException (msg);
                    }
                    
                } else {
                    conf.set("pig.sortOrder",
                        ObjectSerializer.serialize(mro.getSortOrder()));
                }
            }
            
            if (mro.isSkewedJoin()) {
                String symlink = addSingleFileToDistributedCache(pigContext,
                        conf, mro.getSkewedJoinPartitionFile(), "pigdistkey");
                conf.set("pig.keyDistFile", symlink);
                nwJob.setPartitionerClass(SkewedPartitioner.class);
                nwJob.setMapperClass(PigMapReduce.MapWithPartitionIndex.class);
                nwJob.setMapOutputKeyClass(NullablePartitionWritable.class);
                nwJob.setGroupingComparatorClass(PigGroupingPartitionWritableComparator.class);
            }
            
            // unset inputs for POStore, otherwise, map/reduce plan will be unnecessarily deserialized 
            for (POStore st: mapStores) { st.setInputs(null); st.setParentPlan(null);}
            for (POStore st: reduceStores) { st.setInputs(null); st.setParentPlan(null);}

            // tmp file compression setups
            if (Utils.tmpFileCompression(pigContext)) {
                conf.setBoolean("pig.tmpfilecompression", true);
                conf.set("pig.tmpfilecompression.codec", Utils.tmpFileCompressionCodec(pigContext));
            }

            conf.set(PIG_MAP_STORES, ObjectSerializer.serialize(mapStores));
            conf.set(PIG_REDUCE_STORES, ObjectSerializer.serialize(reduceStores));
            String tmp;
            long maxCombinedSplitSize = 0;
            if (!mro.combineSmallSplits() || pigContext.getProperties().getProperty("pig.splitCombination", "true").equals("false"))
                conf.setBoolean("pig.noSplitCombination", true);
            else if ((tmp = pigContext.getProperties().getProperty("pig.maxCombinedSplitSize", null)) != null) {
                try {
                    maxCombinedSplitSize = Long.parseLong(tmp);
                } catch (NumberFormatException e) {
                    log.warn("Invalid numeric format for pig.maxCombinedSplitSize; use the default maximum combined split size");
                }
            }
            if (maxCombinedSplitSize > 0)
                conf.setLong("pig.maxCombinedSplitSize", maxCombinedSplitSize);
                        
            // Serialize the UDF specific context info.
            UDFContext.getUDFContext().serialize(conf);
            Job cjob = new Job(new JobConf(nwJob.getConfiguration()), new ArrayList());
            jobStoreMap.put(cjob,new Pair<List<POStore>, Path>(storeLocations, tmpLocation));
            
            return cjob;
            
        } catch (JobCreationException jce) {
            throw jce;
        } catch(Exception e) {
            int errCode = 2017;
            String msg = "Internal error creating job configuration.";
            throw new JobCreationException(msg, errCode, PigException.BUG, e);
        }
    }
    
    /**
     * Currently the estimation of reducer number is only applied to HDFS, The estimation is based on the input size of data storage on HDFS.
     * Two parameters can been configured for the estimation, one is pig.exec.reducers.max which constrain the maximum number of reducer task (default is 999). The other
     * is pig.exec.reducers.bytes.per.reducer(default value is 1000*1000*1000) which means the how much data can been handled for each reducer.
     * e.g. the following is your pig script
     * a = load '/data/a';
     * b = load '/data/b';
     * c = join a by $0, b by $0;
     * store c into '/tmp';
     * 
     * The size of /data/a is 1000*1000*1000, and size of /data/b is 2*1000*1000*1000.
     * Then the estimated reducer number is (1000*1000*1000+2*1000*1000*1000)/(1000*1000*1000)=3
     * @param conf
     * @param lds
     * @throws IOException
     */
    static int estimateNumberOfReducers(Configuration conf, List<POLoad> lds) throws IOException {
           long bytesPerReducer = conf.getLong("pig.exec.reducers.bytes.per.reducer", (long) (1000 * 1000 * 1000));
        int maxReducers = conf.getInt("pig.exec.reducers.max", 999);
        long totalInputFileSize = getTotalInputFileSize(conf, lds);
       
        log.info("BytesPerReducer=" + bytesPerReducer + " maxReducers="
            + maxReducers + " totalInputFileSize=" + totalInputFileSize);
        
        int reducers = (int)Math.ceil((totalInputFileSize+0.0) / bytesPerReducer);
        reducers = Math.max(1, reducers);
        reducers = Math.min(maxReducers, reducers);
        conf.setInt("mapred.reduce.tasks", reducers);

        log.info("Neither PARALLEL nor default parallelism is set for this job. Setting number of reducers to " + reducers);
        return reducers;
    }

    private static long getTotalInputFileSize(Configuration conf, List<POLoad> lds) throws IOException {
        List<String> inputs = new ArrayList<String>();
        if(lds!=null && lds.size()>0){
            for (POLoad ld : lds) {
                inputs.add(ld.getLFile().getFileName());
            }
        }
        long size = 0;
        FileSystem fs = FileSystem.get(conf);
        for (String input : inputs){
            //Using custom uri parsing because 'new Path(location).toUri()' fails
            // for some valid uri's (eg jdbc style), and 'new Uri(location)' fails
            // for valid hdfs paths that contain curly braces
            if(!UriUtil.isHDFSFileOrLocal(input)){
                //skip  if it is not hdfs or local file
                continue;
            }
            //the input file location might be a list of comma separeated files, 
            // separate them out
            for(String location : LoadFunc.getPathStrings(input)){
                if(! UriUtil.isHDFSFileOrLocal(location)){
                    continue;
                }
                FileStatus[] status=fs.globStatus(new Path(location));
                if (status != null){
                    for (FileStatus s : status){
                        size += getPathLength(fs, s);
                    }
                }
            }
        }
        return size;
   }
   
    private static long getPathLength(FileSystem fs,FileStatus status) throws IOException{
        if (!status.isDir()){
            return status.getLen();
        }else{
            FileStatus[] children = fs.listStatus(status.getPath());
            long size=0;
            for (FileStatus child : children){
                size +=getPathLength(fs, child);
            }
            return size;
        }
    }
        
    public static class PigSecondaryKeyGroupComparator extends WritableComparator {
        public PigSecondaryKeyGroupComparator() {
//            super(TupleFactory.getInstance().tupleClass(), true);
            super(NullableTuple.class, true);
        }

        @SuppressWarnings("unchecked")
		@Override
        public int compare(WritableComparable a, WritableComparable b)
        {
            PigNullableWritable wa = (PigNullableWritable)a;
            PigNullableWritable wb = (PigNullableWritable)b;
            if ((wa.getIndex() & PigNullableWritable.mqFlag) != 0) { // this is a multi-query index
                if ((wa.getIndex() & PigNullableWritable.idxSpace) < (wb.getIndex() & PigNullableWritable.idxSpace)) return -1;
                else if ((wa.getIndex() & PigNullableWritable.idxSpace) > (wb.getIndex() & PigNullableWritable.idxSpace)) return 1;
                // If equal, we fall through
            }
            
            // wa and wb are guaranteed to be not null, POLocalRearrange will create a tuple anyway even if main key and secondary key
            // are both null; however, main key can be null, we need to check for that using the same logic we have in PigNullableWritable
            Object valuea = null;
            Object valueb = null;
            try {
                // Get the main key from compound key
                valuea = ((Tuple)wa.getValueAsPigType()).get(0);
                valueb = ((Tuple)wb.getValueAsPigType()).get(0);
            } catch (ExecException e) {
                throw new RuntimeException("Unable to access tuple field", e);
            }
            if (!wa.isNull() && !wb.isNull()) {
                
                int result = DataType.compare(valuea, valueb);
                
                // If any of the field inside tuple is null, then we do not merge keys
                // See PIG-927
                if (result == 0 && valuea instanceof Tuple && valueb instanceof Tuple)
                {
                    try {
                        for (int i=0;i<((Tuple)valuea).size();i++)
                            if (((Tuple)valueb).get(i)==null)
                                return (wa.getIndex()&PigNullableWritable.idxSpace) - (wb.getIndex()&PigNullableWritable.idxSpace);
                    } catch (ExecException e) {
                        throw new RuntimeException("Unable to access tuple field", e);
                    }
                }
                return result;
            } else if (valuea==null && valueb==null) {
                // If they're both null, compare the indicies
                if ((wa.getIndex() & PigNullableWritable.idxSpace) < (wb.getIndex() & PigNullableWritable.idxSpace)) return -1;
                else if ((wa.getIndex() & PigNullableWritable.idxSpace) > (wb.getIndex() & PigNullableWritable.idxSpace)) return 1;
                else return 0;
            }
            else if (valuea==null) return -1; 
            else return 1;
        }
    }
    
    public static class PigWritableComparator extends WritableComparator {
        @SuppressWarnings("unchecked")
        protected PigWritableComparator(Class c) {
            super(c);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class PigIntWritableComparator extends PigWritableComparator {
        public PigIntWritableComparator() {
            super(NullableIntWritable.class);
        }
    }

    public static class PigLongWritableComparator extends PigWritableComparator {
        public PigLongWritableComparator() {
            super(NullableLongWritable.class);
        }
    }

    public static class PigFloatWritableComparator extends PigWritableComparator {
        public PigFloatWritableComparator() {
            super(NullableFloatWritable.class);
        }
    }

    public static class PigDoubleWritableComparator extends PigWritableComparator {
        public PigDoubleWritableComparator() {
            super(NullableDoubleWritable.class);
        }
    }

    public static class PigCharArrayWritableComparator extends PigWritableComparator {
        public PigCharArrayWritableComparator() {
            super(NullableText.class);
        }
    }

    public static class PigDBAWritableComparator extends PigWritableComparator {
        public PigDBAWritableComparator() {
            super(NullableBytesWritable.class);
        }
    }

    public static class PigTupleWritableComparator extends PigWritableComparator {
        public PigTupleWritableComparator() {
            super(TupleFactory.getInstance().tupleClass());
        }
    }

    public static class PigBagWritableComparator extends PigWritableComparator {
        public PigBagWritableComparator() {
            super(BagFactory.getInstance().newDefaultBag().getClass());
        }
    }
    
    // XXX hadoop 20 new API integration: we need to explicitly set the Grouping 
    // Comparator
    public static class PigGroupingIntWritableComparator extends WritableComparator {
        public PigGroupingIntWritableComparator() {
            super(NullableIntWritable.class, true);
        }
    }

    public static class PigGroupingLongWritableComparator extends WritableComparator {
        public PigGroupingLongWritableComparator() {
            super(NullableLongWritable.class, true);
        }
    }

    public static class PigGroupingFloatWritableComparator extends WritableComparator {
        public PigGroupingFloatWritableComparator() {
            super(NullableFloatWritable.class, true);
        }
    }

    public static class PigGroupingDoubleWritableComparator extends WritableComparator {
        public PigGroupingDoubleWritableComparator() {
            super(NullableDoubleWritable.class, true);
        }
    }

    public static class PigGroupingCharArrayWritableComparator extends WritableComparator {
        public PigGroupingCharArrayWritableComparator() {
            super(NullableText.class, true);
        }
    }

    public static class PigGroupingDBAWritableComparator extends WritableComparator {
        public PigGroupingDBAWritableComparator() {
            super(NullableBytesWritable.class, true);
        }
    }

    public static class PigGroupingTupleWritableComparator extends WritableComparator {
        public PigGroupingTupleWritableComparator() {
            super(NullableTuple.class, true);
        }
    }
    
    public static class PigGroupingPartitionWritableComparator extends WritableComparator {
        public PigGroupingPartitionWritableComparator() {
            super(NullablePartitionWritable.class, true);
        }
    }

    public static class PigGroupingBagWritableComparator extends WritableComparator {
        public PigGroupingBagWritableComparator() {
            super(BagFactory.getInstance().newDefaultBag().getClass(), true);
        }
    }
    
    private void selectComparator(
            MapReduceOper mro,
            byte keyType,
            org.apache.hadoop.mapreduce.Job job) throws JobCreationException {
        // If this operator is involved in an order by, use the pig specific raw
        // comparators.  If it has a cogroup, we need to set the comparator class
        // to the raw comparator and the grouping comparator class to pig specific
        // raw comparators (which skip the index).  Otherwise use the hadoop provided
        // raw comparator.
        
        // An operator has an order by if global sort is set or if it's successor has
        // global sort set (because in that case it's the sampling job) or if
        // it's a limit after a sort. 
        boolean hasOrderBy = false;
        if (mro.isGlobalSort() || mro.isLimitAfterSort() || mro.usingTypedComparator()) {
            hasOrderBy = true;
        } else {
            List<MapReduceOper> succs = plan.getSuccessors(mro);
            if (succs != null) {
                MapReduceOper succ = succs.get(0);
                if (succ.isGlobalSort()) hasOrderBy = true;
            }
        }
        if (hasOrderBy) {
            switch (keyType) {
            case DataType.INTEGER:            	
                job.setSortComparatorClass(PigIntRawComparator.class);               
                break;

            case DataType.LONG:
                job.setSortComparatorClass(PigLongRawComparator.class);               
                break;

            case DataType.FLOAT:
                job.setSortComparatorClass(PigFloatRawComparator.class);
                break;

            case DataType.DOUBLE:
                job.setSortComparatorClass(PigDoubleRawComparator.class);
                break;

            case DataType.CHARARRAY:
                job.setSortComparatorClass(PigTextRawComparator.class);
                break;

            case DataType.BYTEARRAY:
                job.setSortComparatorClass(PigBytesRawComparator.class);
                break;

            case DataType.MAP:
                int errCode = 1068;
                String msg = "Using Map as key not supported.";
                throw new JobCreationException(msg, errCode, PigException.INPUT);

            case DataType.TUPLE:
                job.setSortComparatorClass(PigTupleSortComparator.class);
                break;

            case DataType.BAG:
                errCode = 1068;
                msg = "Using Bag as key not supported.";
                throw new JobCreationException(msg, errCode, PigException.INPUT);

            default:
                break;
            }
            return;
        }

        switch (keyType) {
        case DataType.INTEGER:
            job.setSortComparatorClass(PigIntWritableComparator.class);
            job.setGroupingComparatorClass(PigGroupingIntWritableComparator.class);
            break;

        case DataType.LONG:
            job.setSortComparatorClass(PigLongWritableComparator.class);
            job.setGroupingComparatorClass(PigGroupingLongWritableComparator.class);
            break;

        case DataType.FLOAT:
            job.setSortComparatorClass(PigFloatWritableComparator.class);
            job.setGroupingComparatorClass(PigGroupingFloatWritableComparator.class);
            break;

        case DataType.DOUBLE:
            job.setSortComparatorClass(PigDoubleWritableComparator.class);
            job.setGroupingComparatorClass(PigGroupingDoubleWritableComparator.class);
            break;

        case DataType.CHARARRAY:
            job.setSortComparatorClass(PigCharArrayWritableComparator.class);
            job.setGroupingComparatorClass(PigGroupingCharArrayWritableComparator.class);
            break;

        case DataType.BYTEARRAY:
            job.setSortComparatorClass(PigDBAWritableComparator.class);
            job.setGroupingComparatorClass(PigGroupingDBAWritableComparator.class);
            break;

        case DataType.MAP:
            int errCode = 1068;
            String msg = "Using Map as key not supported.";
            throw new JobCreationException(msg, errCode, PigException.INPUT);

        case DataType.TUPLE:
            job.setSortComparatorClass(PigTupleWritableComparator.class);
            job.setGroupingComparatorClass(PigGroupingTupleWritableComparator.class);
            break;

        case DataType.BAG:
            errCode = 1068;
            msg = "Using Bag as key not supported.";
            throw new JobCreationException(msg, errCode, PigException.INPUT);

        default:
            errCode = 2036;
            msg = "Unhandled key type " + DataType.findTypeName(keyType);
            throw new JobCreationException(msg, errCode, PigException.BUG);
        }
    }

    private void setupDistributedCacheForJoin(MapReduceOper mro,
            PigContext pigContext, Configuration conf) throws IOException {       
                    
        new JoinDistributedCacheVisitor(mro.mapPlan, pigContext, conf)
                .visit();
             
        new JoinDistributedCacheVisitor(mro.reducePlan, pigContext, conf)
                .visit();
    }

    private static void setupDistributedCache(PigContext pigContext,
                                              Configuration conf, 
                                              Properties properties, String key, 
                                              boolean shipToCluster) 
    throws IOException {
        // Set up the DistributedCache for this job        
        String fileNames = properties.getProperty(key);
        
        if (fileNames != null) {
            String[] paths = fileNames.split(",");
            setupDistributedCache(pigContext, conf, paths, shipToCluster);
        }
    }
        
    private static void setupDistributedCache(PigContext pigContext,
            Configuration conf, String[] paths, boolean shipToCluster) throws IOException {
        // Turn on the symlink feature
        DistributedCache.createSymlink(conf);
            
        for (String path : paths) {
            path = path.trim();
            if (path.length() != 0) {
                Path src = new Path(path);
                
                // Ensure that 'src' is a valid URI
                URI srcURI = null;
                try {
                    srcURI = new URI(src.toString());
                } catch (URISyntaxException ue) {
                    int errCode = 6003;
                    String msg = "Invalid cache specification. " +
                    "File doesn't exist: " + src;
                    throw new ExecException(msg, errCode, PigException.USER_ENVIRONMENT);
                }
                
                // Ship it to the cluster if necessary and add to the
                // DistributedCache
                if (shipToCluster) {
                    Path dst = 
                        new Path(FileLocalizer.getTemporaryPath(pigContext).toString());
                    FileSystem fs = dst.getFileSystem(conf);
                    fs.copyFromLocalFile(src, dst);
                    
                    // Construct the dst#srcName uri for DistributedCache
                    URI dstURI = null;
                    try {
                        dstURI = new URI(dst.toString() + "#" + src.getName());
                    } catch (URISyntaxException ue) {
                        byte errSrc = pigContext.getErrorSource();
                        int errCode = 0;
                        switch(errSrc) {
                        case PigException.REMOTE_ENVIRONMENT:
                            errCode = 6004;
                            break;
                        case PigException.USER_ENVIRONMENT:
                            errCode = 4004;
                            break;
                        default:
                            errCode = 2037;
                            break;
                        }
                        String msg = "Invalid ship specification. " +
                        "File doesn't exist: " + dst;
                        throw new ExecException(msg, errCode, errSrc);
                    }
                    DistributedCache.addCacheFile(dstURI, conf);
                } else {
                    DistributedCache.addCacheFile(srcURI, conf);
                }
            }
        }        
    }
    
    private static String addSingleFileToDistributedCache(
            PigContext pigContext, Configuration conf, String filename,
            String prefix) throws IOException {

        if (!FileLocalizer.fileExists(filename, pigContext)) {
            throw new IOException(
                    "Internal error: skew join partition file "
                    + filename + " does not exist");
        }
                     
        String symlink = filename;
                     
        // XXX Hadoop currently doesn't support distributed cache in local mode.
        // This line will be removed after the support is added by Hadoop team.
        if (pigContext.getExecType() != ExecType.LOCAL) {
            symlink = prefix + "_" 
                    + Integer.toString(System.identityHashCode(filename)) + "_"
                    + Long.toString(System.currentTimeMillis());
            filename = filename + "#" + symlink;
            setupDistributedCache(pigContext, conf, new String[] { filename },
                    false);  
        }
         
        return symlink;
    }
    
    private static class JoinDistributedCacheVisitor extends PhyPlanVisitor {
                 
        private PigContext pigContext = null;
                
         private Configuration conf = null;
         
         public JoinDistributedCacheVisitor(PhysicalPlan plan, 
                 PigContext pigContext, Configuration conf) {
             super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
                     plan));
             this.pigContext = pigContext;
             this.conf = conf;
         }
         
         @Override
        public void visitFRJoin(POFRJoin join) throws VisitorException {
             
             // XXX Hadoop currently doesn't support distributed cache in local mode.
             // This line will be removed after the support is added
             if (pigContext.getExecType() == ExecType.LOCAL) return;
             
             // set up distributed cache for the replicated files
             FileSpec[] replFiles = join.getReplFiles();
             ArrayList<String> replicatedPath = new ArrayList<String>();
             
             FileSpec[] newReplFiles = new FileSpec[replFiles.length];
             
             // the first input is not replicated
             for (int i = 0; i < replFiles.length; i++) {
                 // ignore fragmented file
                 String symlink = "";
                 if (i != join.getFragment()) {
                     symlink = "pigrepl_" + join.getOperatorKey().toString() + "_"
                         + Integer.toString(System.identityHashCode(replFiles[i].getFileName()))
                         + "_" + Long.toString(System.currentTimeMillis()) 
                         + "_" + i;
                     replicatedPath.add(replFiles[i].getFileName() + "#"
                             + symlink);
                 }
                 newReplFiles[i] = new FileSpec(symlink, 
                         (replFiles[i] == null ? null : replFiles[i].getFuncSpec()));               
             }
             
             join.setReplFiles(newReplFiles);
             
             try {
                 setupDistributedCache(pigContext, conf, replicatedPath
                         .toArray(new String[0]), false);
             } catch (IOException e) {
                 String msg = "Internal error. Distributed cache could not " +
                               "be set up for the replicated files";
                 throw new VisitorException(msg, e);
             }
         }
         
         @Override
         public void visitMergeJoin(POMergeJoin join) throws VisitorException {
             
        	 // XXX Hadoop currently doesn't support distributed cache in local mode.
             // This line will be removed after the support is added
             if (pigContext.getExecType() == ExecType.LOCAL) return;
             
             String indexFile = join.getIndexFile();
             
             // merge join may not use an index file
             if (indexFile == null) return;
             
             try {
                String symlink = addSingleFileToDistributedCache(pigContext,
                        conf, indexFile, "indexfile_");
                join.setIndexFile(symlink);
            } catch (IOException e) {
                String msg = "Internal error. Distributed cache could not " +
                        "be set up for merge join index file";
                throw new VisitorException(msg, e);
            }
         }
         
         @Override
        public void visitMergeCoGroup(POMergeCogroup mergeCoGrp)
                throws VisitorException {
          
             // XXX Hadoop currently doesn't support distributed cache in local mode.
             // This line will be removed after the support is added
             if (pigContext.getExecType() == ExecType.LOCAL) return;
             
             String indexFile = mergeCoGrp.getIndexFileName();
             
             if (indexFile == null) throw new VisitorException("No index file");
             
             try {
                String symlink = addSingleFileToDistributedCache(pigContext,
                        conf, indexFile, "indexfile_mergecogrp_");
                mergeCoGrp.setIndexFileName(symlink);
            } catch (IOException e) {
                String msg = "Internal error. Distributed cache could not " +
                        "be set up for merge cogrp index file";
                throw new VisitorException(msg, e);
            }
        }
     }
    
    /**
     * Update the stats for every candidate plan
     * then
     * decide which plans to keep and which ones to throw away
     * @param jobID
     * @param jobStats
     * @param jobExecTimes
	 * @author iman
     */
    public void updateCandidatePlanStats(String  jobID,
    		JobStats jobStats, Long jobExecTimes,JobGraph jGraph,Map<String,String> jobIDMap) {
    	if(jobID==null){
    		//no job to update
    		return;
    	}
    	log.debug("Update shared plan stat information for job with id"+jobID);
    	
    	if(!this.isOptimizeBySharing){
    		return;
    	}
		//get candidate job with the same assigned job id
		//MapReduceOper mro = candidatePlansToShare.get(jobID);
		//SharedMapReducePlan candidateSharedPlan=new SharedMapReducePlan(mro,null,null);
    	Pair<SharedMapReducePlan,Vector<SharedMapReducePlan>> planAndSubPlans=candidatePlansToShare.get(jobID);
    	
    	if(planAndSubPlans!=null){
    		log.debug("found a candidate shared stored for the jobid");
    		
	    	SharedMapReducePlan candidateSharedPlan=planAndSubPlans.first;
	    	Vector<SharedMapReducePlan> candidateDiscoveredPlans=planAndSubPlans.second;
			//SharedMapReducePlan candidateSharedPlan=candidatePlansToShare.get(jobID);
	    	//SharedMapReducePlan candidateSharedPlan=new SharedMapReducePlan(jobPlan,null,null);
	    	
	    	//if plan has a related job... then grab info from that related job
	    	if(candidateSharedPlan.isDiscoveredPlan() || (!candidateSharedPlan.isDiscoveredPlan()&&isOptimizeBySharingWholePlan)){
		    	List<String> relatedJobIDs=candidateSharedPlan.getJobForInputInfo();
		    	if(relatedJobIDs!=null){
			    	for(String relatedJobID:relatedJobIDs){
				    	String relatedJobIDAssigned=jobIDMap.get(relatedJobID);
				    	if(relatedJobID!=null){
				    		//grab that job and update the info for shared plan
				    		Iterator<JobStats> iter = jGraph.iterator();
			                while (iter.hasNext()) {
			                    JobStats js = iter.next();
			                    String jsJobId=js.getJobId();
			                    if(jsJobId.equals(relatedJobIDAssigned)){
			                    	long sharedPlanBytesRead=js.getHdfsBytesRead();
			                    	long sharedPlanAvgTime=js.getAvgMapTime();
			                    	candidateSharedPlan.setHdfsBytesRead(sharedPlanBytesRead);
					                candidateSharedPlan.setAvgPlanTime(sharedPlanAvgTime);
			                    	break;
			                    }
			                }
				    	}
			    	}
		    	}
				//update the plan that we are going to share with its collected stats
				candidateSharedPlan.updateStats(jobStats);
				//insert plan into list of shared plans
				insertIntoSharedPlans(candidateSharedPlan);
	    	}
			//for every proposed subplan to share, update its stats and insert into list of shared plans
			if(candidateDiscoveredPlans!=null){
				for(SharedMapReducePlan subPlan:candidateDiscoveredPlans){
					//TODO update stats
					//update the plan that we are going to share with its collected stats
					subPlan.updateDiscoveredPlanStats(jobStats);
					//insert plan into list of shared plans
					insertIntoSharedPlans(subPlan);
				}
			}
			//remove it from the candidate plans
	        //candidatePlansToShare.remove(jobID);
    	}
	}

	private void insertIntoSharedPlans(SharedMapReducePlan candidateSharedPlan) {
		//iterate through the shared plans to put the new cand shared plan in its order
		int i=0;
		while(i<sharedPlans.size()){
			SharedMapReducePlan existingPlan=sharedPlans.get(i);
			if(candidateSharedPlan.isBetterPlan(existingPlan)){
				break;
			}
			i++;
		}
        sharedPlans.insertElementAt(candidateSharedPlan,i);
	}

	public void findSharingOpportunities(MROperPlan plan, List<MapReduceOper> roots) throws JobCreationException {
		if(roots==null || roots.size()==0){
			return;
		}
		List<MapReduceOper> sucessorAllMROs=new ArrayList<MapReduceOper>();
		try {
	        for (MapReduceOper mro: roots) {
	            if(mro instanceof NativeMapReduceOper) {
	                return ;
	            }
	            
	            if(this.isOptimizeBySharing){
	            	/*for(SharedMapReducePlan sharedPlan: sharedPlans){
	            		MapReduceOper sharedMRPlan=sharedPlan.getMRPlan();
	            		MapReduceOper planReplacedWithView=null;
	            		boolean noExecForMRO=false;
						if(sharedMRPlan.isEquivalent(mro)){
	        				System.out.println("Found an equivalent view to this plan");
	        				//get the successor MR plan for mro
	        				List<MapReduceOper> successorMROs = this.plan.getSuccessors(mro);
	        				if(successorMROs==null|| successorMROs.size()<=0){
	        					//there are no successors to this plan, we only need to have a special condition to copy the 
	        					//o/p to the new location and terminate
	        					//TODO
	        					log.info("Found an exact match view, copy output to new location");
	        					POStore sharedPlanStoreLoc=sharedMRPlan.getStore(sharedMRPlan);//getPlanFinalStoreLocation(sharedMRPlan);
	    						POStore newPlanStoreLoc=mro.getStore(mro);//getPlanFinalStoreLocation(mro);
	    						copyResultLocations(sharedPlanStoreLoc,newPlanStoreLoc);
	        					//check if the output of this job currently exist
	        					
	        				}else{
		        				//update successor MR plans with the new load replacing the old tmp one
		        				for (MapReduceOper successorMRO: successorMROs) {
		        					successorMRO.updateLoadOperator(mro,sharedMRPlan);
		        				}
		        				//update the successors of these operators
		        				sucessorAllMROs.addAll(successorMROs);
	        				}
	        				//@iman remove this plan from the list of plans
	        				//plan.trimBelow(mro);
	        	            plan.remove(mro);
	        	            
	        				noExecForMRO=true;
	        				//@iman set the first iter to be false as at least one iter has been performed
	        		        
	        				//bestSharedPlanSoFar=null; //make sure it is null, so we will not execute that parts that handles shared plans again
	        				break;
	        				
	        				//end found an equivalent view of the plan
	        			}else{
	        				//the plan is not equivalent with the view, check if it is subset of it
	        				planReplacedWithView=mro.getPlanRecplacedWithView(sharedMRPlan,pigContext);
	        				if(planReplacedWithView!=null ){//&& (bestSharedPlanSoFar==null || sharedPlan.isBetterPlan(bestSharedPlanSoFar))
	        					System.out.println("Found an view that is a subset of this plan");
	        					//get the successor MR plan for mro
	            				List<MapReduceOper> successorMROs = plan.getSuccessors(mro);
	            				if(planReplacedWithView==sharedMRPlan){
	            					//the new plan is the same as the shared plan after changing the store location
	            					if(successorMROs==null|| successorMROs.size()<=0){
	            						// only copy the files in the old store location to the new one
	            						log.info("To copy the files from the old location to the new one");
	            						POStore sharedPlanStoreLoc=sharedMRPlan.getStore(sharedMRPlan);//getPlanFinalStoreLocation(sharedMRPlan);
	            						POStore newPlanStoreLoc=mro.getStore(mro);//getPlanFinalStoreLocation(mro);
	            						copyResultLocations(sharedPlanStoreLoc,newPlanStoreLoc);
	            						//@iman remove this plan from the list of plans
	                    				//plan.trimBelow(mro);
	                    	            plan.remove(mro);
	            						//set flag that we will not exec mro
		        						noExecForMRO=true;
		        						//@iman set the first iter to be false as at least one iter has been performed
		        				        
	            					}else{
	            						log.info("To update successor MR tim hortons hot chocolate halalplans by merging the new mro into their maps");
		    	        				//update successor MR plans by merging the new mro into their maps
		    	        				for (int i=0;i<successorMROs.size();i++) {
		    	        					MapReduceOper successorMRO= successorMROs.get(i);
		    	        					//TODO replace the load operator with the location of store operator from shared plan
		    	        					if(successorMRO.updateLoadOperator(mro,sharedMRPlan)==true){
		    	        						//@iman remove this plan from the list of plans
			                    				//plan.trimBelow(mro);
			                    	            plan.remove(mro);
		    	        						//set flag that we will not exec mro
		    	        						noExecForMRO=true;
		    	        						//@iman set the first iter to be false as at least one iter has been performed
		    	        				        
		    	        					}
		    	        				}
		    	        				
		    	        				sucessorAllMROs.addAll(successorMROs);
		            				}
	            					break;
	            				}else{
	            					log.info("the differences in the shared plan and the new plan are more than store location ");
	            					log.info("the shared plan is subsumed in this plan");
	            					log.info("therefore, we replace the last shared op with a load from the store loc of the shared plan");
	            					log.info("the new rewritten plan is executed and nothing will be changed for subsecuent MR plans");
	            					//store info about sharedPlan bytes read and execution time
	            					//sharedPlanBytesRead=sharedPlan.getHdfsBytesRead();
	            					//sharedPlanAvgTime=sharedPlan.getAvgPlanTime();
	            					//the differences in the shared plan and the new plan are more than store location
	            					
		            				
	            					if(successorMROs!=null && successorMROs.size()>0){
	            						sucessorAllMROs.addAll(successorMROs);
	            					}
	            				}
	            				//print the shared plan that is used for rewritting the query
	            				if(this.isMoreDebugInfo){
		            				boolean verbose = true;
		                    		PrintStream ps=System.out;
		                            ps.println("#--------------------------------------------------");
		                            ps.println("# The plan that we will use to rewrite the query:  ");
		                            ps.println("#--------------------------------------------------");
		                            MROperPlan execPlan= new MROperPlan();
		                            execPlan.add(sharedMRPlan);
		                            MRPrinter printer = new MRPrinter(ps, execPlan);
		                            printer.setVerbose(verbose);
		                            try {
		                				printer.visit();
		                			} catch (VisitorException e) {
		                				log.warn("Unable to print shared plan", e);
		                			}
	            				}
		        				//break;
	        				}//end if the shared plan is subset of the plan i am currently executing
	        			}//end if we could not find an equivalent view and therefore we try to make a replacement using the shared plan 
	            	}//end for every shared plan*/
	            	
	            	this.rewriteQueryUsingSharedPlans(plan, mro, sharedPlans);
				}//is optimize by sharing
	            
	        }
	        
	        //finished all mros, now process the next level of mros
	        findSharingOpportunities(plan, sucessorAllMROs);
		}catch(Exception e) {
            int errCode = 2017;
            String msg = "Internal error creating job configuration.";
            throw new JobCreationException(msg, errCode, PigException.BUG, e);
        }
	} 
	
	public void injectSores(MROperPlan plan, List<MapReduceOper> roots) throws JobCreationException {
		if(!isDiscoverNewPlansOn){
			return;
		}
		if(roots==null || roots.size()==0){
			return;
		}
		List<MapReduceOper> sucessorAllMROs=new ArrayList<MapReduceOper>();
		try {
	        for (MapReduceOper mro: roots) {
	            if(mro instanceof NativeMapReduceOper) {
	                return ;
	            }
	            
	            List<MapReduceOper> successorMROs = plan.getSuccessors(mro);
	            if(successorMROs!=null){
	            	sucessorAllMROs.addAll(successorMROs);
	            }
	            
	            //inject stores in this mro 
	            if(this.isUseDiscovePlanHeuristicsReducer){
	            	mro.discoverUsefulSubplansReducer(pigContext, conf, plan);
	            }else{
	            	mro.discoverUsefulSubplans(pigContext, conf, plan);
	            }
	        }
	       
	        
	        //inject stores in the successor mros
	        injectSores(plan,sucessorAllMROs);
		}catch(Exception e) {
            int errCode = 2017;
            String msg = "Internal error creating job configuration.";
            throw new JobCreationException(msg, errCode, PigException.BUG, e);
        }
	} 
}
