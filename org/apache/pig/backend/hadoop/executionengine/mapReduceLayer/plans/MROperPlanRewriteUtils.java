package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.hadoop.executionengine.HJob;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.SharedMapReducePlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;

public class MROperPlanRewriteUtils {

	private static final String TMPFILES_JOB_DFN = "tmpfiles.job.dfn";
	private static final String RESTORE_ON = "restore.on";
	private static final String RESTORE_REUSE_PRIOR_OUTPUTS = "restore.reusePriorOutputs";
	private static final String RESTORE_DISCOVER_PLANS = "restore.discoverPlans";
	
	
	//private PigContext pigContext;
	private static String tmpFileJobDefnLocationDefault="/home/ashraf/sharedPigPlans.plans";

	/*MROperPlanRewriteUtils(PigContext pigContext){
		this.pigContext=pigContext;
		
	}*/
	/**
     * Load the table with jobs and their tmp file
     * @throws IOException 
     * @throws ClassNotFoundException 
	 * @author iman
     */
    static private Vector<SharedMapReducePlan> loadJobDfnForTmpFiles(PigContext pigContext) throws IOException, ClassNotFoundException {
    	
		//String tmpFileJobDefnLocation = conf.get(TMPFILES_JOB_DFN);
    	String tmpFileJobDefnLocation = pigContext.getProperties().getProperty(TMPFILES_JOB_DFN, tmpFileJobDefnLocationDefault);
		
		//load the job definitions
    	Vector<SharedMapReducePlan> sharedPlans;
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
		
		return sharedPlans;
	}
    
    public static MROperPlan rewriteWithSharedDefault(MROperPlan planToRewrite, Vector<SharedMapReducePlan> sharedPlans, Vector<Boolean> selectedSharedPlans, PigContext pigContext) throws VisitorException, PlanException, CloneNotSupportedException{
    	
    	//check ReStoreOn and ReusePriorOutpots is on
    	boolean isReStoreON = pigContext.getProperties().getProperty(RESTORE_ON, "false").equals("true");
    	/*String isReStoreONProp = pigContext.getProperties().getProperty(RESTORE_ON);
    	if(isReStoreONProp!=null && isReStoreONProp.equalsIgnoreCase("true")){
    		isReStoreON = true;
    	}*/
    	boolean isReusePriorOutput = pigContext.getProperties().getProperty(RESTORE_REUSE_PRIOR_OUTPUTS, "false").equals("true");
    	/*String isReusePriorOutputProp = pigContext.getProperties().getProperty(RESTORE_REUSE_PRIOR_OUTPUTS);
    	if(isReusePriorOutputProp!=null && isReusePriorOutputProp.equalsIgnoreCase("true")){
    		isReusePriorOutput = true;
    	}*/
    	
    	
    	
    	//MROperPlan rewrittenPlan=null;
    	MROperPlan plan =  null;
    	//if not optimize by sharing then exit
    	if(isReStoreON && isReusePriorOutput ){
	    	//get the shared plans 
	    	Vector<SharedMapReducePlan> loadedSharedPlans=null;
			try {
				loadedSharedPlans = loadJobDfnForTmpFiles(pigContext);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
	    	if(loadedSharedPlans!=null){
	    		sharedPlans.addAll(loadedSharedPlans);
	    		for(SharedMapReducePlan sharedPlan:loadedSharedPlans){
	    			selectedSharedPlans.add(false);
	    		}
	    		
	    	}
	    	
	    	plan =  planToRewrite.clone();
	    	Vector<OperatorKey> seenPlans=new Vector<OperatorKey>();
	    	
	    	//iterate through the plan and try to find sharing opportunities
	    	boolean changeMade=true;
	    	while(changeMade){
	    		changeMade=false;
		    	List<MapReduceOper> roots = new LinkedList<MapReduceOper>();
		    	for(MapReduceOper root:plan.getRoots()){
		    		if(!seenPlans.contains(root.getOperatorKey())){
		    			roots.add(root);
		    		}
		    	}
		        //for each mapReduce plan, 
		        for (MapReduceOper mro: roots) {
		        	//add it to the seen list
		        	seenPlans.add(mro.getOperatorKey());
		        	
		        	//try to rewrite it using shared plans
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
	        					System.out.println("Found an exact match view");
	        					//check if the output of this job currently exist
	        					
	        				}else{
		        				//update successor MR plans with the new load replacing the old tmp one
		        				for (MapReduceOper successorMRO: successorMROs) {
		        					successorMRO.updateLoadOperator(mro,sharedMRPlan);
		        				}
	        				}
	        				
	        				//remove the node from the plan
	        				plan.remove(mro);
	        				
	        				//a change was made to the plan
	        				changeMade=true;
	        				int indexOfSharedPlans = sharedPlans.indexOf(sharedPlan);
	        				if(indexOfSharedPlans >= 0){
	        					selectedSharedPlans.set(indexOfSharedPlans, true);
	        				}
	        				
	        				break;
	            		}else{//check if the mro can be rewritten to use one of the shared plans
	            			
	            			planReplacedWithView=mro.getPlanRecplacedWithView(sharedMRPlan,pigContext);
	            			
	            			if(planReplacedWithView!=null ){
	        					System.out.println("Found a view that is a subset of this plan");
	        					
	        					//get the successor MR plan for mro
	            				List<MapReduceOper> successorMROs = plan.getSuccessors(mro);
	            				
	            				if(planReplacedWithView==sharedMRPlan){
	            					
	            					//the new plan is the same as the shared plan after changing the store location
	            					if(successorMROs==null|| successorMROs.size()<=0){
	            						// only copy the files in the old store location to the new one
	            						System.out.println("To copy the files from the old location to the new one");
	            						System.out.println("This is just an attempt, so we do not need to do anything!");
	            						//POStore sharedPlanStoreLoc=sharedMRPlan.getStore(sharedMRPlan);//getPlanFinalStoreLocation(sharedMRPlan);
	            						//POStore newPlanStoreLoc=mro.getStore(mro);//getPlanFinalStoreLocation(mro);
	            						//copyResultLocations(sharedPlanStoreLoc,newPlanStoreLoc);
	            					}else{
	            						System.out.println("To update successor MR plans by merging the new mro into their maps");
		    	        				//update successor MR plans by merging the new mro into their maps
		    	        				for (int i=0;i<successorMROs.size();i++) {
		    	        					MapReduceOper successorMRO= successorMROs.get(i);
		    	        					//replace the load operator with the location of store operator from shared plan
		    	        					successorMRO.updateLoadOperator(mro,sharedMRPlan);
		    	        				}
	            					}
	            					
	            					//remove the node from the plan
	    	        				plan.remove(mro);
	    	        				
	            					//a change was made to the plan
	    	        				changeMade=true;
	    	        				int indexOfSharedPlans = sharedPlans.indexOf(sharedPlan);
	    	        				if(indexOfSharedPlans >= 0){
	    	        					selectedSharedPlans.set(indexOfSharedPlans, true);
	    	        				}
	    	        				
	    	        				break;
	            				}else{
	            					
	            					
	            					//a change was made to the plan
	    	        				changeMade=true;
	    	        				int indexOfSharedPlans = sharedPlans.indexOf(sharedPlan);
	    	        				if(indexOfSharedPlans >= 0){
	    	        					selectedSharedPlans.set(indexOfSharedPlans, true);
	    	        				}
	    	        				
	    	        				//would continue with other shared plans, may be we can
	    	        				//still find another shared plan that can be used to rewrite
	    	        				//the plan in shand
	            				}
	            				
	            				
	            				
	            			}
	            		}
		        	}
		        }
	    	}
    	}
    	return plan;
    }
    
    
    /*private void copyResultLocations(POStore oldPlanStore, POStore newPlanStore) throws IOException {
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
	}*/
    
    
    public static MROperPlan rewriteWithShared(MROperPlan planToRewrite, Vector<SharedMapReducePlan> sharedPlans, Vector<SharedMapReducePlan> selectedSharedPlans, PigContext pigContext) throws VisitorException, PlanException, CloneNotSupportedException{
    	//check ReStoreOn and ReusePriorOutpots is on
    	boolean isReStoreON = pigContext.getProperties().getProperty(RESTORE_ON, "false").equals("true");
    	
    	boolean isReusePriorOutput = pigContext.getProperties().getProperty(RESTORE_REUSE_PRIOR_OUTPUTS, "false").equals("true");
    	
    	//MROperPlan rewrittenPlan=null;
    	MROperPlan plan =  null;
    	//if not optimize by sharing then exit
    	if(isReStoreON && isReusePriorOutput ){
    		
    		plan =  planToRewrite.clone();
	    	Vector<OperatorKey> seenPlans=new Vector<OperatorKey>();
	    	
	    	
	    	//iterate through the plan and try to find sharing opportunities
	    	boolean changeMade=true;
	    	while(changeMade){
	    		changeMade=false;
		    	List<MapReduceOper> roots = new LinkedList<MapReduceOper>();
		    	for(MapReduceOper root:plan.getRoots()){
		    		if(!seenPlans.contains(root.getOperatorKey())){
		    			roots.add(root);
		    		}
		    	}
		        //for each mapReduce plan, 
		        for (MapReduceOper mro: roots) {
		        	//add it to the seen list
		        	seenPlans.add(mro.getOperatorKey());
		        	
		        	//try to rewrite it using shared plans
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
	        					System.out.println("Found an exact match view");
	        					//check if the output of this job currently exist
	        					
	        				}else{
		        				//update successor MR plans with the new load replacing the old tmp one
		        				for (MapReduceOper successorMRO: successorMROs) {
		        					successorMRO.updateLoadOperator(mro,sharedMRPlan);
		        				}
	        				}
	        				
	        				//remove the node from the plan
	        				plan.remove(mro);
	        				
	        				//a change was made to the plan
	        				changeMade=true;
	        				selectedSharedPlans.add(sharedPlan);
	        				
	        				
	        				break;
	            		}else{//check if the mro can be rewritten to use one of the shared plans
	            			
	            			planReplacedWithView=mro.getPlanRecplacedWithView(sharedMRPlan,pigContext);
	            			
	            			if(planReplacedWithView!=null ){
	        					System.out.println("Found a view that is a subset of this plan");
	        					
	        					//get the successor MR plan for mro
	            				List<MapReduceOper> successorMROs = plan.getSuccessors(mro);
	            				
	            				if(planReplacedWithView==sharedMRPlan){
	            					
	            					//the new plan is the same as the shared plan after changing the store location
	            					if(successorMROs==null|| successorMROs.size()<=0){
	            						// only copy the files in the old store location to the new one
	            						System.out.println("To copy the files from the old location to the new one");
	            						System.out.println("This is just an attempt, so we do not need to do anything!");
	            						//POStore sharedPlanStoreLoc=sharedMRPlan.getStore(sharedMRPlan);//getPlanFinalStoreLocation(sharedMRPlan);
	            						//POStore newPlanStoreLoc=mro.getStore(mro);//getPlanFinalStoreLocation(mro);
	            						//copyResultLocations(sharedPlanStoreLoc,newPlanStoreLoc);
	            					}else{
	            						System.out.println("To update successor MR plans by merging the new mro into their maps");
		    	        				//update successor MR plans by merging the new mro into their maps
		    	        				for (int i=0;i<successorMROs.size();i++) {
		    	        					MapReduceOper successorMRO= successorMROs.get(i);
		    	        					//replace the load operator with the location of store operator from shared plan
		    	        					successorMRO.updateLoadOperator(mro,sharedMRPlan);
		    	        				}
	            					}
	            					
	            					//remove the node from the plan
	    	        				plan.remove(mro);
	    	        				
	            					//a change was made to the plan
	    	        				changeMade=true;
	    	        				selectedSharedPlans.add(sharedPlan);
	    	        				
	    	        				
	    	        				break;
	            				}else{
	            					
	            					
	            					//a change was made to the plan
	    	        				changeMade=true;
	    	        				selectedSharedPlans.add(sharedPlan);
	    	        				
	    	        				//would continue with other shared plans, may be we can
	    	        				//still find another shared plan that can be used to rewrite
	    	        				//the plan in shand
	            				}
	            				
	            				
	            				
	            			}
	            		}
		        	}
		        }
	    	}
    	}
    	
    	return plan;
    }

    public static MROperPlan rewriteToInjectStores(MROperPlan planToRewrite, PigContext pigContext, 
    		MultiMap<String, MapReduceOper> storedPlans){
    	boolean isReStoreON = pigContext.getProperties().getProperty(RESTORE_ON, "false").equals("true");
    	
    	boolean isDiscoverPlans = pigContext.getProperties().getProperty(RESTORE_DISCOVER_PLANS, "false").equals("true");
    	
    	MROperPlan plan =  null;
    	if(isReStoreON && isDiscoverPlans ){
    		plan =  planToRewrite.clone();
    		
    		Vector<MapReduceOper> planOps=new Vector<MapReduceOper>();
    		Vector<MapReduceOper> seen=new Vector<MapReduceOper>();
    		List<MapReduceOper> roots = new LinkedList<MapReduceOper>();
    		for(MapReduceOper root:plan.getRoots()){
	    		if(!seen.contains(root.getOperatorKey())){
	    			roots.add(root);
	    		}
	    	}
    		planOps.addAll(roots);
    		
    		//while(seen.size() < plan.size()){
    			Vector<MapReduceOper> planOpsSucc=new Vector<MapReduceOper>();
				//for each mapReduce plan, 
		        for (MapReduceOper mro: planOps) {
		        
		        	seen.add(mro);
		        	try {
		        		MultiMap<String, MapReduceOper> storedPlansTmp = mro.discoverSubplans(pigContext, plan);
		        		if(storedPlansTmp!=null){
							Set<String> storedPlanOuts = storedPlansTmp.keySet();
							for(String storedPlanOut: storedPlanOuts){
								storedPlans.put(storedPlanOut, storedPlansTmp.get(storedPlanOut));
							}
						}
					} catch (VisitorException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (PlanException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (CloneNotSupportedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		        	
		        	//get successors of this operator and add it to the list of successors
		        	List<MapReduceOper> planOpSucc = plan.getSuccessors(mro);
		        	if(planOpSucc!=null){
			        	for(MapReduceOper succ:planOpSucc){
			        		if(seen.contains(succ)){
			        			//an error must have occured!!
			        			System.out.println("error: how come a succ has already been seen!!");
			        		}
			        		//if all pred of the succ has already been seen, then we can add it to the succ
			        		//if it is not already added
			        		List<MapReduceOper> succPreds=plan.getPredecessors(succ);
			        		boolean haveSeenAllPreds = true;
			        		for(MapReduceOper succPred:succPreds){
			        			if(!seen.contains(succPred)){
			        				haveSeenAllPreds=false;
			        				break;
			        			}
			        		}
			        		if(haveSeenAllPreds){
			        			//add succ to the list that will be examined next
			        			planOpsSucc.add(succ);
			        		}
			        	}
		        	}
		        }
		        
		        //set planOps to the successors that need to be examined next iteration
		        planOps.removeAllElements();
		        //planOps.addAll(planOpsSucc);
    		//}
    	}
    	return plan;
    }

	public static void updateRepository(PigStats stats, MultiMap<String, MapReduceOper> discoveredPlans,
			Vector<SharedMapReducePlan> sharedPlans,
			Vector<Boolean> selectedSharedPlans, PigContext pigContext) throws VisitorException {
		
		//if restore is on and discover shared plans is on
		boolean isReStoreON = pigContext.getProperties().getProperty(RESTORE_ON, "false").equals("true");
		boolean isDiscoverPlans = pigContext.getProperties().getProperty(RESTORE_DISCOVER_PLANS, "false").equals("true");
		if(!isReStoreON || !isDiscoverPlans){
			return;
		}
		
		
		Vector<SharedMapReducePlan> sharedPlansToKeep = new Vector<SharedMapReducePlan>();
		//add shared plans to it
		if(sharedPlans!=null){
			sharedPlansToKeep.addAll(sharedPlans);
		}
		
		//get jobs from stats
		List<JobStats> jobStats=getjobStatsFromStats(stats, pigContext);
		
		//for each discovered plan, iterate through the jobs to get its info
		for(JobStats job:jobStats){
			for (OutputStats output : job.getOutputs()) {
				String jobOutFileName =output.getPOStore().getSFile().getFileName();
				
				Collection<MapReduceOper> discoveredPlanColl = discoveredPlans.get(jobOutFileName);
				if(discoveredPlanColl!=null){
					for(MapReduceOper discoveredPlan:discoveredPlanColl){
						LinkedList<POStore> stores=new LinkedList<POStore>();
						//get Stores
						if(!discoveredPlan.mapPlan.isEmpty()){
							//get stores from the reduce plan
							LinkedList<POStore> storesTmp=PlanHelper.getStores(discoveredPlan.mapPlan);
							if(storesTmp!=null){
								stores.addAll(storesTmp);
							}
						}
						if(!discoveredPlan.reducePlan.isEmpty()){
							//get stores from the reduce plan
							LinkedList<POStore> storesTmp=PlanHelper.getStores(discoveredPlan.reducePlan);
							if(storesTmp!=null){
								stores.addAll(storesTmp);
							}
						}
						
						if(stores==null || stores.isEmpty()){
							List<POStore> storesTmp=getStores(discoveredPlan);
							if(storesTmp!=null){
								stores.addAll(storesTmp);
							}
						}
						for(POStore store:stores){
							if(store.getSFile().getFileName().equals(jobOutFileName)){
								//create a shared plan 
								SharedMapReducePlan candidateSharedPlan=new SharedMapReducePlan(discoveredPlan,store,jobOutFileName);
								//update the plan that we are going to share with its collected stats
								candidateSharedPlan.updateStats(job);
								//insert plan into list of shared plans
								insertIntoSharedPlans(candidateSharedPlan,sharedPlansToKeep);
							}
						}
						
					}
				}
			}
			
		}
		
		//finally dump the shared plans into the file
		dumpSharedPlans(sharedPlansToKeep, pigContext);
	}

	private static List<POStore> getStores(MapReduceOper discoveredPlan) {
		List<PhysicalOperator> leaves=new LinkedList<PhysicalOperator>();
		List<POStore> stores=new LinkedList<POStore>();
		
		if(discoveredPlan!=null){
			if(discoveredPlan.mapPlan!=null && !discoveredPlan.mapPlan.isEmpty()){
				List<PhysicalOperator> leavesTmp=discoveredPlan.mapPlan.getLeaves();
				if(leavesTmp!=null){
					leaves.addAll(leavesTmp);
				}
			}
			
			if(discoveredPlan.reducePlan!=null && !discoveredPlan.reducePlan.isEmpty()){
				List<PhysicalOperator> leavesTmp=discoveredPlan.reducePlan.getLeaves();
				if(leavesTmp!=null){
					leaves.addAll(leavesTmp);
				}
			}
		}
		
		if(leaves!=null){
			for(PhysicalOperator leaf:leaves){
				if(leaf instanceof POStore){
					stores.add((POStore) leaf);
				}
			}
		}
		return stores;
	}

	private static void insertIntoSharedPlans(
			SharedMapReducePlan candidateSharedPlan,
			Vector<SharedMapReducePlan> sharedPlansToKeep) {

		//iterate through the shared plans to put the new cand shared plan in its order
				int i=0;
				while(i<sharedPlansToKeep.size()){
					SharedMapReducePlan existingPlan=sharedPlansToKeep.get(i);
					if(candidateSharedPlan.isBetterPlan(existingPlan)){
						break;
					}
					i++;
				}
				sharedPlansToKeep.insertElementAt(candidateSharedPlan,i);
	}

	private static List<JobStats> getjobStatsFromStats(PigStats stats, PigContext pigContext) {
		LinkedList<JobStats> jobs = new LinkedList<JobStats>();
        JobGraph jGraph = stats.getJobGraph();
        Iterator<JobStats> iter = jGraph.iterator();
        while (iter.hasNext()) {
            JobStats js = iter.next();
            
            jobs.add(js);
        }
		return jobs;
	}

	
	private static List<ExecJob> getjobsFromStats(PigStats stats, PigContext pigContext) {
		LinkedList<ExecJob> jobs = new LinkedList<ExecJob>();
        JobGraph jGraph = stats.getJobGraph();
        Iterator<JobStats> iter = jGraph.iterator();
        while (iter.hasNext()) {
            JobStats js = iter.next();
            
            for (OutputStats output : js.getOutputs()) {
                if (js.isSuccessful()) {                
                    jobs.add(new HJob(HJob.JOB_STATUS.COMPLETED, pigContext, output
                            .getPOStore(), output.getAlias(), stats));
                } else {
                    HJob hjob = new HJob(HJob.JOB_STATUS.FAILED, pigContext, output
                            .getPOStore(), output.getAlias(), stats);
                    hjob.setException(js.getException());
                    jobs.add(hjob);
                }
            }
        }
		return jobs;
	}


	
	
	
	/**
     * dump shared plans discovered in this query
	 * @author iman
     */
    public static void dumpSharedPlans(Vector<SharedMapReducePlan>sharedPlans, PigContext pigContext){
    	boolean  isMoreDebugInfo = true;
    	/*if(!this.isOptimizeBySharing){
    		return;
    	}*/
    	//print the existing plans
    	if(isMoreDebugInfo){
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
					//log.warn("Unable to print shared plan", e);
				}
				ps.println("plan stats:");
				ps.println("bytes read: " + sharedPlan.getHdfsBytesRead()+" bytes written: "+sharedPlan.getHdfsBytesWritten()+" read/write= "+sharedPlan.getReadWriteRatio());
				ps.println("avg time taken by this job "+sharedPlan.getAvgPlanTime());
				System.out.println();
	    	}
    	}
    	
    	String tmpFileJobDefnLocation = pigContext.getProperties().getProperty(TMPFILES_JOB_DFN, tmpFileJobDefnLocationDefault);//conf.get(TMPFILES_JOB_DFN);
    	if(tmpFileJobDefnLocation==null){
			tmpFileJobDefnLocation="/home/ashraf/sharedPigPlans.plans";
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
				//log.error("Could not create/find the file of shared plans", e);
			} catch (IOException e) {
				//log.error("Unable to write/serialize shared plans set", e);
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
				//log.error("Could not create/find the file of shared plans", e);
			} catch (IOException e) {
				//log.error("Unable to write/serialize shared plans set", e);
			}
	        

		}
    }
}
