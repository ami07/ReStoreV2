package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.ReverseDependencyOrderWalker;
import org.apache.pig.impl.util.Pair;

public class QuerySharingOptimizer {

	MROperPlan plan;
    Configuration conf;
    PigContext pigContext;
    
    private final Log log = LogFactory.getLog(getClass());
    
    public static final String PIG_STORE_CONFIG = "pig.store.config";

    public static final String LOG_DIR = "_logs";

    public static final String END_OF_INP_IN_MAP = "pig.invoke.close.in.map";
    
    // A mapping of job to pair of store locations and tmp locations for that job
    private Map<Job, Pair<List<POStore>, Path>> jobStoreMap;
    
	public QuerySharingOptimizer(PigContext pigContext, Configuration conf) throws IOException {
		this.pigContext = pigContext;
        this.conf = conf;
        jobStoreMap = new HashMap<Job, Pair<List<POStore>, Path>>();
	}
	
	public MROperPlan discoverSharinpOpportunities(){
		return null;
	}

}
