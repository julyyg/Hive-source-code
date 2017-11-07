package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.PreOrderOnceWalker;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezWork;

public class DistributedSortedTableGlobalOrderOptimizer implements PhysicalPlanResolver {

	@Override
	public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
		Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
	    opRules.put(new RuleRegExp("R1",".*"),
	    		new GlobalOrderProcessor());
	    WalkerCtx walkerctx = new WalkerCtx(pctx.getParseContext());
	    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, walkerctx);
	    GraphWalker ogw = new PreOrderOnceWalker(disp);
	    ArrayList<Node> topNodes = new ArrayList<Node>();
	    topNodes.addAll(pctx.getRootTasks());
	    ogw.startWalking(topNodes, null);
		return pctx;
	}

	static private class GlobalOrderProcessor implements NodeProcessor {

		@Override
		public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
				throws SemanticException {
			// TODO Auto-generated method stub
			Task<? extends Serializable> task = (Task<? extends Serializable>) nd;
			if (!(task instanceof TezTask))
				return null;
			// create a the context for walking operators
			TezWork work = (TezWork) task.getWork();
			assert(procCtx instanceof WalkerCtx);
			List<ReduceSinkOperator> rss = ((WalkerCtx) procCtx).getParseContext()
					.getReduceSinkOperatorsAddedByEnforceBucketingSorting();
			for (BaseWork w : ((TezWork) task.getWork()).getAllWorkUnsorted()) {
				if (w instanceof MapWork) {
					Set<Operator<?>> ops = w.getAllOperators();
					boolean candidate = false;
					for (ReduceSinkOperator rs : rss) {
						if (ops.contains(rs))
							candidate = true;
					}
					if (candidate)
						((MapWork) w).setAddedBySortedTable(true);
				} else if (w instanceof ReduceWork) {
					Set<Operator<?>> ops = w.getAllOperators();
					boolean candidate = false;
					for (ReduceSinkOperator rs : rss) {
						if (ops.contains(rs))
							candidate = true;
					}
					if (candidate)
						((ReduceWork) w).setAddedBySortedTable(true);
				}
			}
			return null;
		}
		
	}
	
	static class WalkerCtx implements NodeProcessorCtx {
	   ParseContext pctx;
	   public WalkerCtx(ParseContext pctx){
		   this.pctx = pctx;
	   }
	   public ParseContext getParseContext(){
		return this.pctx;
		   
	   }
	   public void setParseContext(ParseContext pctx) {
		   this.pctx = pctx;
	   }
	}
}

