package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.PreOrderOnceWalker;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.optimizer.physical.DistributedSortedTableGlobalOrderOptimizer.WalkerCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.UnionWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedSortedTableGloabalOrderDispatcher implements Dispatcher {

	static final Logger LOG = LoggerFactory.getLogger(DistributedSortedTableGloabalOrderDispatcher.class.getName());
	private final PhysicalContext physicalContext;
	private final Map<Rule, NodeProcessor> rules;

	public DistributedSortedTableGloabalOrderDispatcher(PhysicalContext context, Map<Rule, NodeProcessor> rules) {
		super();
		physicalContext = context;
		this.rules = rules;
	}

	@Override
	public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs) throws SemanticException {
		// TODO Auto-generated method stub
		Task<? extends Serializable> task = (Task<? extends Serializable>) nd;
		if (!(task instanceof TezTask))
			return null;
		// create a the context for walking operators
		TezWork work = (TezWork) task.getWork();
		List<ReduceSinkOperator> rss = physicalContext.getParseContext()
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
