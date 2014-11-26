package cws.core.algorithms.heterogeneous;

import java.util.List;

import cws.core.algorithms.Algorithm;
import cws.core.algorithms.AlgorithmStatistics;
import cws.core.Cloud;
import cws.core.EnsembleManager;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.log.WorkflowLog;

import cws.core.Cloud;
import cws.core.EnsembleManager;
import cws.core.Provisioner;
import cws.core.Scheduler;
import cws.core.VM;
import cws.core.VMFactory;
import cws.core.VMListener;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;
import cws.core.dag.algorithms.CriticalPath;
import cws.core.dag.algorithms.TopologicalOrder;
import cws.core.engine.Environment;
import cws.core.jobs.Job;
import cws.core.jobs.Job.Result;
import cws.core.jobs.JobListener;

/**
 * ??ds
 *
 * @author David Shepherd
 */
public abstract class HeterogeneousAlgorithm extends Algorithm {

    public HeterogeneousAlgorithm(double budget, double deadline, List<DAG> dags,
            AlgorithmStatistics algorithmStatistics,
            CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, algorithmStatistics, cloudsim);
    }
}
