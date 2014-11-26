package cws.core.algorithms.heterogeneous;

import java.util.List;

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
import cws.core.algorithms.AlgorithmStatistics;
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
public class Simple extends HeterogeneousAlgorithm  implements Scheduler{

    public Simple(double budget, double deadline, List<DAG> dags,
                  AlgorithmStatistics algorithmStatistics,
                  CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, algorithmStatistics, cloudsim);

        // Only for single dag, not an ensemble.
        assert(dags.size() == 1);
    }

    @Override
    public long getPlanningnWallTime() {
        return 0;
    }

    @Override
    protected void simulateInternal() {

    }

    @Override
    public void scheduleJobs(WorkflowEngine engine) {

    }

}
