package cws.core.algorithms.heterogeneous;

import java.util.List;

import cws.core.Cloud;
import cws.core.EnsembleManager;
import cws.core.Scheduler;
import cws.core.VM;
import cws.core.VMFactory;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.engine.Environment;
import cws.core.Provisioner;

import cws.core.algorithms.AlgorithmStatistics;

public class DynamicHeterogeneousAlgorithm extends HeterogeneousAlgorithm {

    public DynamicHeterogeneousAlgorithm(double budget, double deadline,
            List<DAG> dags,
            CloudSimWrapper cloudsim) {
        super(budget, deadline, dags,
                new AlgorithmStatistics(dags, budget, deadline, cloudsim),
                cloudsim);
    }

    @Override
    public void simulateInternal() {
        prepareEnvironment();
        getCloudsim().startSimulation();
        // getProvisioner().terminateAllVMs();
    }

    private void prepareEnvironment() {

        getProvisioner().provisionInitialResources(getWorkflowEngine());
    }

    @Override
    public long getPlanningnWallTime() {
        // planning is always 0 for dynamic algorithms
        return 0;
    }
}
