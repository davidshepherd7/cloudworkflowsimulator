package cws.core.algorithms;

import java.util.List;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.engine.Environment;
import cws.core.provisioner.SimpleUtilizationBasedProvisioner;
import cws.core.scheduler.WorkflowLocalityAndStorageAwareEnsembleScheduler;

/**
 * Storage and file locality aware version of WADPDS algorithm.
 * 
 * Storage awareness here means that during task runtime estimations, file transfer estimation is taken into account.
 * 
 * File locality means that the scheduler tries to minimize the number of file transfers between tasks.
 */
public class CacheAwareStorageAwareWADPDS extends DynamicAlgorithm {
    public CacheAwareStorageAwareWADPDS(double budget, double deadline, List<DAG> dags, double maxScaling,
            AlgorithmStatistics ensembleStatistics, Environment environment, CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, new WorkflowLocalityAndStorageAwareEnsembleScheduler(cloudsim, environment),
                new SimpleUtilizationBasedProvisioner(maxScaling, cloudsim), ensembleStatistics, environment, cloudsim);
    }
}
