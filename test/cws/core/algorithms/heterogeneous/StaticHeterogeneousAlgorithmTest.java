package cws.core.algorithms.heterogeneous;


import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import static java.util.Arrays.asList;
import java.util.SortedSet;
import java.util.TreeMap;

import org.cloudbus.cloudsim.core.CloudSim;
import cws.core.Cloud;
import cws.core.EnsembleManager;

import cws.core.Scheduler;
import cws.core.VM;
import cws.core.VMFactory;
import cws.core.VMListener;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.algorithms.AlgorithmStatistics;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;
import cws.core.jobs.Job;
import cws.core.jobs.Job.Result;
import cws.core.jobs.JobListener;
import cws.core.provisioner.ConstantDistribution;


import cws.core.algorithms.Plan;
import cws.core.algorithms.Plan.Resource;
import cws.core.algorithms.Plan.Solution;
import cws.core.algorithms.Plan.Slot;
import cws.core.algorithms.Plan.NoFeasiblePlan;

public class StaticHeterogeneousAlgorithmTest {

    @Before
    public void init() {
        CloudSim.init(0, null, false);
    }

    @Test
    public void testVMStarted() {

        // Very simple DAG with one task
        DAG dag = new DAG();
        dag.addTask(new Task("a", "", 1.0));

        CloudSimWrapper cloudsim = mock(CloudSimWrapper.class);
        StaticHeterogeneousAlgorithm algo = makeAlgo(dag, cloudsim);

        // Create and set up the plan
        algo.plan();

        // Check that the VM was launched
        verify(cloudsim, times(1)).send(anyInt(), anyInt(), 
                eq(0.0), eq(WorkflowEvent.VM_LAUNCH), any());

        // And that the dag was submitted
        verify(cloudsim, times(1)).send(anyInt(), anyInt(),
                eq(0.0), eq(WorkflowEvent.DAG_SUBMIT), any(DAGJob.class));
    }

    private StaticHeterogeneousAlgorithm makeAlgo(DAG dag, CloudSimWrapper cloudsim) {

        VMType vmtype = VMTypeBuilder.newBuilder().mips(1).
                cores(1).price(1.0)
                .provisioningTime(new ConstantDistribution(0.0))
                .deprovisioningTime(new ConstantDistribution(0.0))
                .build();
        
        // Make algorithm. TrivialPlanner only uses one VM so only add one.
        StaticHeterogeneousAlgorithm algo = new StaticHeterogeneousAlgorithm.Builder(
                asList(dag),
                new TrivialPlanner(),
                cloudsim)
                .addInitialVMs(asList(vmtype))
                .build();

        algo.setWorkflowEngine(mock(WorkflowEngine.class));
        algo.setCloud(mock(Cloud.class));
        algo.setEnsembleManager(mock(EnsembleManager.class));

        return algo;
    }

}
