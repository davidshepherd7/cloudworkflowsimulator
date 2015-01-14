package cws.core.algorithms.heterogeneous;


import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
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
import cws.core.Provisioner;
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

    Provisioner provisioner;
    WorkflowEngine engine;

    @Before
    public void init() {
        // We need cloudsim initialised to avoid problems with registering
        // CWSSimEntity based classes.
        CloudSim.init(0, null, false);

        // Need a mock provisioner to check that VMs are launched
        provisioner = mock(Provisioner.class);

        // Need this so that we can get the provisioner
        engine = mock(WorkflowEngine.class);
        when(engine.getProvisioner()).thenReturn(provisioner);
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
        verify(provisioner, times(1)).launchVMAtTime(any(VM.class), anyDouble());

        // And that the dag was submitted
        verify(cloudsim, times(1)).send(anyInt(), anyInt(),
                eq(0.0), eq(WorkflowEvent.DAG_SUBMIT), any(DAGJob.class));
    }


    @Test
    public void testVMStartTime() {

        // Very simple DAG with one task
        DAG dag = new DAG();
        dag.addTask(new Task("a", "", 1.0));

        CloudSimWrapper cloudsim = mock(CloudSimWrapper.class);

        VMType vmtype = VMTypeBuilder.newBuilder().mips(1).
                cores(1).price(1.0)
                .provisioningTime(new ConstantDistribution(0.0))
                .deprovisioningTime(new ConstantDistribution(0.0))
                .build();

        final double startTime = 10;
        final double stopTime = 20;
        Plan initialPlan = new Plan();
        initialPlan.resources.add(new Resource(vmtype, 10, 20));

        // Make algorithm. TrivialPlanner only uses one VM so only add one.
        StaticHeterogeneousAlgorithm algo = new StaticHeterogeneousAlgorithm.Builder(
                asList(dag),
                new TrivialPlanner(),
                cloudsim)
                .initialPlan(initialPlan)
                .build();

        algo.setWorkflowEngine(engine);
        algo.setCloud(mock(Cloud.class));
        algo.setEnsembleManager(mock(EnsembleManager.class));

        // Create and set up the plan
        algo.plan();

        // Check that the VM would be started at the right time
        verify(provisioner, times(1)).launchVMAtTime(any(VM.class),
                doubleThat(greaterThanOrEqualTo(startTime)));

        // Harder to check stop time, would need a full integration test
        // really... Problem is that the termination signal is sent after
        // lots of other stuff happens.

        // Actually we don't necessarily stop at the stop time anyway, e.g.
        // if a Job overruns we don't kill the VM.
    }

    // @Test
    // public void testVMQueues() {
    //     // Check that by calling jobFinished, jobReleased we can get all tasks sent to VM,
    //     // then get the VM terminated.


    //     // Very simple DAG
    //     DAG dag = new DAG();
    //     dag.addTask(new Task("a", "", 1.0));

    //     CloudSimWrapper cloudsim = mock(CloudSimWrapper.class);
    //     StaticHeterogeneousAlgorithm algo = makeAlgo(dag, cloudsim);

    //     // Create and set up the plan
    //     algo.plan();

    //     VM vm = mock(VM.class);

    //     // // Release and finish all jobs
    //     Job job = mock(Job.class);
    //     when(job.getVM()).thenReturn(vm);
    //     when(job.getResult()).thenReturn(Result.SUCCESS);
    //     // when(job.getDAGJob()).theReturn();

    //     algo.jobReleased(job);
    //     algo.jobFinished(job);

    //     // Job job2 = mock(Job.class);
    //     // algo.jobReleased(job2);
    //     // algo.jobFinished(job2);

    //     // Job job3 = mock(Job.class);
    //     // algo.jobReleased(job3);
    //     // algo.jobFinished(job3);


    //     // // Check that 3 tasks were sent
    //     // verify(cloudsim, times(3)).send(anyInt(), anyInt(),
    //     //         eq(0.0), eq(WorkflowEvent.VM_TERMINATE), any());

    //     // Check that the VM was terminated
    //     verify(cloudsim, times(1)).send(anyInt(), anyInt(),
    //             eq(0.0), eq(WorkflowEvent.VM_TERMINATE), any());
    // }


    // ??ds Check that vms are stopped correctly


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

        algo.setWorkflowEngine(engine);
        algo.setCloud(mock(Cloud.class));
        algo.setEnsembleManager(mock(EnsembleManager.class));

        return algo;
    }

}
