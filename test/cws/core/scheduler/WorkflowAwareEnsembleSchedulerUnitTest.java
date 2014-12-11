package cws.core.scheduler;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.cloudbus.cloudsim.core.CloudSim;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import com.google.common.collect.ImmutableList;

import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.dag.DAG;
import cws.core.dag.DAGFile;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;
import cws.core.engine.Environment;
import cws.core.jobs.Job;

public class WorkflowAwareEnsembleSchedulerUnitTest {
    WorkflowAwareEnsembleScheduler scheduler;
    WorkflowEngine engine;
    CloudSimWrapper cloudsim;
    Environment environment;

    Queue<Job> jobs;
    List<VM> freeVMs;

    @Before
    public void setUp() throws Exception {
        CloudSim.init(0, null, false);
        cloudsim = mock(CloudSimWrapper.class);
        when(cloudsim.clock()).thenReturn(1.0);

        environment = mock(Environment.class);

        when(environment.getSingleVMPrice()).thenReturn(1.0);
        when(environment.getBillingTimeInSeconds()).thenReturn(3600.0);

        scheduler = new WorkflowAwareEnsembleScheduler(cloudsim, environment);

        engine = mock(WorkflowEngine.class);
        when(engine.getDeadline()).thenReturn(10.0);
        when(engine.getBudget()).thenReturn(10.0);

        jobs = new LinkedList<Job>();
        freeVMs = new ArrayList<VM>();

        when(engine.getQueuedJobs()).thenReturn(jobs);
        when(engine.getFreeVMs()).thenReturn(freeVMs);
    }

    @Test
    public void shouldDoNothingWithEmptyQueue() {
        freeVMs.add(createVMMock());
        // empty queues

        Queue<Job> expected = jobs;

        scheduler.scheduleJobs(engine);
        assertTrue(expected.equals(jobs));
    }

    @Test
    public void shouldScheduleFirstJobIfOneVMAvailable() {
        Job job = createSimpleJobMock();
        jobs.add(job);
        freeVMs.add(createVMMock());

        Queue<Job> expected = new LinkedList<Job>();

        when(environment.getComputationPredictedRuntime(job.getDAGJob().getDAG())).thenReturn(10.0);

        scheduler.scheduleJobs(engine);

        assertTrue(expected.equals(jobs));
    }

    @Test
    public void shouldNotScheduleIfNoVMAvailable() {
        // empty VMs
        jobs.add(createSimpleJobMock());

        Queue<Job> expected = jobs;

        scheduler.scheduleJobs(engine);
        assertTrue(expected.equals(jobs));
    }

    class IsInputTransferJob extends ArgumentMatcher<Job> {
        @Override
        public boolean matches(Object job) {
            return ((Job) job).getTask().getId().startsWith("input-gs");
        }
    }

    class IsOutputTransferJob extends ArgumentMatcher<Job> {
        @Override
        public boolean matches(Object job) {
            return ((Job) job).getTask().getId().startsWith("output-gs");
        }
    }

    private Job createSimpleJobMock(ImmutableList<DAGFile> inputs, ImmutableList<DAGFile> outputs) {
        Task task = mock(Task.class);

        DAG dag = new DAG();
        dag.addTask(task);

        DAGJob dagjob = new DAGJob(dag, 0);

        Job job = new Job(dagjob, task, -1, cloudsim);

        when(task.getInputFiles()).thenReturn(inputs);
        when(task.getOutputFiles()).thenReturn(outputs);

        when(task.getId()).thenReturn("");

        return job;
    }

    private Job createSimpleJobMock() {
        return createSimpleJobMock(ImmutableList.of(), ImmutableList.of());
    }

    private VM createVMMock() {
        VM vm = mock(VM.class);
        VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).build();
        when(vm.getVmType()).thenReturn(vmType);
        return vm;
    }
}
