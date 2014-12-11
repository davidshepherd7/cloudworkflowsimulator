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
import cws.core.dag.DAG;
import cws.core.dag.DAGFile;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;
import cws.core.engine.Environment;
import cws.core.jobs.Job;

public class EnsembleDynamicSchedulerUnitTest {
    DAGDynamicScheduler scheduler;
    WorkflowEngine engine;
    CloudSimWrapper cloudsim;
    Queue<Job> jobs;
    List<VM> freeVMs;
    private Environment environment;

    @Before
    public void setUp() throws Exception {
        CloudSim.init(0, null, false);
        cloudsim = mock(CloudSimWrapper.class);
        environment = mock(Environment.class);

        scheduler = new EnsembleDynamicScheduler(cloudsim, environment);
        engine = mock(WorkflowEngine.class);
        when(engine.getDeadline()).thenReturn(1.0);

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
        Job job = createJobMock();
        jobs.add(job);
        freeVMs.add(createVMMock());

        Queue<Job> expected = new LinkedList<Job>();

        scheduler.scheduleJobs(engine);

        assertTrue(expected.equals(jobs));
    }

    @Test
    public void shouldNotScheduleIfNoVMAvailable() {
        // empty VMs
        jobs.add(createJobMock());

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

    private Job createJobMock(ImmutableList<DAGFile> inputs, ImmutableList<DAGFile> outputs) {
        Task task = mock(Task.class);
        Job job = new Job(new DAGJob(new DAG(), 2), task, -1, cloudsim);

        when(task.getInputFiles()).thenReturn(inputs);
        when(task.getOutputFiles()).thenReturn(outputs);

        when(task.getId()).thenReturn("");

        return job;
    }

    private Job createJobMock() {
        return createJobMock(ImmutableList.of(), ImmutableList.of());
    }

    private VM createVMMock() {
        return mock(VM.class);
    }
}
