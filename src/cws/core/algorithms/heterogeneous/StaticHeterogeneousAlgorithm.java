package cws.core.algorithms.heterogeneous;

import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeMap;

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

import cws.core.algorithms.Plan;
import cws.core.algorithms.Plan.Resource;
import cws.core.algorithms.Plan.Solution;
import cws.core.algorithms.Plan.Slot;
import cws.core.algorithms.Plan.NoFeasiblePlan;


/**
 * A base class for static scheduling algorithms that do not assume that
 * all VMs are identical.
 *
 * @author David Shepherd
 */
public class StaticHeterogeneousAlgorithm extends HeterogeneousAlgorithm implements VMListener, JobListener, Scheduler {

    // Changes from StaticAlgorithm:

    // Also I've decided to remove the construction of the Cloud,
    // WorkflowEngine, and EnsembleManager from prepareEnvironment(). This
    // should really be done externally (and doing it externally should
    // help make the code more testable and flexible).

    // Planning is delegated to a Planner object, this makes
    // it *much* easier to test the planning algorithm.

    // I'm not going to handle ensembles of dags just yet (it would
    // make the planning a little more complex).

    // Most of the rest of the code is the same, except for the removal of
    // helper functions which don't make sense without an Environment
    // instance. Possibly the classes should be re-merged at some point.

    /** Plan */
    private Plan plan = new Plan();

    /** List of DAGs that were admitted to run */
    private final List<DAG> admittedDAGs = new LinkedList<DAG>();

    /** Set of jobs that are ready to run arranged by Task */
    private final HashMap<Task, Job> readyJobs = new HashMap<Task, Job>();

    /** Mapping of Task to the VM that will run the task */
    private final HashMap<Task, VM> taskMap = new HashMap<Task, VM>();

    /** Schedule of tasks for each VM */
    private final HashMap<VM, LinkedList<Task>> vmQueues = new HashMap<VM, LinkedList<Task>>();

    /** Set of idle VMs */
    private final HashSet<VM> idleVms = new HashSet<VM>();

    /** The plan containig the initially allocated VMs (as VMTypes) */
    private final Plan initialPlan;

    /** The class responsible for creating the plan */
    private Planner planner;

    private StaticHeterogeneousAlgorithm(Builder builder) {
        super(builder.budget, builder.deadline,
                builder.dags, builder.ensembleStatistics,
                builder.cloudsim);

        this.initialPlan = builder.initialPlan;
        this.planner = builder.planner;
    }

    /** Store planning time taken in nanos */
    private Long planningTime = null;

    @Override
    public long getPlanningnWallTime() {
        return planningTime;
    }


    // The following methods are used before the simulation starts to
    // create a plan/schedule.
    // ============================================================

    /**
     * Develop a plan for running as many DAGs as we can from the list of
     * DAGs provided to the constructor.
     */
    public void plan() {

        if (this.getAllDags().size() > 1) {
            throw new RuntimeException(
                    "Only tested with single DAGs, but try removing this throw if you like");
        }

        // We assume the dags are in priority order, try to generate a plan
        // for each DAG.
        Plan plan = new Plan(this.initialPlan);
        for (DAG dag : getAllDags()) {
            plan = planDAG(dag, plan);
        }

        allocatePlannedResources(plan);

        // Submit the admitted DAGs
        for (DAG dag : admittedDAGs) {
            submitDAG(dag);
        }
    }

    /**
     * Develop a plan for a single DAG
     */
    private Plan planDAG(DAG dag, Plan currentPlan) {

        Plan newPlan;
        try {
            // Create the plan
            newPlan = this.planner.planDAG(dag, currentPlan);

        } catch (NoFeasiblePlan m) {
            // If no good plan could be found then record it and return the
            // original plan
            getCloudsim().log("Rejecting DAG: " + m.getMessage());
            return currentPlan;
        }


        // Reject if too expensive
        if (newPlan.getCost() > getBudget()) {
            getCloudsim().log("Rejecting DAG: New plan exceeds budget: "
                    + newPlan.getCost());
            return currentPlan;
        } else {
            // Plan was feasible, accept it
            this.admittedDAGs.add(dag);
            getCloudsim().log("Admitting DAG. Cost of new plan: " + plan.getCost());
            return newPlan;
        }
    }


    private void allocatePlannedResources(Plan plan) {

        for (Resource r : plan.resources) {
            // create VM
            VMType vmType = r.vmtype;
            VM vm = VMFactory.createVM(vmType, getCloudsim());

            // Build task<->vm mappings
            LinkedList<Task> vmQueue = new LinkedList<Task>();
            vmQueues.put(vm, vmQueue);
            for (Slot slot : r.getSlots()) {
                Task task = slot.task;
                this.taskMap.put(task, vm);
                vmQueue.add(task);
            }

            // Launch the VM at its appointed time
            getProvisioner().launchVMAtTime(vm, r.getStart());

            // We don't actually force the VM to terminate at it's
            // termination time, instead we allow the VM to be terminated
            // natuarlly when it runs out of jobs. If the planner has done
            // its job this should be before the termination time.
        }
    }


    private void submitDAG(DAG dag) {
        // Wrap into a job which also stores owner ID
        DAGJob dagJob = new DAGJob(dag, getEnsembleManager().getId());

        // Priority is based on order of input list
        dagJob.setPriority(getAllDags().indexOf(dag));

        // Submit the dag
        getCloudsim().send(getEnsembleManager().getId(),
                getWorkflowEngine().getId(),
                0.0, WorkflowEvent.DAG_SUBMIT, dagJob);
    }


    // These methods handle running the actual simulation
    // ============================================================

    @Override
    public void simulateInternal() {
        prepareEnvironment();

        final long planningStartWallTime = System.nanoTime();

        plan();

        final long planningFinishWallTime = System.nanoTime();
        planningTime = planningFinishWallTime - planningStartWallTime ;

        getCloudsim().startSimulation();
    }

    private void prepareEnvironment() {
        getProvisioner().getCloud().addVMListener(this);
        this.getWorkflowEngine().addJobListener(this);
    }



    // The following methods control the behaviour while the simulation is
    // running. They are called through the VMListener and JobListener
    // interfaces and are used to automatically send jobs to the
    // appropriate VMs based on the plan.
    // ============================================================


    @Override
    public void scheduleJobs(WorkflowEngine engine) {
        // Just clear any jobs that were queued
        engine.getQueuedJobs().clear();
    }

    @Override
    public void vmLaunched(VM vm) {
        idleVms.add(vm);
        submitNextTaskFor(vm);
    }

    @Override
    public void vmTerminated(VM vm) {
        idleVms.remove(vm);
    }

    @Override
    public void jobReleased(Job job) {
        Task task = job.getTask();

        // Mark the job ready
        readyJobs.put(task, job);

        // Try to submit the next task
        VM vm = taskMap.get(task);
        submitNextTaskFor(vm);
    }

    @Override
    public void jobSubmitted(Job job) {
    }

    @Override
    public void jobStarted(Job job) {
    }

    @Override
    public void jobFinished(Job job) {
        VM vm = job.getVM();

        // Sanity check
        DAG dag = job.getDAGJob().getDAG();
        if (!admittedDAGs.contains(dag)) {
            throw new RuntimeException("Running DAG that wasn't accepted");
        }

        // If the task failed, retry it on the same VM
        if (job.getResult() == Result.FAILURE) {
            // We need to re-add the task to the VM's queue here.
            // The workflow engine will take care of releasing a new Job
            // for the task, we just have to be ready for it when the next
            // task for this VM is submitted at the end of this method.
            LinkedList<Task> queue = vmQueues.get(vm);
            queue.addFirst(job.getTask());
        }

        idleVms.add(vm);
        submitNextTaskFor(vm);
    }

    private void submitNextTaskFor(VM vm) {
        // If the VM is busy, do nothing
        if (!idleVms.contains(vm)) {
            return;
        }

        // Get next task for VM
        LinkedList<Task> vmqueue = vmQueues.get(vm);
        Task task = vmqueue.peek();
        if (task == null) {
            // No more tasks
            getProvisioner().terminateVM(vm);
        } else {
            // If job for task is ready
            if (readyJobs.containsKey(task)) {
                // Submit job
                Job next = readyJobs.get(task);
                submitJob(vm, next);
            }
        }
    }

    private void submitJob(VM vm, Job job) {
        Task task = job.getTask();

        // Advance queue
        LinkedList<Task> vmqueue = vmQueues.get(vm);
        Task next = vmqueue.poll();
        if (next != task) {
            throw new RuntimeException("Not next task");
        }

        // Remove the job from the ready queue
        if (!readyJobs.containsKey(task)) {
            throw new RuntimeException("Task not ready");
        }
        readyJobs.remove(task);

        // Submit the job to the VM
        idleVms.remove(vm);
        job.setVM(vm);
        vm.jobSubmit(job);
    }

    static public class Builder {

        // Effectively infinite as the default
        private double budget = Double.MAX_VALUE;
        private double deadline = Double.MAX_VALUE;

        private List<DAG> dags;
        private Planner planner;
        private Plan initialPlan = new Plan();
        private Map<VMType, Integer> vmNumbers;
        private CloudSimWrapper cloudsim;
        private AlgorithmStatistics ensembleStatistics;


        public Builder(List<DAG> dags, Planner planner, CloudSimWrapper cloudsim) {
            this.dags = dags;
            this.planner = planner;
            this.cloudsim = cloudsim;
        }

        public Builder budget(double budget) {
            this.budget = budget;
            return this;
        }

        public Builder deadline(double deadline) {
            this.deadline = deadline;
            return this;
        }

        public Builder addInitialVMs(List<VMType> vms) {
            for(VMType vmt : vms) {
                this.initialPlan.resources.add(new Resource(vmt));
            }
            return this;
        }

        public Builder initialPlan(Plan plan) {
            this.initialPlan = plan;
            return this;
        }

        public StaticHeterogeneousAlgorithm build() {

            this.ensembleStatistics =
                    new AlgorithmStatistics(dags, budget, deadline, cloudsim);

            StaticHeterogeneousAlgorithm built =
                    new StaticHeterogeneousAlgorithm(this);

            return built;
        }
    }
}
