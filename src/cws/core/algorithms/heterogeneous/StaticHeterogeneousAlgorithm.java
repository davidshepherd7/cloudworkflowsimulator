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

    // We don't inherit from Provisioner. It's simpler and clearer to use a
    // NullProvisioner instead in my opinion.

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


    public StaticHeterogeneousAlgorithm(double budget, double deadline,
            List<DAG> dags, AlgorithmStatistics ensembleStatistics,
            Planner planner, Plan initialPlan,
            CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, ensembleStatistics, cloudsim);

        this.initialPlan = initialPlan;
        this.planner = planner;
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

        if(this.getAllDags().size() > 1) {
            throw new RuntimeException("Can only handle single DAGs");
        }

        // We assume the dags are in priority order, try to generate a plan
        // for each DAG.
        Plan plan = new Plan(this.initialPlan);
        for (DAG dag : getAllDags()) {
            try {
                // Create the plan
                Plan newPlan = planDAG(dag, plan);

                // Plan was feasible, accept it
                if (newPlan.getCost() <= getBudget()) {
                    admittedDAGs.add(dag);
                    plan = newPlan;
                    getCloudsim().log("Admitting DAG. Cost of new plan: " + plan.getCost());

                    // Reject if too expensive
                } else {
                    getCloudsim().log("Rejecting DAG: New plan exceeds budget: " + newPlan.getCost());
                }
                // Or if no plan could be found
            } catch (NoFeasiblePlan m) {
                getCloudsim().log("Rejecting DAG: " + m.getMessage());
            }
        }

        // Now allocate the resources specified in the plan
        for (Resource r : plan.resources) {
            // create VM
            VMType vmType = r.vmtype;
            VM vm = VMFactory.createVM(vmType, getCloudsim());

            // Build task<->vm mappings
            LinkedList<Task> vmQueue = new LinkedList<Task>();
            vmQueues.put(vm, vmQueue);
            for (Slot slot : r.getSlots()) {
                Task task = slot.task;
                taskMap.put(task, vm);
                vmQueue.add(task);
            }

            // Launch the VM at its appointed time
            launchVM(vm, r.getStart());
        }

        // Submit the admitted DAGs
        for (DAG dag : admittedDAGs) {
            submitDAG(dag);
        }
    }

    /**
     * Develop a plan for a single DAG
     */
    Plan planDAG(DAG dag, Plan currentPlan) throws NoFeasiblePlan {

        // Error checks
        if(dag.getTasks().length == 0) {
            throw new RuntimeException("No tasks in dag");
        }

        return this.planner.planDAG(dag, currentPlan);
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


    private void launchVM(VM vm, double start) {
        double now = getCloudsim().clock();
        double delay = start - now;
        getCloudsim().send(getWorkflowEngine().getId(), getCloud().getId(),
                delay, WorkflowEvent.VM_LAUNCH, vm);
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
        this.getCloud().addVMListener(this);
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
            getCloudsim().send(getWorkflowEngine().getId(), getCloud().getId(), 0.0, WorkflowEvent.VM_TERMINATE, vm);
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
}
