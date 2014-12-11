package cws.core;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;

import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.exception.UnknownWorkflowEventException;
import cws.core.jobs.Job;
import cws.core.jobs.RuntimeDistribution;

/**
 * A VM is a virtual machine that executes Jobs.
 * 
 * It has a number of cores, and each core has a certain power measured
 * in MIPS (millions of instructions per second).
 * 
 * It has an input Port that is used to transfer data to the VM, and an output
 * Port that is used to transfer data from the VM. Both ports have the same
 * bandwidth.
 * 
 * Jobs can be queued and are executed in FIFO order. The scheduling is
 * space shared.
 * 
 * It has a price per billing unit. The cost of a VM is computed by multiplying the
 * runtime in billing units by the billing unit price. The runtime is rounded up to the
 * nearest billing unit for this calculation.
 * 
 * Each VM has a provisioning delay between when it is launched and when it
 * is ready, and a deprovisioning delay between when it is terminated and
 * when the provider stops charging for it.
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class VM extends CWSSimEntity {

    private static int nextId = 0;

    /** Contains VM parameters like cores number, price for billing unit **/
    private final VMType vmType;

    /** The SimEntity that owns this VM */
    private int owner = -1;

    /** The Cloud that runs this VM */
    private int cloud = -1;

    /** Current idle cores */
    private int idleCores;

    /** Queue of jobs submitted to this VM */
    private LinkedList<Job> jobs;

    /** Set of jobs currently running */
    private Set<Job> runningJobs;

    /** Time that the VM was launched */
    private double launchTime;

    /** Time that the VM was terminated */
    private double terminateTime;

    /** Has this VM been terminated? */
    private boolean isTerminated;

    /** Has this VM been started? */
    private boolean isLaunched;

    /** Varies the actual runtime of tasks according to the specified distribution */
    private final RuntimeDistribution runtimeDistribution;

    /** Varies the failure rate of tasks according to a specified distribution */
    private final FailureModel failureModel;

    /** Read intervals of all jobs. */
    private final Map<Job, Interval> readIntervals = new HashMap<Job, VM.Interval>();

    /** Write intervals of all jobs. */
    private final Map<Job, Interval> writeIntervals = new HashMap<Job, VM.Interval>();

    /** Computation intervals of all jobs. */
    private final Map<Job, Interval> computationIntervals = new HashMap<Job, VM.Interval>();

    VM(VMType vmType, CloudSimWrapper cloudsim, FailureModel failureModel, RuntimeDistribution runtimeDistribution) {
        super("VM" + (nextId++), cloudsim);
        this.vmType = vmType;
        this.jobs = new LinkedList<Job>();
        this.runningJobs = new HashSet<Job>();
        this.idleCores = vmType.getCores();
        this.launchTime = -1.0;
        this.terminateTime = -1.0;
        this.isTerminated = false;
        this.isLaunched = false;
        this.failureModel = failureModel;
        this.runtimeDistribution = runtimeDistribution;
    }

    /**
     * Returns true when this VM is not executing any job.
     */
    public boolean isFree() {
        if (isTerminated) {
            throw new IllegalStateException(
                    "Attempted to determine whether terminated VM is free. Check for termination first.");
        }
        return idleCores == 1;
    }

    /**
     * Runtime of the VM in seconds. If the VM has not been launched, then
     * the result is 0. If the VM is not terminated, then we use the current
     * simulation time as the termination time. After the VM is terminated
     * the runtime does not change.
     */
    public double getRuntime() {
        if (launchTime < 0)
            return 0.0;
        else if (terminateTime < 0)
            return getCloudsim().clock() - launchTime;
        else
            return terminateTime - launchTime;
    }

    /**
     * Compute the total cost of this VM. This is computed by taking the
     * runtime, rounding it up to the nearest whole billing unit, and multiplying
     * by the billing unit price.
     */
    public double getCost() {
        double billingUnits = getRuntime() / vmType.getBillingTimeInSeconds();
        double fullBillingUnits = Math.ceil(billingUnits);
        return fullBillingUnits * vmType.getPriceForBillingUnit();
    }

    @Override
    public void processEvent(CWSSimEvent ev) {
        if (!isTerminated) {
            switch (ev.getTag()) {
            case WorkflowEvent.VM_LAUNCH:
                launch();
                break;
            case WorkflowEvent.VM_TERMINATE:
                terminate();
                break;
            case WorkflowEvent.JOB_SUBMIT:
                jobSubmit((Job) ev.getData());
                break;
            case WorkflowEvent.JOB_FINISHED:
                jobFinish((Job) ev.getData());
                break;
            case WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED:
                allInputsTrasferred((Job) ev.getData());
                break;
            case WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED:
                allOutputsTransferred((Job) ev.getData());
                break;
            default:
                throw new UnknownWorkflowEventException("Unknown event: " + ev);
            }
        } else {
            if (ev.getTag() == WorkflowEvent.VM_LAUNCH || ev.getTag() == WorkflowEvent.JOB_SUBMIT) {
                throw new IllegalStateException("Attempted to send launch or submit event to terminated VM:"
                        + this.getId());
            }
        }
    }

    /**
     * Launches this VM.
     */
    void launch() {
        Preconditions.checkState(!isLaunched, "Attempted to launch already launched VM:" + this.getId());
        isLaunched = true;
        getCloudsim().log(String.format("VM %d started", getId()));
    }

    /**
     * Terminates this VM. A VM cannot be terminated twice.
     */
    void terminate() {
        Preconditions.checkState(!isTerminated, "Cannot terminate already terminated VM");
        getCloudsim().log("VM " + getId() + " is going to terminate");
        // Can no longer accept jobs
        isTerminated = true;

        // Log termination only for running jobs
        for (Job runningJob : runningJobs) {
            getCloudsim().log("Terminating job " + runningJob.getID() + " on VM " + runningJob.getVM().getId());
        }

        // Log that queued jobs were not executed
        for (Job queuedJob : jobs) {
            getCloudsim().log("Removing job " + queuedJob.getID() + " from queue on VM " + queuedJob.getVM().getId());
        }

        // Move running jobs back to the queue...
        jobs.addAll(runningJobs);
        runningJobs.clear();

        // ... and fail all queued jobs
        for (Job job : jobs) {
            job.setResult(Job.Result.FAILURE);
            getCloudsim().send(getId(), job.getOwner(), 0.0, WorkflowEvent.JOB_FINISHED, job);
        }

        // Reset dynamic state
        jobs.clear();
        idleCores = vmType.getCores();
        getCloudsim().log(String.format("VM %d terminate request success", getId()));
    }

    /**
     * Submits the given job to this VM and marks the VM as non-free.
     */
    public void jobSubmit(Job job) {
        Preconditions.checkState(!isTerminated,
                "Attempted to submit job to a terminated VM. Check for termination first.");
        Preconditions.checkState(isLaunched, "Attempted to submit job to a not launched VM.");
        job.setSubmitTime(getCloudsim().clock());
        job.setState(Job.State.IDLE);
        job.setVM(this);

        // Queue the job
        jobs.add(job);

        // This shouldn't do anything if the VM is busy
        startJobs();
    }

    private void allInputsTrasferred(Job job) {
        if (runningJobs.contains(job)) {
            throw new IllegalStateException("Job already running: " + job);
        }
        // add it to the running set
        runningJobs.add(job);

        // Compute the duration of the job on this VM
        double size = job.getTask().getSize();
        double predictedRuntime = size / vmType.getMips();

        // Compute actual runtime
        double actualRuntime = this.runtimeDistribution.getActualRuntime(predictedRuntime);

        // Decide whether the job succeeded or failed
        if (failureModel.failureOccurred()) {
            job.setResult(Job.Result.FAILURE);

            // How long did it take to fail?
            actualRuntime = failureModel.runtimeBeforeFailure(actualRuntime);
        } else {
            job.setResult(Job.Result.SUCCESS);
        }

        getCloudsim()
                .log(String
                        .format("Starting computational part of job %s (task_id = %s, workflow = %s) on VM %s. Will finish in %f",
                                job.getID(), job.getTask().getId(), job.getDAGJob().getDAG().getId(), job.getVM()
                                        .getId(), actualRuntime));

        getCloudsim().send(getId(), getId(), actualRuntime, WorkflowEvent.JOB_FINISHED, job);

        // Mark that read has finished.
        readIntervals.get(job).stop();
        // Mark that computation has started.
        computationIntervals.put(job, new Interval());
    }

    private void allOutputsTransferred(Job job) {
        if (!runningJobs.contains(job) || job.getState() != Job.State.RUNNING) {
            throw new IllegalStateException("Outputs of non-running job transferred:" + job.getID());
        }
        // remove from the running set
        runningJobs.remove(job);

        // Complete the job
        job.setFinishTime(getCloudsim().clock());
        job.setState(Job.State.TERMINATED);

        // Tell the owner
        getCloudsim().send(getId(), job.getOwner(), 0.0, WorkflowEvent.JOB_FINISHED, job);

        // The core that was running the job is now free
        idleCores++;

        // Mark that write has finished.
        writeIntervals.get(job).stop();

        // We may be able to start more jobs now
        startJobs();
    }

    private void jobStart(Job job) {
        if (job.getState() != Job.State.IDLE) {
            throw new IllegalStateException("Attempted to start non-idle job:" + job.getID());
        } else if (idleCores != 1) {
            // NOTE(bryk): Here we assume that VMs always have only one core. It should be changed once we enable more
            // cores in VMs.
            throw new IllegalStateException("Number of idle cores is not 1, actual number:" + idleCores);
        }
        getCloudsim().log("Starting " + job.toString() + " on VM " + job.getVM().getId());
        // The job is now running
        job.setStartTime(getCloudsim().clock());
        job.setState(Job.State.RUNNING);

        // Tell the owner
        getCloudsim().send(getId(), job.getOwner(), 0.0, WorkflowEvent.JOB_STARTED, job);

        getCloudsim().send(getId(), getCloudsim().getEntityId("StorageManager"), 0.0,
                WorkflowEvent.STORAGE_BEFORE_TASK_START, job);

        // One core is now busy running the job
        idleCores--;

        // Mark that read has started.
        readIntervals.put(job, new Interval());
    }

    private void jobFinish(Job job) {
        if (job.getState() != Job.State.RUNNING) {
            throw new RuntimeException("Non-running job finished:" + job.getID());
        }

        String msg = String.format("Computational part of job %s "
                + "(task_id = %s, workflow = %s, retry = %s) on VM %s finished", job.getID(), job.getTask().getId(),
                job.getDAGJob().getDAG().getId(), job.isRetry(), job.getVM().getId());
        getCloudsim().log(msg);

        getCloudsim().send(getId(), getCloudsim().getEntityId("StorageManager"), 0.0,
                WorkflowEvent.STORAGE_AFTER_TASK_COMPLETED, job);

        // Mark that computation has finished
        computationIntervals.get(job).stop();
        // Mark that write has started.
        writeIntervals.put(job, new Interval());
    }

    private void startJobs() {
        // While there are still idle jobs and cores
        while (jobs.size() > 0 && idleCores > 0) {
            // Start the next job in the queue
            jobStart(jobs.poll());
        }
    }

    public int getOwner() {
        if (owner == -1) {
            throw new IllegalStateException();
        }
        return owner;
    }

    public void setOwner(int owner) {
        this.owner = owner;
    }

    public int getCloud() {
        if (cloud == -1) {
            throw new IllegalStateException();
        }
        return cloud;
    }

    public void setCloud(int cloud) {
        this.cloud = cloud;
    }

    public int getIdleCores() {
        return idleCores;
    }

    public Job[] getQueuedJobs() {
        return jobs.toArray(new Job[0]);
    }

    public int getQueueLength() {
        return jobs.size();
    }

    public void setLaunchTime(double launchTime) {
        this.launchTime = launchTime;
    }

    public double getLaunchTime() {
        return launchTime;
    }

    public double getTerminateTime() {
        return terminateTime;
    }

    public void setTerminateTime(double terminateTime) {
        this.terminateTime = terminateTime;
    }

    public VMType getVmType() {
        return vmType;
    }

    public RuntimeDistribution getRuntimeDistribution() {
        return runtimeDistribution;
    }

    public FailureModel getFailureModel() {
        return failureModel;
    }

    public boolean isTerminated() {
        return isTerminated;
    }

    public double getProvisioningDelay() {
        return vmType.getProvisioningDelay().sample();
    }

    public double getDeprovisioningDelay() {
        return vmType.getDeprovisioningDelay().sample();
    }

    /**
     * Assumes one core VMs.
     */
    public double getTimeSpentOnComputations() {
        double time = 0;
        for (Interval interval : computationIntervals.values()) {
            time += interval.getDuration();
        }
        return time;
    }

    /**
     * Assumes one core VMs.
     */
    public double getTimeSpentOnTransfers() {
        double time = 0;
        for (Interval interval : readIntervals.values()) {
            time += interval.getDuration();
        }
        for (Interval interval : writeIntervals.values()) {
            time += interval.getDuration();
        }
        return time;
    }

    /**
     * Represents interval of time in seconds spanning from start time to end time (or VM termination time if not set).
     */
    private final class Interval {
        private final double startTime = getCloudsim().clock();
        private Double endTime;

        /**
         * Stops the interval at current simulation time.
         */
        public void stop() {
            endTime = getCloudsim().clock();
        }

        /**
         * Returns the duration in seconds of this interval.
         */
        public double getDuration() {
            double duration;
            if (endTime == null) {
                if (isTerminated) {
                    duration = terminateTime - startTime;
                } else {
                    throw new IllegalStateException("VM not terminated, but should be");
                }
            } else {
                duration = endTime - startTime;
            }
            if (duration < 0) {
                throw new IllegalStateException("Duration is < 0, but shouldn't be");
            }
            return duration;
        }
    }
}
