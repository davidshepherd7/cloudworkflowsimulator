package cws.core;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.jobs.IdentityRuntimeDistribution;
import cws.core.jobs.RuntimeDistribution;
import cws.core.jobs.UniformRuntimeDistribution;

public class VMFactory {
    private static final double DEFAULT_RUNTIME_VARIANCE = 0.0;
    private static final double DEFAULT_FAILURE_RATE = 0.0;

    private static FailureModel failureModel = new FailureModel(0, 0.0);
    private static double runtimeVariance;
    private static double failureRate;

    public static FailureModel getFailureModel() {
        return failureModel;
    }

    public static void setFailureModel(FailureModel failureModel) {
        VMFactory.failureModel = failureModel;
    }

    /**
     * @param cloudSimWrapper - initialized CloudSimWrapper instance. It needs to be inited, because we're creating
     *            storage manager here.
     */
    public static VM createVM(VMType vmType, CloudSimWrapper cloudSimWrapper) {
        return new VM(vmType, cloudSimWrapper, failureModel);
    }

    public static void buildCliOptions(Options options) {
        Option failureRate = new Option("fr", "failure-rate", true, "Faliure rate, defaults to " + DEFAULT_FAILURE_RATE);
        failureRate.setArgName("RATE");
        options.addOption(failureRate);
    }

    public static void readCliOptions(CommandLine args, long seed) {
        failureRate = Double.parseDouble(args.getOptionValue("failure-rate", DEFAULT_FAILURE_RATE + ""));

        System.out.printf("failureRate = %f\n", failureRate);

        if (failureRate > 0.0) {
            VMFactory.setFailureModel(new FailureModel(seed, failureRate));
        }
    }

    public static double getFailureRate() {
        return failureRate;
    }
}
