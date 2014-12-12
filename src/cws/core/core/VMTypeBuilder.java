package cws.core.core;

import org.cloudbus.cloudsim.distributions.ContinuousDistribution;

import cws.core.provisioner.ConstantDistribution;

import cws.core.jobs.RuntimeDistribution;
import cws.core.jobs.IdentityRuntimeDistribution;

public class VMTypeBuilder {
    private static final double DEFAULT_BILLING_TIME = 3600;
    private static final long DEFAULT_CACHE_SIZE = 100000000;

    private static final ContinuousDistribution DEFAULT_PROVISIONING_DELAY = new ConstantDistribution(0.0);
    private static final ContinuousDistribution DEFAULT_DEPROVISIONING_DELAY = new ConstantDistribution(10.0);

    private static final double DEFAULT_POWER_CONSUMPTION = 50.0;
    
    private static final RuntimeDistribution DEFAULT_RUNTIME_DISTRIBUTION
        = new IdentityRuntimeDistribution();

    public static MipsStep newBuilder() {
        return new Steps();
    }

    public interface MipsStep {
        CoresStep mips(double mips);
    }

    public interface CoresStep {
        PriceStep cores(int cores);
    }

    public interface PriceStep {
        OptionalsStep price(double price);
    }

    public interface OptionalsStep {
        OptionalsStep billingTimeInSeconds(double billingTimeInSeconds);

        OptionalsStep provisioningTime(ContinuousDistribution provisioningTime);

        OptionalsStep deprovisioningTime(ContinuousDistribution deprovisioningTime);

        OptionalsStep cacheSize(long cacheSize);

        OptionalsStep powerConsumptionInWatts(double powerConsumptionInWatts);

        OptionalsStep runtimeDistribution(RuntimeDistribution runtimeDistribution);
        
        VMType build();
    }

    static class Steps implements MipsStep, CoresStep, PriceStep, OptionalsStep {
        private double mips;
        private int cores;
        private double price;

        private double billingTimeInSeconds = DEFAULT_BILLING_TIME;
        private ContinuousDistribution provisioningTime = DEFAULT_PROVISIONING_DELAY;
        private ContinuousDistribution deprovisioningTime = DEFAULT_DEPROVISIONING_DELAY;
        private long cacheSize = DEFAULT_CACHE_SIZE;
        private double powerConsumptionInWatts = DEFAULT_POWER_CONSUMPTION;
        private RuntimeDistribution runtimeDistribution = DEFAULT_RUNTIME_DISTRIBUTION;

        @Override
        public CoresStep mips(double mips) {
            this.mips = mips;
            return this;
        }

        @Override
        public PriceStep cores(int cores) {
            this.cores = cores;
            return this;
        }

        @Override
        public OptionalsStep price(double price) {
            this.price = price;
            return this;
        }

        @Override
        public OptionalsStep billingTimeInSeconds(double billingTimeInSeconds) {
            this.billingTimeInSeconds = billingTimeInSeconds;
            return this;
        }

        @Override
        public OptionalsStep provisioningTime(ContinuousDistribution provisioningTime) {
            this.provisioningTime = provisioningTime;
            return this;
        }

        @Override
        public OptionalsStep deprovisioningTime(ContinuousDistribution deprovisioningTime) {
            // TODO(mequrel): add checks for >= 0.0 somewhere
            this.deprovisioningTime = deprovisioningTime;
            return this;
        }

        @Override
        public OptionalsStep cacheSize(long cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }

        @Override
        public OptionalsStep powerConsumptionInWatts(double powerConsumptionInWatts) {
            this.powerConsumptionInWatts = powerConsumptionInWatts;
            return this;
        }

        public OptionalsStep runtimeDistribution(RuntimeDistribution runtimeDistribution) {
            this.runtimeDistribution = runtimeDistribution;
            return this;
        }

        @Override
        public VMType build() {
            return new VMType(mips, cores, price, billingTimeInSeconds,
                    provisioningTime, deprovisioningTime, cacheSize,
                    powerConsumptionInWatts, runtimeDistribution);
        }
    }
}
