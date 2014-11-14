package cws.core.core;

import org.cloudbus.cloudsim.distributions.ContinuousDistribution;

import cws.core.core.power.PowerModel;
import cws.core.core.power.StaticPowerModel;

public class VMType implements Cloneable {
    /**
     * The relationship between processing power (mips) and electrical power
     * consumed for this VM
     */
    private final PowerModel powerModel;

    /**
     * The number of cores of this VM
     */
    private final int cores;

    /**
     * Price per billing unit of usage
     */
    private final double billingUnitPrice;

    /**
     * For how long we pay in advance
     */
    private final double billingTimeInSeconds;

    /**
     * Delay from when the VM is launched until it is ready
     */
    private final ContinuousDistribution provisioningDelay;

    /**
     * Delay from when the VM is terminated until it is no longer charged
     */
    private final ContinuousDistribution deprovisioningDelay;

    /**
     * The number of bytes on internal disk that can be used as a cache
     * @see {@link cws.core.storage.cache.VMCacheManager}
     */
    private final long cacheSize;

    public double getMips() {
        return powerModel.getCurrentState().mips;
    }

    public int getCores() {
        return cores;
    }

    public double getPriceForBillingUnit() {
        return billingUnitPrice;
    }

    public double getBillingTimeInSeconds() {
        return billingTimeInSeconds;
    }

    public ContinuousDistribution getProvisioningDelay() {
        return provisioningDelay;
    }

    public ContinuousDistribution getDeprovisioningDelay() {
        return deprovisioningDelay;
    }

    public long getCacheSize() {
        return cacheSize;
    }

    public VMType(double mips, int cores,
                  double billingUnitPrice, double billingTimeInSeconds,
                  ContinuousDistribution provisioningTime,
                  ContinuousDistribution deprovisioningTime, long cacheSize) {
        this.powerModel = new StaticPowerModel(0.0, mips);
        this.cores = cores;
        this.billingUnitPrice = billingUnitPrice;
        this.billingTimeInSeconds = billingTimeInSeconds;
        this.provisioningDelay = provisioningTime;
        this.deprovisioningDelay = deprovisioningTime;
        this.cacheSize = cacheSize;
    }

    public VMType(PowerModel powerModel, int cores,
                  double billingUnitPrice, double billingTimeInSeconds,
                  ContinuousDistribution provisioningTime,
                  ContinuousDistribution deprovisioningTime, long cacheSize) {
        this.powerModel = powerModel;
        this.cores = cores;
        this.billingUnitPrice = billingUnitPrice;
        this.billingTimeInSeconds = billingTimeInSeconds;
        this.provisioningDelay = provisioningTime;
        this.deprovisioningDelay = deprovisioningTime;
        this.cacheSize = cacheSize;
    }
}
