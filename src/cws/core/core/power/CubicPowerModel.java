package cws.core.core.power;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import cws.core.core.power.PowerState;
import cws.core.core.power.PowerModel;
import cws.core.core.power.NoSuchPowerStateException;



/**
 * A power model with a number of discrete power state and whose power
 * increases cubicly as the frequency (mips) grows.
 */
public class CubicPowerModel implements PowerModel {

    private List<Double> possibleMips;
    private int currentState;

    private double basePower;
    private double powerDiff;

    public CubicPowerModel(double basePower, double powerDiff,
                           double baseMips, double maxMips,
                           int nStates, int currentState) {

        double mipsDiff = (maxMips - baseMips)/nStates;
        this.possibleMips = new ArrayList<Double>(nStates);
        for(int i=0; i<nStates; i++) {
            possibleMips.add(baseMips + i*mipsDiff);
        }

        this.basePower = basePower;
        this.powerDiff = powerDiff;

        this.currentState = currentState;
    }

    public CubicPowerModel(double basePower, double powerDiff,
                           double baseMips, double mipsDiff,
                           int nStates) {
        this(basePower, powerDiff, baseMips,  mipsDiff, nStates, 0);
    }

    // Could add arbitrary lists of mips states instead?

    public PowerState getPowerState(int stateNumber) {
        double mips = possibleMips.get(stateNumber);
        double baseMips = possibleMips.get(0);

        double power = basePower
            + powerDiff * Math.pow((mips - baseMips) / baseMips, 3);

        return new PowerState(power, mips);
    }

    @Override
    public PowerState getCurrentState() {
        return getPowerState(currentState);
    }

    @Override
    public PowerState getLowerPowerState() {
        if(!hasLowerPowerState()) return null;
        return getPowerState(currentState - 1);
    }

    @Override
    public PowerState getHigherPowerState() {
        if(!hasHigherPowerState()) return null;
        return getPowerState(currentState + 1);
    }

    @Override
    public void setHigherPowerState() throws NoSuchPowerStateException {
        if(!hasHigherPowerState()) throw new NoSuchPowerStateException();
        currentState++;
    }

    @Override
    public void setLowerPowerState() throws NoSuchPowerStateException {
        if(!hasLowerPowerState()) throw new NoSuchPowerStateException();
        currentState--;
    }

    public boolean hasHigherPowerState() {
        return currentState + 1 < possibleMips.size();
    }

    public boolean hasLowerPowerState() {
        return currentState - 1 >= 0;
    }
}
