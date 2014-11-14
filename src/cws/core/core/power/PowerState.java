package cws.core.core.power;

public class PowerState {
    public PowerState(double power, double mips)
    {
        this.power = power;
        this.mips = mips;
    }

    public double power;
    public double mips;

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;

        if (!(other instanceof PowerState)) return false;
        PowerState otherPowerState = (PowerState) other;

        return (this.power == otherPowerState.power)
            && (this.mips == otherPowerState.mips);
        }
}
