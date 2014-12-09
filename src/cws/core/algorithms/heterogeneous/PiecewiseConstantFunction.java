package cws.core.algorithms.heterogeneous;

import java.util.Map;
import java.util.TreeMap;
import java.util.NavigableMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Collection;
import java.util.NoSuchElementException;
import static java.util.Collections.unmodifiableCollection;
import static java.lang.Math.pow;
import static java.lang.Math.sqrt;


/**
 * A representation of a piecewise constant function using a map. The first
 * value represents the time of the jump, and the second value of the
 * function.
 *
 * The value at a jump time is defined to always be equal to the value just
 * after the jump time.
 *
 * @author David Shepherd
 */
public class PiecewiseConstantFunction {

    /** Storage for the map from jump times to values */
    private TreeMap<Double, Double> jumpTimesAndValues;

    private double initialValue = 0.0;

    public PiecewiseConstantFunction(PiecewiseConstantFunction other) {
        this(other.initialValue, other.jumpTimesAndValues);
    }

    public PiecewiseConstantFunction(double initialValue,
            TreeMap<Double, Double> jumpTimesAndValues) {
        this.initialValue = initialValue;
        this.jumpTimesAndValues = new TreeMap<>(jumpTimesAndValues);
    }

    public PiecewiseConstantFunction(double initialValue) {
        this(initialValue, new TreeMap<Double, Double>());
    }

    /** Get the value of the function at t=-inf. */
    public double getInitialValue() {
        return initialValue;
    }

    /** Evaluate the function */
    public double getValue(double time) {
        Map.Entry<Double, Double> entry = jumpTimesAndValues.floorEntry(time);
        if (entry == null) {
            return this.initialValue;
        } else {
            return entry.getValue();
        }
    }

    /** Set up the function */
    public void addJump(double time, double value) {
        jumpTimesAndValues.put(time, value);
    }

    /** Iterate over the jump times and values */
    public Collection<Map.Entry<Double, Double>> jumps() {
        return unmodifiableCollection(jumpTimesAndValues.entrySet());
    }

    /** Iterate over the jump times */
    public Collection<Double> jumpTimes() {
        return unmodifiableCollection(jumpTimesAndValues.keySet());
    }

    /** Iterate over the jump values */
    public Collection<Double> jumpValues() {
        return unmodifiableCollection(jumpTimesAndValues.values());
    }
    
    @Override
    public String toString() {
        return "initial=" + initialValue
                + " " + jumpTimesAndValues.toString();
    }

    /** Helper function. Replace by
     * com.google.common.base.Objects.firstNonNull or
     * com.google.common.base.Optional when possible.
     */
    private<T> T firstNonNull(T a, T b) {
        if (a != null)return a;
        else if (b != null) return b;
        else throw new NullPointerException();
    }

    /** Integral of the function between a and b */
    public double integral(double a, double b) {
        // Start with the integral between a and the first key greater than a
        double total = this.getValue(a) * (jumpTimesAndValues.ceilingKey(a) - a);

        // Get the map containing only the jumps for times greater than (or
        // equal) a and less than (or equal) b.
        NavigableMap<Double, Double> subMap = jumpTimesAndValues
                .tailMap(a, true)
                .headMap(b, true);

        // Add the integral over each of the intervals following these
        // jumps.
        for (Map.Entry<Double, Double> e : subMap.entrySet()) {
            final double intervalStart = e.getKey();
            final Double intervalEnd = firstNonNull(subMap.higherKey(e.getKey()), b);
            total += e.getValue() * (intervalEnd - intervalStart);
        }

        return total;
    }

    /** Return a new function which is the square of this one */
    public PiecewiseConstantFunction square() {
        // Urgh... it should be possible to write a much more general
        // function that takes another function and applies it to each
        // value. But we don't have function types in Java7 (afaik) so we
        // can't....

        final double initial = pow(this.getInitialValue(), 2);

        PiecewiseConstantFunction newF = new PiecewiseConstantFunction(initial);

        for (final Map.Entry<Double, Double> e : this.jumps()) {
            newF.addJump(e.getKey(), pow(e.getValue(), 2));
        }

        return newF;
    }


    /** Return a new function which is h(t) = this(t) - other(t) */
    public PiecewiseConstantFunction minus(PiecewiseConstantFunction other) {
        final double initial = this.getInitialValue() - other.getInitialValue();
        PiecewiseConstantFunction newF = new PiecewiseConstantFunction(initial);

        Set<Double> allJumps = new HashSet<>(this.jumpTimes());
        allJumps.addAll(other.jumpTimes());

        for (final double jumpTime : allJumps) {
            final double jumpValue = this.getValue(jumpTime) - other.getValue(jumpTime);
            newF.addJump(jumpTime, jumpValue);
        }

        return newF;
    }

    /** Get the 2-norm of this function (square root of the integral of the
     *  square of the function).
     */
    public double twoNorm(double a, double b) {
        return sqrt(this.square().integral(a, b));
    }
}
