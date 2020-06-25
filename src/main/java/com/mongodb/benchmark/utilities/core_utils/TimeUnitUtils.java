package com.mongodb.benchmark.utilities.core_utils;

import com.mongodb.benchmark.utilities.math_utils.MathUtilities;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jeffrey Schmidt
 */
public class TimeUnitUtils {
    
    private static final Logger logger = LoggerFactory.getLogger(TimeUnitUtils.class.getName());
    
    public static final BigDecimal MILLISECONDS_PER_SECOND = new BigDecimal(1000);
    public static final BigDecimal MILLISECONDS_PER_MINUTE = new BigDecimal(60000);
    public static final BigDecimal MILLISECONDS_PER_HOUR = new BigDecimal(3600000);
    public static final BigDecimal MILLISECONDS_PER_DAY = new BigDecimal(86400000);

    public static final int TIME_UNIT_SCALE = 7;
    public static final int TIME_UNIT_PRECISION = 31;
    public static final RoundingMode TIME_UNIT_ROUNDING_MODE = RoundingMode.HALF_UP;
    public static final MathContext TIME_UNIT_MATH_CONTEXT = new MathContext(TIME_UNIT_PRECISION, TIME_UNIT_ROUNDING_MODE);
    
    public static BigDecimal getMillisecondValueForTime(String timeUnit, BigDecimal timeValue) {
        
        if ((timeUnit == null) || (timeValue == null)) {
            return null;
        }
        
        if (timeUnit.equalsIgnoreCase("Seconds")) return MathUtilities.smartBigDecimalScaleChange(timeValue.multiply(MILLISECONDS_PER_SECOND), TIME_UNIT_SCALE, TIME_UNIT_ROUNDING_MODE);
        else if (timeUnit.equalsIgnoreCase("Minutes")) return MathUtilities.smartBigDecimalScaleChange(timeValue.multiply(MILLISECONDS_PER_MINUTE), TIME_UNIT_SCALE, TIME_UNIT_ROUNDING_MODE);
        else if (timeUnit.equalsIgnoreCase("Hours")) return MathUtilities.smartBigDecimalScaleChange(timeValue.multiply(MILLISECONDS_PER_HOUR), TIME_UNIT_SCALE, TIME_UNIT_ROUNDING_MODE);
        else if (timeUnit.equalsIgnoreCase("Days")) return MathUtilities.smartBigDecimalScaleChange(timeValue.multiply(MILLISECONDS_PER_DAY), TIME_UNIT_SCALE, TIME_UNIT_ROUNDING_MODE);
        
        return null;
    }
    
    public static BigDecimal getValueForTimeFromMilliseconds(String timeUnit, Long timeInMs) {
        
        if ((timeUnit == null) || (timeInMs == null)) {
            return null;
        }
        
        BigDecimal timeInMs_BigDecimal = new BigDecimal(timeInMs);
        
        if (timeUnit.equalsIgnoreCase("Seconds")) return timeInMs_BigDecimal.divide(MILLISECONDS_PER_SECOND, TIME_UNIT_SCALE, TIME_UNIT_ROUNDING_MODE).stripTrailingZeros();
        else if (timeUnit.equalsIgnoreCase("Minutes")) return timeInMs_BigDecimal.divide(MILLISECONDS_PER_MINUTE, TIME_UNIT_SCALE, TIME_UNIT_ROUNDING_MODE).stripTrailingZeros();
        else if (timeUnit.equalsIgnoreCase("Hours")) return timeInMs_BigDecimal.divide(MILLISECONDS_PER_HOUR, TIME_UNIT_SCALE, TIME_UNIT_ROUNDING_MODE).stripTrailingZeros();
        else if (timeUnit.equalsIgnoreCase("Days")) return timeInMs_BigDecimal.divide(MILLISECONDS_PER_DAY, TIME_UNIT_SCALE, TIME_UNIT_ROUNDING_MODE).stripTrailingZeros();
        
        return null;
    }
    
}
