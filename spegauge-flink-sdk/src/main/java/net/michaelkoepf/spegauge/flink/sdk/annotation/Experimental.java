package net.michaelkoepf.spegauge.flink.sdk.annotation;


import java.lang.annotation.Documented;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.ElementType.TYPE;


/**
 * Indicates that features are experimental and may suffer from performance or other issues.
 */
@Documented
@Target(value={CONSTRUCTOR, FIELD, LOCAL_VARIABLE, METHOD, PACKAGE, MODULE, PARAMETER, TYPE})
public @interface Experimental {
}
