package com.surajpanda.dmq.cluster;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.RECORD_COMPONENT})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = UniqueBrokerIdsValidator.class)
public @interface UniqueBrokerIds {

  String message() default "cluster.brokers must not contain duplicate broker ids";

  Class<?>[] groups() default {};

  Class<? extends Payload>[] payload() default {};
}
