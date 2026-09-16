package com.surajpanda.dmq.cluster;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class UniqueBrokerIdsValidator
    implements ConstraintValidator<UniqueBrokerIds, List<BrokerAddress>> {

  @Override
  public boolean isValid(List<BrokerAddress> brokers, ConstraintValidatorContext context) {

    if (brokers == null) {
      return true;
    }

    Set<String> seenIds = new HashSet<>();

    for (BrokerAddress broker : brokers) {

      if (broker == null || broker.id() == null) {
        continue;
      }

      if (!seenIds.add(broker.id())) {
        return false;
      }
    }

    return true;
  }
}
