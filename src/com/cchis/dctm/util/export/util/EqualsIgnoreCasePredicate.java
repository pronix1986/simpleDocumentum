package com.cchis.dctm.util.export.util;

import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.functors.NullPredicate;

import java.io.Serializable;

public class EqualsIgnoreCasePredicate implements Predicate, Serializable {
    static final long serialVersionUID = 1L;
    private final String iValue;

    public static Predicate getInstance(String object) {
        return (object == null ? NullPredicate.INSTANCE : new EqualsIgnoreCasePredicate(object));
    }

    private EqualsIgnoreCasePredicate(String object) {
        this.iValue = object;
    }

    public boolean evaluate(Object object) {
        return this.iValue.equalsIgnoreCase((String) object);
    }
}

