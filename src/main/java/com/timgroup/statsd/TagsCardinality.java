package com.timgroup.statsd;

public enum TagsCardinality {
    DEFAULT(null),
    NONE("none"),
    LOW("low"),
    ORCHESTRATOR("orchestrator"),
    HIGH("high");

    String value;

    TagsCardinality(String cardinality) {
        value = cardinality;
    }

    static TagsCardinality fromString(String str) {
        if (str != null) {
            for (TagsCardinality v : TagsCardinality.class.getEnumConstants()) {
                if (str.equals(v.value)) {
                    return v;
                }
            }
        }
        return null;
    }
}
