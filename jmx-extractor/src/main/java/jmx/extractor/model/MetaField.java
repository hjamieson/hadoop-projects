package jmx.extractor.model;

import jmx.extractor.JsonUtils;

public class MetaField {
    public enum TypeIndicator {STRING, INT, LONG, DOUBLE, BOOL}

    private String value;
    private TypeIndicator typeIndicator;

    public MetaField(String value, TypeIndicator ti) {
        this.value = value;
        this.typeIndicator = ti;
    }

    public MetaField(int value) {
        this(Integer.toString(value), TypeIndicator.INT);
    }

    public MetaField(boolean bool) {
        this(Boolean.toString(bool), TypeIndicator.BOOL);
    }

    public MetaField(long value) {
        this(Long.toString(value), TypeIndicator.LONG);
    }

    public MetaField(double value) {
        this(Double.toString(value), TypeIndicator.DOUBLE);
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public TypeIndicator getMeta() {
        return typeIndicator;
    }

    public void setMeta(TypeIndicator meta) {
        this.typeIndicator = meta;
    }

    @Override
    public String toString() {
        return String.format("(%s/%s)", value, typeIndicator);
    }
}
