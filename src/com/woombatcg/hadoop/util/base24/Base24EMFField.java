package com.woombatcg.hadoop.util.base24;

public class Base24EMFField {

    private String identifier;
    private int index;
    private String requirement;

    public Base24EMFField(String identifier, int index, String requirement) {
        this.identifier = identifier;
        this.index = index;
        this.requirement = requirement;
    }

    public String toString() {
        return "{ identifier: " + identifier + "," + " requirement: " + requirement + "," + " index: " + index + " }";
    }
}
