package com.alibabacloud.polar_race.engine.common.pc;

public class KvDouble {
    byte[] key;
    byte[] value;

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public KvDouble(byte[] key) {
        this.key = key;
        value = new byte[4096];
    }
}
