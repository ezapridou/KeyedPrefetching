package net.michaelkoepf.spegauge.api.common.model.sqbench;

import net.michaelkoepf.spegauge.api.common.ObservableEvent;

public class EntityRecordFull extends EventWithTimestamp implements ObservableEvent {

    public String serializedEntity;

    public enum Type {
        // TODO: you can add further entities here
        A("A"),
        B("B"),
        C("C"),
        JSONA("JSONA"), // data inside JSON attribute
        JSONB("JSONB"),
        JSONC("JSONC");

        public final String value;

        Type(String value) {
            this.value = value;
        }
    }

    public Type type;
    public long uniqueTupleId;
    public long wallClockTimeStampMilliSecondsSinceEpoch;
    public long PK;
    public long FK;
    public long selectivityAttribute1;
    public long selectivityAttribute2;

    public long longAttribute1;
    public long longAttribute2;

    public String plainString;
    public String jsonString;
    public String xmlString;

    public int filterAttribute;

    public EntityRecordFull() {

    }

    // Nexmark JSON A
    public EntityRecordFull(Type type, long eventTimeStampMilliSecondsSinceEpoch, String jsonString, long PK) {
        super(eventTimeStampMilliSecondsSinceEpoch, System.currentTimeMillis());
        this.type = type;
        this.jsonString = jsonString;
        this.PK = PK;

        if (type == Type.JSONA || type == Type.JSONB) {
            this.serializedEntity =  type.value
                    + "\t"
                    + eventTimeStampMilliSecondsSinceEpoch
                    + "\t"
                    + jsonString
                    + "\t"
                    + PK;
        } else {
            throw new IllegalArgumentException("Type must be JSON");
        }
    }

    // Nexmark JSON C
    public EntityRecordFull(Type type, long eventTimeStampMilliSecondsSinceEpoch, long FK, String jsonString) {
        super(eventTimeStampMilliSecondsSinceEpoch, System.currentTimeMillis());
        this.type = type;
        this.jsonString = jsonString;
        this.FK = FK;

        if (type == Type.JSONA || type == Type.JSONB) {
            this.serializedEntity =  type.value
                    + "\t"
                    + eventTimeStampMilliSecondsSinceEpoch
                    + "\t"
                    + jsonString
                    + "\t"
                    + FK;
        } else {
            throw new IllegalArgumentException("Type must be JSON");
        }
    }

    // Nexmark JSON B
    public EntityRecordFull(Type type, long eventTimeStampMilliSecondsSinceEpoch, long PK, long FK, String jsonString) {
        super(eventTimeStampMilliSecondsSinceEpoch, System.currentTimeMillis());
        this.type = type;
        this.jsonString = jsonString;
        this.PK = PK;
        this.FK = FK;

        if (type == Type.JSONA || type == Type.JSONB) {
            this.serializedEntity =  type.value
                    + "\t"
                    + eventTimeStampMilliSecondsSinceEpoch
                    + "\t"
                    + jsonString
                    + "\t"
                    + PK
                    + "\t"
                    + FK;
        } else {
            throw new IllegalArgumentException("Type must be JSON");
        }
    }

    public EntityRecordFull(Type type, long eventTimeStampMilliSecondsSinceEpoch, String jsonString) {
        super(eventTimeStampMilliSecondsSinceEpoch, System.currentTimeMillis());
        this.type = type;
        //this.uniqueTupleId = uniqueTupleId;
        //this.wallClockTimeStampMilliSecondsSinceEpoch = wallClockTimeStampMilliSecondsSinceEpoch;
        this.jsonString = jsonString;


        if (type == Type.JSONA || type == Type.JSONB) {
            this.serializedEntity =  type.value
                    + "\t"
                    + eventTimeStampMilliSecondsSinceEpoch
                    + "\t"
                    + jsonString;
        } else {
            throw new IllegalArgumentException("Type must be JSON");
        }
    }

    public EntityRecordFull(Type type, long uniqueTupleId, long eventTimeStampMilliSecondsSinceEpoch,
                            long wallClockTimeStampMilliSecondsSinceEpoch, long PK, long FK, long selectivityAttribute1,
                            long selectivityAttribute2, long longAttribute1, long longAttribute2, Integer filterAttribute,
                            String plainString, String jsonString, String xmlString) {
        super(eventTimeStampMilliSecondsSinceEpoch, System.currentTimeMillis());
        this.type = type;
        this.uniqueTupleId = uniqueTupleId;
        this.wallClockTimeStampMilliSecondsSinceEpoch = wallClockTimeStampMilliSecondsSinceEpoch;
        this.PK = PK;
        this.FK = FK;
        this.selectivityAttribute1 = selectivityAttribute1;
        this.selectivityAttribute2 = selectivityAttribute2;
        this.longAttribute1 = longAttribute1;
        this.longAttribute2 = longAttribute2;
        this.filterAttribute = filterAttribute;
        this.plainString = plainString;
        this.jsonString = jsonString;
        this.xmlString = xmlString;

        if (type == Type.A || type == Type.B || type == Type.C) {
            this.serializedEntity =  type.value
                    + "\t"
                    + uniqueTupleId
                    + "\t"
                    + eventTimeStampMilliSecondsSinceEpoch
                    + "\t"
                    + wallClockTimeStampMilliSecondsSinceEpoch
                    + "\t"
                    + PK
                    + "\t"
                    + FK
                    + "\t"
                    + selectivityAttribute1
                    + "\t"
                    + selectivityAttribute2
                    + "\t"
                    + longAttribute1
                    + "\t"
                    + longAttribute2
                    + "\t"
                    + (filterAttribute == null ? "" : filterAttribute)
                    + "\t"
                    + (plainString == null ? "" : plainString)
                    + "\t"
                    + (jsonString == null ? "" : jsonString)
                    + "\t"
                    + (xmlString == null ? "" : xmlString);
        } else {
            throw new IllegalArgumentException("Type must be A, B or C");
        }
    }


    public String toStringTSV() {
        return serializedEntity;
    }

    @Override
    public long getEventEmissionTimeStampMilliSeconds() {
        return wallClockTimeStampMilliSecondsSinceEpoch;
    }
}
