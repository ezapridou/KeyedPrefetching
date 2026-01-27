package net.michaelkoepf.spegauge.api.sut;

import java.io.Serializable;

public class DataRange implements Serializable {
    public long start;
    public long end;

    public DataRange(long start, long end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof DataRange)) {
            return false;
        }
        DataRange other = (DataRange) obj;
        return this.start == other.start && this.end == other.end;
    }

    @Override
    public int hashCode() {
        int result = 31;
        result = 31 * result + (int) (start ^ (start >>> 32));
        result = 31 * result + (int) (end ^ (end >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "DataRange[" +
                "start=" + start +
                ", end=" + end +
                ']';
    }
}
