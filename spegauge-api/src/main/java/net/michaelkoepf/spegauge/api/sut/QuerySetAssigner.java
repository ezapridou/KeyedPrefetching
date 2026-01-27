package net.michaelkoepf.spegauge.api.sut;


public interface QuerySetAssigner {
    /**
     * Sets the query with the given id as active.
     * @param queryId
     */
    void setQueryActive(int queryId);

    /**
     * Sets the query with the given id as inactive.
     * @param queryId
     */
    void setQueryInactive(int queryId);

    /**
     * Sets the active queries.
     * @param activeQueries
     */
    void setActiveQueries(boolean[] activeQueries);
}
