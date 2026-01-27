/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.michaelkoepf.spegauge.api.common.model.sqbench;

import java.io.Serializable;

public class NexmarkEvent extends EventWithTimestamp implements Serializable {
    /** The type of object stored in this event. * */
    public enum Type {
        PERSON,
        AUCTION,
        BID,
        MARKER
    }

    public Person newPerson;
    public Auction newAuction;
    public Bid bid;

    public Type type;

    public NexmarkEvent() {
    }

    public NexmarkEvent(Person newPerson, long eventTimeStampMilliSecondsSinceEpoch, long processingTimeMilliSecondsStart) {
        super(eventTimeStampMilliSecondsSinceEpoch, processingTimeMilliSecondsStart);
        this.newPerson = newPerson;
        newAuction = null;
        bid = null;
        this.type = Type.PERSON;
    }

    public NexmarkEvent(Auction newAuction, long eventTimeStampMilliSecondsSinceEpoch, long processingTimeMilliSecondsStart) {
        super(eventTimeStampMilliSecondsSinceEpoch, processingTimeMilliSecondsStart);
        newPerson = null;
        this.newAuction = newAuction;
        bid = null;
        this.type = Type.AUCTION;
    }

    public NexmarkEvent(Bid bid, long eventTimeStampMilliSecondsSinceEpoch, long processingTimeMilliSecondsStart) {
        super(eventTimeStampMilliSecondsSinceEpoch, processingTimeMilliSecondsStart);
        newPerson = null;
        newAuction = null;
        this.bid = bid;
        this.type = Type.BID;
    }

    // used for markers
    public NexmarkEvent(Type type, long eventTimeStampMilliSecondsSinceEpoch, long processingTimeMilliSecondsStart) {
        super(eventTimeStampMilliSecondsSinceEpoch, processingTimeMilliSecondsStart);
        newPerson = null;
        newAuction = null;
        bid = null;
        this.type = type;
    }
}
