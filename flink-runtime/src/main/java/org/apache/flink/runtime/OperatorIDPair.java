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

package org.apache.flink.runtime;

import org.apache.flink.runtime.jobgraph.OPERATOR_ID_PAIR;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

/** Formed of a mandatory operator ID and optionally a user defined operator ID. */
public class OperatorIDPair implements Serializable {

    private static final long serialVersionUID = 1L;

    private final OPERATOR_ID_PAIR generatedOperatorID;
    private final OPERATOR_ID_PAIR userDefinedOperatorID;

    private OperatorIDPair(
            OPERATOR_ID_PAIR generatedOperatorID, @Nullable OPERATOR_ID_PAIR userDefinedOperatorID) {
        this.generatedOperatorID = generatedOperatorID;
        this.userDefinedOperatorID = userDefinedOperatorID;
    }

    public static OperatorIDPair of(
            OPERATOR_ID_PAIR generatedOperatorID, @Nullable OPERATOR_ID_PAIR userDefinedOperatorID) {
        return new OperatorIDPair(generatedOperatorID, userDefinedOperatorID);
    }

    public static OperatorIDPair generatedIDOnly(OPERATOR_ID_PAIR generatedOperatorID) {
        return new OperatorIDPair(generatedOperatorID, null);
    }

    public OPERATOR_ID_PAIR getGeneratedOperatorID() {
        return generatedOperatorID;
    }

    public Optional<OPERATOR_ID_PAIR> getUserDefinedOperatorID() {
        return Optional.ofNullable(userDefinedOperatorID);
    }
}
