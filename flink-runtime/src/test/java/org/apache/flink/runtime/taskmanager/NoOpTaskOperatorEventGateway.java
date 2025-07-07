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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.runtime.jobgraph.OPERATOR_ID_PAIR;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.jobmaster.JobMasterOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.SerializedValue;

import java.util.concurrent.CompletableFuture;

/** A test / mock implementation of the {@link JobMasterOperatorEventGateway}. */
public class NoOpTaskOperatorEventGateway implements TaskOperatorEventGateway {

    @Override
    public void sendOperatorEventToCoordinator(
            OPERATOR_ID_PAIR operatorID, SerializedValue<OperatorEvent> event) {}

    @Override
    public CompletableFuture<CoordinationResponse> sendRequestToCoordinator(
            OPERATOR_ID_PAIR operator, SerializedValue<CoordinationRequest> request) {
        return CompletableFuture.completedFuture(null);
    }
}
