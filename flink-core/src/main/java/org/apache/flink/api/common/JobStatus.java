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

package org.apache.flink.api.common;

import org.apache.flink.annotation.PublicEvolving;

/** Possible states of a job once it has been accepted by the dispatcher. */
@PublicEvolving
public enum JobStatus {
    /**
     * The job has been received by the Dispatcher, and is waiting for the job manager to receive
     * leadership and to be created.
     */
    INITIALIZING(JobTerminalStateKind.NON_TERMINAL_STATE),

    /** Job is newly created, no task has started to run. */
    CREATED(JobTerminalStateKind.NON_TERMINAL_STATE),

    /** Some tasks are scheduled or running, some may be pending, some may be finished. */
    RUNNING(JobTerminalStateKind.NON_TERMINAL_STATE),

    /** The job has failed and is currently waiting for the cleanup to complete. */
    FAILING(JobTerminalStateKind.NON_TERMINAL_STATE),

    /** The job has failed with a non-recoverable task failure. */
    FAILED(JobTerminalStateKind.GLOBALLY),

    /** Job is being cancelled. */
    CANCELLING(JobTerminalStateKind.NON_TERMINAL_STATE),

    /** Job has been cancelled. */
    CANCELED(JobTerminalStateKind.GLOBALLY),

    /** All of the job's tasks have successfully finished. */
    FINISHED(JobTerminalStateKind.GLOBALLY),

    /** The job is currently undergoing a reset and total restart. */
    RESTARTING(JobTerminalStateKind.NON_TERMINAL_STATE),

    /**
     * The job has been suspended which means that it has been stopped but not been removed from a
     * potential HA job store.
     */
    SUSPENDED(JobTerminalStateKind.LOCALLY),

    /** The job is currently reconciling and waits for task execution report to recover state. */
    RECONCILING(JobTerminalStateKind.NON_TERMINAL_STATE);

    // --------------------------------------------------------------------------------------------

    private enum JobTerminalStateKind {
        NON_TERMINAL_STATE,
        LOCALLY,
        GLOBALLY
    }

    private final JobTerminalStateKind terminalState;

    JobStatus(JobTerminalStateKind terminalState) {
        this.terminalState = terminalState;
    }

    /**
     * Checks whether this state is <i>globally terminal</i>. A globally terminal job is complete
     * and cannot fail any more and will not be restarted or recovered by another standby master
     * node.
     *
     * <p>When a globally terminal state has been reached, all recovery data for the job is dropped
     * from the high-availability services.
     *
     * @return True, if this job status is globally terminal, false otherwise.
     */
    public boolean isGloballyTerminalState() {
        return terminalState == JobTerminalStateKind.GLOBALLY;
    }

    /**
     * Checks whether this state is <i>locally terminal</i>. Locally terminal refers to the state of
     * a job's execution graph within an executing JobManager. If the execution graph is locally
     * terminal, the JobManager will not continue executing or recovering the job.
     *
     * <p>The only state that is locally terminal, but not globally terminal is {@link #SUSPENDED},
     * which is typically entered when the executing JobManager loses its leader status.
     *
     * @return True, if this job status is terminal, false otherwise.
     */
    public boolean isTerminalState() {
        return terminalState != JobTerminalStateKind.NON_TERMINAL_STATE;
    }
}
