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

package org.apache.flink.client.deployment;

/** Description of the cluster to start by the {@link ClusterDescriptor}. */
public final class StreamingClusterSpecification {
    private final int masterMemoryMB;
    private final int taskManagerMemoryMB;
    private final int slotsPerTaskManager;

    private StreamingClusterSpecification(
            int masterMemoryMB, int taskManagerMemoryMB, int slotsPerTaskManager) {
        this.masterMemoryMB = masterMemoryMB;
        this.taskManagerMemoryMB = taskManagerMemoryMB;
        this.slotsPerTaskManager = slotsPerTaskManager;
    }

    public int getMasterMemoryMB() {
        return masterMemoryMB;
    }

    public int getTaskManagerMemoryMB() {
        return taskManagerMemoryMB;
    }

    public int getSlotsPerTaskManager() {
        return slotsPerTaskManager;
    }

    @Override
    public String toString() {
        return "ClusterSpecification{"
                + "masterMemoryMB="
                + masterMemoryMB
                + ", taskManagerMemoryMB="
                + taskManagerMemoryMB
                + ", slotsPerTaskManager="
                + slotsPerTaskManager
                + '}';
    }

    /** Builder for the {@link StreamingClusterSpecification} instance. */
    public static class StreamingClusterSpecificationBuilder {
        private int masterMemoryMB = 768;
        private int taskManagerMemoryMB = 1024;
        private int slotsPerTaskManager = 1;

        public StreamingClusterSpecificationBuilder setMasterMemoryMB(int masterMemoryMB) {
            this.masterMemoryMB = masterMemoryMB;
            return this;
        }

        public StreamingClusterSpecificationBuilder setTaskManagerMemoryMB(int taskManagerMemoryMB) {
            this.taskManagerMemoryMB = taskManagerMemoryMB;
            return this;
        }

        public StreamingClusterSpecificationBuilder setSlotsPerTaskManager(int slotsPerTaskManager) {
            this.slotsPerTaskManager = slotsPerTaskManager;
            return this;
        }

        public StreamingClusterSpecification createClusterSpecification() {
            return new StreamingClusterSpecification(
                    masterMemoryMB, taskManagerMemoryMB, slotsPerTaskManager);
        }
    }
}
