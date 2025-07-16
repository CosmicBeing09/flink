/********************************
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

package org.apache.flink.configuration;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.InlineElement;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.JobManagerOptions.HybridPartitionDataConsumeConstraint.ALL_PRODUCERS_FINISHED;
import static org.apache.flink.configuration.JobManagerOptions.HybridPartitionDataConsumeConstraint.ONLY_FINISHED_PRODUCERS;
import static org.apache.flink.configuration.JobManagerOptions.HybridPartitionDataConsumeConstraint.UNFINISHED_PRODUCERS;
import static org.apache.flink.configuration.description.LinkElement.link;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/** Configuration options for the JobManager. */
@PublicEvolving
public class JobManagerOptions {

    public static final MemorySize MIN_JVM_HEAP_SIZE = MemorySize.ofMebiBytes(128);

    // ... other code ...

    @Documentation.Section({
            Documentation.Sections.EXPERT_SCHEDULING,
            Documentation.Sections.ALL_JOB_MANAGER
    })
    public static final ConfigOption<Duration> SCHEDULER_SCALING_RESOURCE_STABILIZATION_TIMEOUT =
            key("jobmanager.adaptive-scheduler.executing.resource-stabilization-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Defines the duration the JobManager delays the scaling operation after a resource change if only sufficient resources are available. "
                                                    + "The scaling operation is performed immediately if the resources have changed and the desired resources are available. "
                                                    + "The timeout begins as soon as either the available resources or the job's resource requirements are changed.")
                                    .linebreak()
                                    .text(
                                            "The resource requirements of a running job can be changed using the %s.",
                                            link(
                                                    "{{.Site.BaseURL}}{{.Site.LanguagePrefix}}/docs/ops/rest_api/#jobs-jobid-resource-requirements-1",
                                                    "REST API endpoint"))
                                    .build());

    @Documentation.Section({
            Documentation.Sections.EXPERT_SCHEDULING,
            Documentation.Sections.ALL_JOB_MANAGER
    })
    public static final ConfigOption<Duration> SUBMISSION_RESOURCE_STABILIZATION_TIMEOUT =
            key("jobmanager.adaptive-scheduler.resource-stabilization-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10L))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The resource stabilization timeout defines the time the JobManager will wait if fewer than the desired but sufficient resources are available. "
                                                    + "The timeout starts once sufficient resources for running the job are available. "
                                                    + "Once this timeout has passed, the job will start executing with the available resources.")
                                    .linebreak()
                                    .text(
                                            "If %s is configured to %s, this configuration value will default to 0, so that jobs are starting immediately with the available resources.",
                                            code(SCHEDULER_MODE.key()),
                                            code(SchedulerExecutionMode.REACTIVE.name()))
                                    .build());

    // ---------------------------------------------------------------------------------------------

    private JobManagerOptions() {
        throw new IllegalAccessError();
    }
}
