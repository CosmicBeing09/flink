import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.configuration.description.Description;

import java.time.Duration;

/***** Configuration options for the JobManager. ***/
@PublicEvolving
public class JobManagerOptions {
    // ... other configurations

    @Documentation.Section({
            Documentation.Sections.EXPERT_SCHEDULING,
            Documentation.Sections.ALL_JOB_MANAGER
    })
    public static final ConfigOption<Duration> SCHEDULER_SCALING_EXECUTING_RESOURCE_STABILIZATION_TIMEOUT =
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

    // ... other configurations

    @Documentation.Section({
            Documentation.Sections.EXPERT_SCHEDULING,
            Documentation.Sections.ALL_JOB_MANAGER
    })
    public static final ConfigOption<Duration> RESOURCE_STABILIZATION_TIMEOUT =
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
    // ... other configurations
}
