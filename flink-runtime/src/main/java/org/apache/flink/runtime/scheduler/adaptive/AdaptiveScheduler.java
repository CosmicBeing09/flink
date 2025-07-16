private final Duration submissionResourceWaitTimeout;
...
        return new

Settings(
        executionMode,
        configuration
                .getOptional(JobManagerOptions.RESOURCE_WAIT_TIMEOUT)
                .

orElse(allocationTimeoutDefault),
        configuration
                .

getOptional(JobManagerOptions.RESOURCE_STABILIZATION_TIMEOUT)
                .

orElse(stabilizationTimeoutDefault),
        configuration.

get(JobManagerOptions.SLOT_IDLE_TIMEOUT),

scalingIntervalMin,
        configuration.

get(
        JobManagerOptions.SCHEDULER_SCALING_RESOURCE_STABILIZATION_TIMEOUT),
        configuration.

get(
        MAXIMUM_DELAY_FOR_SCALE_TRIGGER, maximumDelayForRescaleTriggerDefault),

rescaleOnFailedCheckpointsCount);
        ...
        this.submissionResourceWaitTimeout =initialResourceAllocationTimeout;
...

public Duration getSubmissionResourceWaitTimeout() {
    return submissionResourceWaitTimeout;
}
...
        settings.

getSubmissionResourceWaitTimeout(),
...
        settings.

getScalingIntervalMin(),
...
        settings.

getScalingResourceStabilizationTimeout(),
...
        settings.

getMaximumDelayForTriggeringRescale());
        ...

transitionToState(
        new WaitingForResources.Factory(
        this,
        LOG,
        settings.getSubmissionResourceWaitTimeout(),
                this::createWaitingForResourceStateTransitionManager,
previousExecutionGraph));
