/...
final Duration executingCooldownTimeout =
        configuration.get(JobManagerOptions.SCHEDULER_SCALING_INTERVAL_MIN);
            ...
executingCooldownTimeout,
        ...

public Duration getScalingIntervalMin() {
    return executingCooldownTimeout;
}
        ...
                return new

Settings(
                    ...
                    executingCooldownTimeout,
                    ...
);
        ...

private StateTransitionManager createExecutingStateTransitionManager(
        StateTransitionManager.Context ctx) {
    return stateTransitionManagerFactory.create(
            ctx,
            clock,
            executingCooldownTimeout,
            settings.getScalingResourceStabilizationTimeout(),
            settings.getMaximumDelayForTriggeringRescale());
}
    ...
