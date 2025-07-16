public void goToRestarting(
        ExecutionGraph executionGraph,
        ExecutionGraphHandler executionGraphHandler,
        OperatorCoordinatorHandler operatorCoordinatorHandler,
        Duration backoffTime,
        boolean restartWithParallelism,
        List<ExceptionHistoryEntry> failureCollection) {

    for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
        final int attemptNumber =
                executionVertex.getCurrentExecutionAttempt().getAttemptNumber();

        this.vertexAttemptNumberStore.setAttemptCount(
                executionVertex.getJobvertexId(),
                executionVertex.getParallelSubtaskIndex(),
                attemptNumber + 1);
    }

    transitionToState(
            new Restarting.Factory(
                    this,
                    executionGraph,
                    executionGraphHandler,
                    operatorCoordinatorHandler,
                    LOG,
                    backoffTime,
                    restartWithParallelism,
                    userCodeClassLoader,
                    failureCollection));

    numRestarts++;
    if (failureCollection.isEmpty()) {
        numRescales++;
    }
}
