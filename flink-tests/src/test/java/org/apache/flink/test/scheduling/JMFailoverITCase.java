private JobGraph prepareEnvAndGetJobGraph(
        Configuration config, boolean operatorCoordinatorsSupportsBatchSnapshot)
        throws Exception {
    flinkCluster =
            TestingMiniCluster.newBuilder(getMiniClusterConfiguration(config))
                    .setHighAvailabilityServicesSupplier(highAvailabilityServicesSupplier)
                    .build();
    flinkCluster.start();

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(-1);
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);

    return operatorCoordinatorsSupportsBatchSnapshot
            ? createJobGraph(env, methodName)
            : createStreamGraphWithUnsupportedBatchSnapshotOperatorCoordinator(env, methodName);
}

private JobGraph createStreamGraphWithUnsupportedBatchSnapshotOperatorCoordinator(
        StreamExecutionEnvironment env, String jobName) throws Exception {

    TupleTypeInfo<Tuple2<Integer, Integer>> typeInfo =
            new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

    File file = new File(temporaryFolder.getParent().toFile(), "data.tmp-" + UUID.randomUUID());
    prepareTestData(file);

    FileSource<String> source =
            FileSource.forRecordStreamFormat(
                            new TextLineInputFormat(), new Path(file.getPath()))
                    .build();

    env.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
            .setParallelism(SOURCE_PARALLELISM)
            .slotSharingGroup("group1")
            .transform("Map", typeInfo, new StubMapFunction2())
            .slotSharingGroup("group2")
            .keyBy(tuple2 -> tuple2.f0)
            .sum(1)
            .slotSharingGroup("group3")
            .transform("Sink", TypeInformation.of(Void.class), new StubRecordSink())
            .slotSharingGroup("group4");

    StreamGraph streamGraph = env.getStreamGraph();
    streamGraph.setGlobalStreamExchangeMode(GlobalStreamExchangeMode.ALL_EDGES_BLOCKING);
    streamGraph.setJobType(JobType.BATCH);
    streamGraph.setJobName(jobName);
    return StreamingJobGraphGenerator.createJobGraph(streamGraph);
}