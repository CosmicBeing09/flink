import org.apache.flink.streaming.runtime.operators.sink.TestSinkV2;/...
InspectableSink sinkWithoutCommitter() {
    TestSinkV2.DefaultSinkWriter<Integer> restoredWriter = new TestSinkV2.DefaultSinkWriter<>();
    return new InspectableSink(TestSinkV2.<Integer>newBuilder().setWriter(restoredWriter).build());
}

InspectableSink sinkWithCommitter() {
    TestSinkV2.DefaultSinkWriter<Integer> restoredWriter =
            new TestSinkV2.DefaultCommittingSinkWriter<>();
    return new InspectableSink(
            TestSinkV2.<Integer>newBuilder()
                    .setWriter(restoredWriter)
                    .setDefaultCommitter()
                    .build());
}

InspectableSink sinkWithTimeBasedWriter() {
    TestSinkV2.DefaultSinkWriter<Integer> restoredWriter = new TimeBasedBufferingSinkWriter();
    return new InspectableSink(
            TestSinkV2.<Integer>newBuilder()
                    .setWriter(restoredWriter)
                    .setDefaultCommitter()
                    .build());
}

InspectableSink sinkWithState(boolean withState, String stateName) {
    TestSinkV2.DefaultSinkWriter<Integer> restoredWriter =
            new TestSinkV2.DefaultStatefulSinkWriter<>();
    TestSinkV2.Builder<Integer> builder =
            TestSinkV2.<Integer>newBuilder()
                    .setDefaultCommitter()
                    .setWithPostCommitTopology(true)
                    .setWriter(restoredWriter);
    if (withState) {
        builder.setWriterState(true);
    }
    if (stateName != null) {
        builder.setCompatibleStateNames(stateName);
    }
    return new InspectableSink(builder.build());
}

    ...
            new OneInputStreamOperatorTestHarness<>(
        new SinkWriterOperatorFactory<>(restoredSink.

getSink()));

        restoredTestHarness.

initializeState(snapshot);
        restoredTestHarness.open();

        // check that the previous state is correctly restored
        assertThat(restoredSink.getRecordCountFromState()).isEqualTo(stateful?2:0);

        restoredTestHarness.close();
        }

        ...
        final OneInputStreamOperatorTestHarness<Integer,CommittableMessage<Integer>>testHarness=
        new OneInputStreamOperatorTestHarness<>(
        new SinkWriterOperatorFactory<>(restoredSink.getSink()));

        testHarness.initializeState(committerState);

        testHarness.open();
        ...
        }
