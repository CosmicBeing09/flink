@Test
public void testExternalizedSwitchRocksDBCheckpointsStandalone() throws Exception {
    final File checkpointDir = temporaryFolder.newFolder();
    StateBackend previousStateBackend = createRocksDBStateBackend(checkpointDir, false);
    StateBackend newStateBackendConfig = createRocksDBStateBackend(checkpointDir, true);
    testExternalizedCheckpoints(
            checkpointDir,
            null,
            previousStateBackend,
            newStateBackendConfig,
            previousStateBackend,
            false,
            recoveryClaimMode);
}

@Test
public void testExternalizedSwitchRocksDBCheckpointsWithLocalRecoveryStandalone()
        throws Exception {
    final File checkpointDir = temporaryFolder.newFolder();
    StateBackend previousStateBackend = createRocksDBStateBackend(checkpointDir, false);
    StateBackend newStateBackendConfig = createRocksDBStateBackend(checkpointDir, true);
    testExternalizedCheckpoints(
            checkpointDir,
            null,
            previousStateBackend,
            newStateBackendConfig,
            previousStateBackend,
            true,
            recoveryClaimMode);
}

@Test
public void testExternalizedSwitchRocksDBCheckpointsZookeeper() throws Exception {
    try (TestingServer zkServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer()) {
        final File checkpointDir = temporaryFolder.newFolder();
        StateBackend previousStateBackend = createRocksDBStateBackend(checkpointDir, false);
        StateBackend newStateBackendConfig = createRocksDBStateBackend(checkpointDir, true);
        testExternalizedCheckpoints(
                checkpointDir,
                zkServer.getConnectString(),
                previousStateBackend,
                newStateBackendConfig,
                previousStateBackend,
                false,
                recoveryClaimMode);
    }
}

@Test
public void testExternalizedSwitchRocksDBCheckpointsWithLocalRecoveryZookeeper()
        throws Exception {
    try (TestingServer zkServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer()) {
        final File checkpointDir = temporaryFolder.newFolder();
        StateBackend previousStateBackend = createRocksDBStateBackend(checkpointDir, false);
        StateBackend newStateBackendConfig = createRocksDBStateBackend(checkpointDir, true);
        testExternalizedCheckpoints(
                checkpointDir,
                zkServer.getConnectString(),
                previousStateBackend,
                newStateBackendConfig,
                previousStateBackend,
                true,
                recoveryClaimMode);
    }
}