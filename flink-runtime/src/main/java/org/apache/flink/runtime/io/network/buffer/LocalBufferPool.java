import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;/overrides

public int getNumberOfRequiredMemorySegments() {
    return minNumberOfMemorySegments;
}

@Override
public int getMaxNumberOfMemorySegments() {
    return maxNumberOfMemorySegments;
}

/**
 * Estimates the number of requested buffers.
 *
 * @return the same value as {@link #getMaxNumberOfMemorySegments()} for bounded pools. For
 *         unbounded pools it returns an approximation based upon {@link
 *         #getExpectedNumberOfMemorySegments()}
 */
public int getEstimatedNumberOfRequestedMemorySegments() {
    if (maxNumberOfMemorySegments < NetworkBufferPool.UNBOUNDED_POOL_SIZE) {
        return maxNumberOfMemorySegments;
    } else {
        return getExpectedNumberOfMemorySegments();
    }
}

@VisibleForTesting
public int getNumberOfRequestedMemorySegments() {
    synchronized (availableMemorySegments) {
        return numberOfRequestedMemorySegments;
    }
}

@Override
public MemorySegment requestMemorySegment() {
    return requestMemorySegment(UNKNOWN_CHANNEL);
}

@GuardedBy("availableMemorySegments")
private boolean requestMemorySegmentFromGlobal() {
    assert Thread.holdsLock(availableMemorySegments);

    if (isRequestedSizeReached()) {
        return false;
    }

    MemorySegment segment = requestPooledMemorySegment();
    if (segment != null) {
        availableMemorySegments.add(segment);
        return true;
    }
    return false;
}

@GuardedBy("availableMemorySegments")
private MemorySegment requestOverdraftMemorySegmentFromGlobal() {
    assert Thread.holdsLock(availableMemorySegments);

    // if overdraft buffers(i.e. buffers exceeding poolSize) is greater than or equal to
    // maxOverdraftBuffersPerGate, no new buffer can be requested.
    if (numberOfRequestedMemorySegments - currentPoolSize >= maxOverdraftBuffersPerGate) {
        return null;
    }

    return requestPooledMemorySegment();
}

@Nullable
@GuardedBy("availableMemorySegments")
private MemorySegment requestPooledMemorySegment() {
    checkState(
            !isDestroyed,
            "Destroyed buffer pools should never acquire segments - this will lead to buffer leaks.");

    MemorySegment segment = networkBufferPool.requestPooledMemorySegment();
    if (segment != null) {
        numberOfRequestedMemorySegments++;
    }
    return segment;
}

@GuardedBy("availableMemorySegments")
private void returnExcessMemorySegments() {
    assert Thread.holdsLock(availableMemorySegments);

    while (hasExcessBuffers()) {
        MemorySegment segment = availableMemorySegments.poll();
        if (segment == null) {
            return;
        }

        returnMemorySegment(segment);
    }
}

@GuardedBy("availableMemorySegments")
private boolean hasExcessBuffers() {
    return numberOfRequestedMemorySegments > currentPoolSize;
}

@GuardedBy("availableMemorySegments")
private boolean isRequestedSizeReached() {
    return numberOfRequestedMemorySegments >= currentPoolSize;
}
