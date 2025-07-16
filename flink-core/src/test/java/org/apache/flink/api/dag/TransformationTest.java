/** A test implementation of {@link Transformation}. */
private static class TestTransformation<T> extends Transformation<T> {

    public TestTransformation(String name, TypeInformation<T> outputType, int parallelism) {
        super(name, outputType, parallelism);
    }

    @Override
    public List<Transformation<?>> getTransitivePredecessorsInternal() {
        return Collections.emptyList();
    }

    @Override
    public List<Transformation<?>> getInputs() {
        return Collections.emptyList();
    }
}
