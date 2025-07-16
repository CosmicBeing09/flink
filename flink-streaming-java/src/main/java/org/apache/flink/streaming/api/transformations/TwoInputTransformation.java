@Override
public List<Transformation<?>> getTransitivePredecessors() {
    List<Transformation<?>> predecessors = Lists.newArrayList();
    predecessors.add(this);
    predecessors.addAll(input1.getTransitivePredecessors());
    predecessors.addAll(input2.getTransitivePredecessors());
    return predecessors;
}