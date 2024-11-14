package org.apache.flink.table.runtime.strategy;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.scheduler.adaptivebatch.BlockingResultInfo;
import org.apache.flink.runtime.scheduler.adaptivebatch.OperatorsFinished;
import org.apache.flink.runtime.scheduler.adaptivebatch.StreamGraphOptimizationStrategy;
import org.apache.flink.streaming.api.graph.StreamGraphContext;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamEdge;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamGraph;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamNode;
import org.apache.flink.streaming.api.graph.util.StreamEdgeUpdateRequestInfo;
import org.apache.flink.streaming.api.graph.util.StreamNodeUpdateRequestInfo;
import org.apache.flink.streaming.api.operators.AdaptiveJoin;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AdaptiveBroadcastJoinOptimizationStrategy implements StreamGraphOptimizationStrategy {
    private static final Logger LOG =
            LoggerFactory.getLogger(AdaptiveBroadcastJoinOptimizationStrategy.class);

    private boolean initialized;

    private Long broadcastThreshold;

    private Map<Integer, Map<Integer, Long>> aggregatedProducedBytesByTypeNumberAndNodeId;

    private Map<Integer, Set<String>> unFinishedUpstreamEdgesByNodeId;

    @Override
    public boolean maybeOptimizeStreamGraph(
            OperatorsFinished operatorsFinished, StreamGraphContext context) {
        ImmutableStreamGraph streamGraph = context.getStreamGraph();
        initialize(streamGraph.getConfiguration());

        List<Integer> finishedStreamNodeIds = operatorsFinished.getFinishedStreamNodeIds();
        for (Integer finishedStreamNodeId : finishedStreamNodeIds) {
            for (ImmutableStreamEdge streamEdge :
                    streamGraph.getStreamNode(finishedStreamNodeId).getOutEdges()) {
                ImmutableStreamNode downstreamNode =
                        streamGraph.getStreamNode(streamEdge.getTargetId());
                if (downstreamNode.getOperatorFactory() instanceof AdaptiveJoin) {
                    long producedBytes =
                            operatorsFinished.getResultInfoMap().get(finishedStreamNodeId).stream()
                                    .mapToLong(BlockingResultInfo::getNumBytesProduced)
                                    .sum();
                    updateStreamNodeStatistic(
                            downstreamNode, streamEdge.getTypeNumber(), producedBytes);
                    AdaptiveJoin adaptiveBroadcastJoin =
                            (AdaptiveJoin) downstreamNode.getOperatorFactory();

                    optimizeJoinNode(downstreamNode, adaptiveBroadcastJoin, streamEdge, context);
                }
            }
        }

        return true;
    }

    private void updateStreamNodeStatistic(
            ImmutableStreamNode streamNode, int typeNumber, long producedBytes) {
        Integer streamNodeId = streamNode.getId();
        if (!unFinishedUpstreamEdgesByNodeId.containsKey(streamNodeId)) {
            Set<String> unFinishedUpstreamEdge = new HashSet<>();
            for (ImmutableStreamEdge edge : streamNode.getInEdges()) {
                unFinishedUpstreamEdge.add(edge.getEdgeId());
            }
            unFinishedUpstreamEdgesByNodeId.put(streamNodeId, unFinishedUpstreamEdge);
        }

        if (!aggregatedProducedBytesByTypeNumberAndNodeId.containsKey(streamNodeId)) {
            aggregatedProducedBytesByTypeNumberAndNodeId.put(streamNodeId, new HashMap<>());
        }
        Map<Integer, Long> aggregatedProducedBytesByTypeNumber =
                aggregatedProducedBytesByTypeNumberAndNodeId.get(streamNodeId);
        aggregatedProducedBytesByTypeNumber.compute(
                typeNumber,
                (key, value) -> {
                    if (value == null) {
                        return producedBytes;
                    } else {
                        return value + producedBytes;
                    }
                });
    }

    private boolean optimizeJoinNode(
            ImmutableStreamNode joinNode,
            AdaptiveJoin adaptiveJoin,
            ImmutableStreamEdge currentEdge,
            StreamGraphContext context) {
        ReadableConfig config = context.getStreamGraph().getConfiguration();
        ClassLoader userClassLoader = context.getStreamGraph().getUserClassLoader();
        if (removeAndCheckAllInputEdgesFinished(joinNode.getId(), currentEdge.getEdgeId())) {
            long leftInputSize =
                    aggregatedProducedBytesByTypeNumberAndNodeId.get(joinNode.getId()).get(1);
            long rightInputSize =
                    aggregatedProducedBytesByTypeNumberAndNodeId.get(joinNode.getId()).get(2);

            Tuple2<Boolean, Boolean> isBroadcastAndLeftBuild =
                    adaptiveJoin.enrichAndCheckBroadcast(
                            leftInputSize, rightInputSize, broadcastThreshold);
            boolean isBroadcast = isBroadcastAndLeftBuild.f0;
            boolean leftIsBuild = isBroadcastAndLeftBuild.f1;

            context.modifyStreamEdge(
                    generateStreamEdgeUpdateRequestInfos(
                            joinNode.getInEdges(), leftIsBuild, isBroadcast));
            context.modifyStreamNode(generateStreamNodeUpdateRequestInfos(joinNode, !leftIsBuild));
            adaptiveJoin.genOperatorFactory(userClassLoader, config);
            freeNodeStatistic(joinNode.getId());
            LOG.info(
                    "[POC] generate adaptive join operator success, StreamNode id {} isBroadcast {} leftIsBuild {}.",
                    joinNode.getId(),
                    isBroadcast,
                    leftIsBuild);
            return true;
        }

        return false;
    }

    private List<ImmutableStreamEdge> filterEdges(
            List<ImmutableStreamEdge> inEdges, int typeNumber) {
        return inEdges.stream()
                .filter(e -> e.getTypeNumber() == typeNumber)
                .collect(Collectors.toList());
    }

    private List<StreamEdgeUpdateRequestInfo> generateStreamEdgeUpdateRequestInfos(
            List<ImmutableStreamEdge> modifiedEdges,
            int modifiedTypeNumber,
            StreamPartitioner<?> outputPartitioner) {
        List<StreamEdgeUpdateRequestInfo> streamEdgeUpdateRequestInfos = new ArrayList<>();
        for (ImmutableStreamEdge streamEdge : modifiedEdges) {
            StreamEdgeUpdateRequestInfo streamEdgeUpdateRequestInfo =
                    new StreamEdgeUpdateRequestInfo(
                                    streamEdge.getEdgeId(),
                                    streamEdge.getSourceId(),
                                    streamEdge.getTargetId())
                            .typeNumber(modifiedTypeNumber);
            if (outputPartitioner != null) {
                streamEdgeUpdateRequestInfo.outputPartitioner(outputPartitioner);
            }
            streamEdgeUpdateRequestInfos.add(streamEdgeUpdateRequestInfo);
        }

        return streamEdgeUpdateRequestInfos;
    }

    private List<StreamEdgeUpdateRequestInfo> generateStreamEdgeUpdateRequestInfos(
            List<ImmutableStreamEdge> inEdges, boolean leftIsBuild, boolean isBroadcast) {
        List<StreamEdgeUpdateRequestInfo> modifiedBuildSideEdges =
                generateStreamEdgeUpdateRequestInfos(
                        filterEdges(inEdges, leftIsBuild ? 1 : 2),
                        1,
                        isBroadcast ? new BroadcastPartitioner<>() : null);
        List<StreamEdgeUpdateRequestInfo> modifiedProbeSideEdges =
                generateStreamEdgeUpdateRequestInfos(
                        filterEdges(inEdges, leftIsBuild ? 2 : 1),
                        2,
                        isBroadcast ? new ForwardPartitioner<>() : null);
        modifiedBuildSideEdges.addAll(modifiedProbeSideEdges);

        return modifiedBuildSideEdges;
    }

    private List<StreamNodeUpdateRequestInfo> generateStreamNodeUpdateRequestInfos(
            ImmutableStreamNode modifiedNode, boolean needSwapInputSide) {
        List<StreamNodeUpdateRequestInfo> streamEdgeUpdateRequestInfos = new ArrayList<>();

        if (needSwapInputSide) {
            TypeSerializer<?>[] typeSerializers = modifiedNode.getTypeSerializersIn();
            Preconditions.checkState(
                    typeSerializers.length == 2,
                    String.format(
                            "Adaptive broadcast join node currently only supports two "
                                    + "inputs, but the join node [%s] has received %s inputs.",
                            modifiedNode.getId(), typeSerializers.length));
            TypeSerializer<?>[] swappedTypeSerializers = new TypeSerializer<?>[2];
            swappedTypeSerializers[0] = typeSerializers[1];
            swappedTypeSerializers[1] = typeSerializers[0];
            StreamNodeUpdateRequestInfo requestInfo =
                    new StreamNodeUpdateRequestInfo(modifiedNode.getId())
                            .typeSerializersIn(swappedTypeSerializers);
            streamEdgeUpdateRequestInfos.add(requestInfo);
        }

        return streamEdgeUpdateRequestInfos;
    }

    private void initialize(ReadableConfig config) {
        if (!initialized) {
            broadcastThreshold =
                    config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD);
            aggregatedProducedBytesByTypeNumberAndNodeId = new HashMap<>();
            unFinishedUpstreamEdgesByNodeId = new HashMap<>();
            initialized = true;
        }
    }

    private boolean removeAndCheckAllInputEdgesFinished(Integer nodeId, String edgeId) {
        unFinishedUpstreamEdgesByNodeId.get(nodeId).remove(edgeId);
        return unFinishedUpstreamEdgesByNodeId.get(nodeId).isEmpty();
    }

    private void freeNodeStatistic(Integer nodeId) {
        aggregatedProducedBytesByTypeNumberAndNodeId.remove(nodeId);
        unFinishedUpstreamEdgesByNodeId.remove(nodeId);
    }
}
