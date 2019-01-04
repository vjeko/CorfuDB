package org.corfudb.runtime.view;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.WorkflowException;
import org.corfudb.runtime.exceptions.WorkflowResultUnknownException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import org.corfudb.runtime.view.ClusterStatusReport.ClusterStatusReliability;
import org.corfudb.runtime.view.ClusterStatusReport.NodeStatus;
import org.corfudb.runtime.view.ClusterStatusReport.ConnectivityStatus;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.workflows.AddNode;
import org.corfudb.runtime.view.workflows.ForceRemoveNode;
import org.corfudb.runtime.view.workflows.HealNode;
import org.corfudb.runtime.view.workflows.RemoveNode;
import org.corfudb.util.CFUtils;
import org.corfudb.util.NodeLocator;

/**
 * A view of the Management Service to manage reconfigurations of the Corfu Cluster.
 * <p>
 * <p>Created by zlokhandwala on 11/20/17.</p>
 */
@Slf4j
public class ManagementView extends AbstractView {

    /**
     * Number of attempts to ping a node to query the cluster status.
     */
    private static final int CLUSTER_STATUS_QUERY_ATTEMPTS = 3;

    public ManagementView(@NonNull CorfuRuntime runtime) {
        super(runtime);
    }

    /**
     * Remove a node from the cluster.
     *
     * @param endpointToRemove Endpoint of the node to be removed from the cluster.
     * @param retry            the number of times to retry a workflow if it fails
     * @param timeout          total time to wait before the workflow times out
     * @param pollPeriod       the poll interval to check whether a workflow completed or not
     * @throws WorkflowResultUnknownException when the side affect of the operation
     *                                        can't be determined
     * @throws WorkflowException              when the remove operation fails
     */
    public void removeNode(@Nonnull String endpointToRemove, int retry,
                           @Nonnull Duration timeout, @Nonnull Duration pollPeriod) {

        new RemoveNode(endpointToRemove, runtime, retry, timeout, pollPeriod).invoke();
    }

    /**
     * Force remove a node from the cluster.
     *
     * @param endpointToRemove Endpoint of the node to be removed from the cluster.
     * @param retry            the number of times to retry a workflow if it fails
     * @param timeout          total time to wait before the workflow times out
     * @param pollPeriod       the poll interval to check whether a workflow completed or not
     * @throws WorkflowResultUnknownException when the side affect of the operation
     *                                        can't be determined
     * @throws WorkflowException              when the remove operation fails
     */
    public void forceRemoveNode(@Nonnull String endpointToRemove, int retry,
                                @Nonnull Duration timeout, @Nonnull Duration pollPeriod) {
        new ForceRemoveNode(endpointToRemove, runtime, retry, timeout, pollPeriod).invoke();
    }

    /**
     * Add a new node to the existing cluster.
     *
     * @param endpointToAdd Endpoint of the new node to be added to the cluster.
     * @param retry         the number of times to retry a workflow if it fails
     * @param timeout       total time to wait before the workflow times out
     * @param pollPeriod    the poll interval to check whether a workflow completed or not
     * @throws WorkflowResultUnknownException when the side affect of the operation
     *                                        can't be determined
     */
    public void addNode(@Nonnull String endpointToAdd, int retry,
                        @Nonnull Duration timeout, @Nonnull Duration pollPeriod) {
        new AddNode(endpointToAdd, runtime, retry, timeout, pollPeriod)
                .invoke();
    }

    /**
     * Heal an unresponsive node.
     *
     * @param endpointToHeal Endpoint of the new node to be healed in the cluster.
     * @param retry          the number of times to retry a workflow if it fails
     * @param timeout        total time to wait before the workflow times out
     * @param pollPeriod     the poll interval to check whether a workflow completed or not
     * @throws WorkflowResultUnknownException when the side affect of the operation
     *                                        can't be determined
     */
    public void healNode(@Nonnull String endpointToHeal, int retry, @Nonnull Duration timeout,
                         @Nonnull Duration pollPeriod) {
        new HealNode(endpointToHeal, runtime, retry, timeout, pollPeriod).invoke();
    }

    /**
     * If all the layout servers are responsive the cluster status is STABLE,
     * if a minority of them are unresponsive then the status is DEGRADED,
     * else the cluster is UNAVAILABLE.
     *
     * @param layout              Current layout on which responsiveness was checked.
     * @param peerResponsiveNodes responsive nodes in the current layout.
     * @return ClusterStatus
     */
    private ClusterStatus getLayoutServersClusterHealth(Layout layout,
                                                        Set<String> peerResponsiveNodes) {
        ClusterStatus clusterStatus = ClusterStatus.STABLE;
        // A quorum of layout servers need to be responsive for the cluster to be STABLE.
        List<String> responsiveLayoutServers = new ArrayList<>(layout.getLayoutServers());
        // Retain only the responsive servers.
        responsiveLayoutServers.retainAll(peerResponsiveNodes);
        if (responsiveLayoutServers.size() != layout.getLayoutServers().size()) {
            clusterStatus = ClusterStatus.DEGRADED;
            int quorumSize = (layout.getLayoutServers().size() / 2) + 1;
            if (responsiveLayoutServers.size() < quorumSize) {
                clusterStatus = ClusterStatus.UNAVAILABLE;
            }
        }
        return clusterStatus;
    }

    /**
     * If the primary sequencer is unresponsive then the cluster is UNAVAILABLE.
     *
     * @param layout              Current layout on which responsiveness was checked.
     * @param peerResponsiveNodes responsive nodes in the current layout.
     * @return ClusterStatus
     */
    private ClusterStatus getSequencerServersClusterHealth(Layout layout,
                                                           Set<String> peerResponsiveNodes) {
        // The primary sequencer should be reachable for the cluster to be STABLE.
        return !peerResponsiveNodes.contains(layout.getPrimarySequencer())
                ? ClusterStatus.UNAVAILABLE : ClusterStatus.STABLE;
    }

    /**
     * Gets the log unit cluster status based on the replication protocol.
     *
     * @param layout              Current layout on which responsiveness was checked.
     * @param peerResponsiveNodes responsive nodes in the current layout.
     * @return ClusterStatus
     */
    private ClusterStatus getLogUnitServersClusterHealth(Layout layout,
                                                         Set<String> peerResponsiveNodes) {
        // logUnitRedundancyStatus marks the cluster as degraded if any of the nodes is performing
        // stateTransfer and is in process of achieving full redundancy.
        ClusterStatus logUnitRedundancyStatus = peerResponsiveNodes.stream()
                .anyMatch(s -> getLogUnitNodeStatusInLayout(layout, s) == NodeStatus.DB_SYNCING)
                ? ClusterStatus.DB_SYNCING : ClusterStatus.STABLE;
        // Check the availability of the log servers in all segments as reads to all addresses
        // should be accessible.
        ClusterStatus logunitClusterStatus = layout.getSegments().stream()
                .map(segment -> segment.getReplicationMode()
                        .getClusterHealthForSegment(segment, peerResponsiveNodes))
                .max(Comparator.comparingInt(ClusterStatus::getHealthValue))
                .orElse(ClusterStatus.UNAVAILABLE);
        // Gets max of cluster status and logUnitRedundancyStatus.
        return Stream.of(logunitClusterStatus, logUnitRedundancyStatus)
                .max(Comparator.comparingInt(ClusterStatus::getHealthValue))
                .orElse(ClusterStatus.UNAVAILABLE);
    }

    /**
     * Analyzes the health of the cluster based on the views of the cluster of all the
     * ManagementAgents.
     * STABLE: if all nodes in the layout are responsive.
     * DEGRADED: if a minority of Layout servers
     * or a minority of LogUnit servers - in QUORUM_REPLICATION mode only are unresponsive.
     * UNAVAILABLE: if a majority of Layout servers or the Primary Sequencer
     * or a node in the CHAIN_REPLICATION or a majority of nodes in QUORUM_REPLICATION is
     * unresponsive.
     *
     * @param layout              Layout based on which the health is analyzed.
     * @param peerResponsiveNodes Responsive nodes according to the management services.
     * @return ClusterStatus
     */
    private ClusterStatus getClusterHealth(Layout layout, Set<String> peerResponsiveNodes) {

        return Stream.of(getLayoutServersClusterHealth(layout, peerResponsiveNodes),
                getSequencerServersClusterHealth(layout, peerResponsiveNodes),
                getLogUnitServersClusterHealth(layout, peerResponsiveNodes))
                // Gets cluster status from the layout, sequencer and log unit clusters.
                // The status is then aggregated by the max of the 3 statuses acquired.
                .max(Comparator.comparingInt(ClusterStatus::getHealthValue))
                .orElse(ClusterStatus.UNAVAILABLE);
    }

    /**
     * Prunes out the unresponsive nodes as seen by the Management Agents on the Corfu nodes.
     *
     * @param layout           Layout based on which the health is analyzed.
     * @param clusterStatusMap Map of clusterStatus from the ManagementAgents.
     * @return Set of responsive servers as seen by the Corfu cluster nodes.
     */
    private Set<String> filterResponsiveNodes(Layout layout, Map<String, ClusterState> clusterStatusMap) {

        // Using the peer views from the nodes to determine health of the cluster.
        // First all nodes are added to the set. All are assumed to be responsive.
        Set<String> peerResponsiveNodes = new HashSet<>(layout.getAllServers());
        // Truncates all nodes which did not respond to the Heartbeat request.
        peerResponsiveNodes.retainAll(clusterStatusMap.keySet());
        // Filter out all nodes which are not reachable by the cluster nodes.
        clusterStatusMap.forEach((endpoint, clusterStatus) -> clusterStatus.getNodeStatusMap()
                // For each clusterState we are only interested in the nodeState of the polling endpoint.
                // In node X's cluster state we fetch node X's node state and so on.
                .getOrDefault(endpoint, NodeState.getDefaultNodeState(NodeLocator.parseString(endpoint)))
                .getConnectivityStatus().entrySet().stream()
                // Filter out all unreachable nodes from each node state's connectivity view.
                .filter(entry -> !entry.getValue())
                .map(Entry::getKey)
                // Remove all the unreachable nodes from the set of all nodes.
                .forEach(peerResponsiveNodes::remove));
        // Filter out the unresponsive servers in the layout.
        layout.getUnresponsiveServers().forEach(peerResponsiveNodes::remove);
        return peerResponsiveNodes;
    }

    /**
     * Attempts to fetch the layout with the highest epoch.
     * This does not consume the corfu runtime layout as that can get stuck in an infinite retry
     * loop.
     *
     * @param layoutServers Layout Servers to fetch the layout from.
     * @return Latest layout from the servers or null if none of them responded with a layout.
     */
    private Layout getHighestEpochLayout(List<String> layoutServers) {
        Layout layout = null;
        Map<String, CompletableFuture<Layout>> layoutFuturesMap = getLayoutFutures(layoutServers);

        // Wait on the Completable futures and retain the layout with the highest epoch.
        for (Entry<String, CompletableFuture<Layout>> entry : layoutFuturesMap.entrySet()) {
            try {
                Layout nodeLayout = entry.getValue().get();
                log.debug("Server:{} responded with: {}", entry.getKey(), nodeLayout);

                if (layout == null || nodeLayout.getEpoch() > layout.getEpoch()) {
                    layout = nodeLayout;
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new UnrecoverableCorfuInterruptedError(ie);
            } catch (ExecutionException ee) {
                log.error("getClusterStatus: Error while fetching layout from {}.",
                        entry.getKey(), ee);
            }
        }
        log.info("getHighestEpochLayout: Layout for cluster status query: {}", layout);
        return layout;
    }

    private Layout getHighestEpochLayout(Map<String, Layout> layoutMap) {
        Layout layout  = null;
        for (Entry<String, Layout> entry : layoutMap.entrySet()) {
            Layout nodeLayout = entry.getValue();
            if (layout == null || nodeLayout.getEpoch() > layout.getEpoch()) {
                layout = nodeLayout;
            }
        }

        return layout;
    }



    /**
     * Returns a LogUnit Server's status in the layout. It is marked as:
     * UP if it is present in all segments or none of the segments and not in the unresponsive list,
     * NOTE: A node is UP if its not in any of the segments as it might not be a LogUnit component
     * but has only the Layout or the Sequencer (or both) component(s) active.
     * DB_SYNCING if it is present in some but not all or none of the segments,
     * DOWN if it is present in the unresponsive servers list.
     *
     * @param layout Layout to check.
     * @param server LogUnit Server endpoint.
     * @return NodeState with respect to the layout specified.
     */
    private NodeStatus getLogUnitNodeStatusInLayout(Layout layout, String server) {
        if (layout.getUnresponsiveServers().contains(server)) {
            return NodeStatus.DOWN;
        }
        final int segmentsCount = layout.getSegments().size();
        int nodeInSegments = 0;
        for (LayoutSegment layoutSegment : layout.getSegments()) {
            if (layoutSegment.getAllLogServers().contains(server)) {
                nodeInSegments++;
            }
        }
        return nodeInSegments == segmentsCount || nodeInSegments == 0
                ? NodeStatus.UP : NodeStatus.DB_SYNCING;
    }

    /**
     * Create node status map.
     * Assigns a node status to all the nodes in the layout.
     * UP: If the node is responsive and present in all the segments OR present in none (for cases
     * where a node is a Layout or Sequencer Server only and does not possess a LogUnit component.)
     * DB_SYNCING: If the node is present in a few segments but not all. This node is syncing data.
     * DOWN: If the node is unresponsive.
     *
     * @param layout            Layout used to compute cluster status
     * @param responsiveServers Set of responsive servers excluding unresponsive Servers list.
     * @param allServers        All servers in the layout.
     * @return Map of nodes mapped to their status.
     */
    private Map<String, NodeStatus> createNodeStatusMap(Layout layout,
                                                        Set<String> responsiveServers,
                                                        Set<String> allServers) {
        Map<String, NodeStatus> nodeStatusMap = new HashMap<>();
        for (String server : allServers) {
            nodeStatusMap.put(server, NodeStatus.DOWN);
            if (responsiveServers.contains(server)) {
                nodeStatusMap.put(server, getLogUnitNodeStatusInLayout(layout, server));
            }
        }
        return nodeStatusMap;
    }

    /**
     * Get the Cluster Status.
     *
     * This is reported as follows:
     * - (1) The status of the cluster itself (regardless of clients connectivity) as reflected in the
     *   layout. This information is presented along each node's status (up, down, db_sync).
     *
     *   It is important to note that as the cluster state is obtained from the layout,
     *   when quorum is not available (majority of nodes) there are lower guarantees on the
     *   reliability of this state.
     *   For example, in the absence of quorum the system might be in an unstable state which
     *   cannot converge due to lack of consensus. This is reflected in the
     *   cluster status report as clusterStatusReliability.
     *
     * - (2) The connectivity status of the client to every node in the cluster,
     *   i.e., can the client connect to the cluster. This will be obtained by
     *   ping(ing) every node and show as RESPONSIVE, for successful connections or UNRESPONSIVE for
     *   clients unable to communicate.
     *
     *  In this sense a cluster can be STABLE with all nodes UP, while not being available for a
     *  client, as all connections from the client to the cluster nodes are down, showing in this
     *  case connectivity status to all nodes as UNRESPONSIVE.
     *
     *  The ClusterStatusReport consists of the following:
     *
     *  CLUSTER-SPECIFIC STATUS
     *  - clusterStatus: the cluster status a perceived by the system's layout.
     *  STABLE, DEGRADED, DB_SYNCING or UNAVAILABLE
     *  - nodeStatusMap: each node's status as perceived by the system's layout.
     *  (UP, DOWN or DB_SYNC)
     *  - Cluster Status Reliability: STRONG_QUORUM, WEAK_SINGLE_NODE or UNAVAILABLE
     *
     *  CLIENT-CLUSTER SPECIFIC STATUS:
     *  - clientServerConnectivityStatusMap: the connectivity status of this client to the cluster.
     *    (RESPONSIVE, UNRESPONSIVE).
     *
     * @return ClusterStatusReport
     */
    public ClusterStatusReport getClusterStatus() {
        Layout layout;
        ClusterStatusReliability statusReliability = ClusterStatusReliability.STRONG_QUORUM;
        List<String> layoutServers = new ArrayList<>(runtime.getLayoutServers());

        try {
            // Obtain Layout from layout servers:
            //      - First, attempt to get layout from quorum (majority of nodes),
            //            if we fail to get from quorum
            //      - Retrieve highest epoch layout (note that this option decreases reliability to weak.
            //        as the cluster status is not obtained from a quorum of nodes).
            Map<String, Layout> layoutMap = getLayouts(layoutServers);
            int serversLayoutResponses = layoutMap.size();
            int quorum = runtime.getLayoutView().getQuorumNumber();

            if (serversLayoutResponses == 0) {
                // If no layout server responded with a Layout, report cluster status as UNAVAILABLE.
                log.debug("getClusterStatus: layout servers {} failed to respond with layouts.", layoutServers);
                return getUnavailableClusterStatusReport(layoutServers);
            } else if (serversLayoutResponses < quorum) {
                // If quorum unreachable, get layout with highest epoch
                log.info("getClusterStatus: Quorum unreachable, reachable={}, required={}",
                        serversLayoutResponses, quorum);
                statusReliability = ClusterStatusReliability.WEAK_SINGLE_NODE;
                layout = getHighestEpochLayout(layoutMap);
            } else {
                layout = getLayoutFromQuorum(layoutMap, quorum);

                if (layout == null) {
                    // No quorum nodes with same layout, fetch the layout with highest epoch.
                    statusReliability = ClusterStatusReliability.WEAK_SINGLE_NODE;
                    layout = getHighestEpochLayout(layoutMap);
                }
            }

            // Get Cluster Status from Layout
            ClusterStatus clusterStatus = getClusterHealth(layout, layout.getAllActiveServers());

            // Get Node Status from Layout
            Map<String, NodeStatus> nodeStatusMap = getNodeStatusMap(layout);

            // To complete cluster status with connectivity information of this
            // client to every node in the cluster, ping all servers
            Map <String, ClusterStatusReport.ConnectivityStatus> connectivityStatusMap = getConnectivityStatusMap(layout);

            return new ClusterStatusReport(layout, clusterStatus, statusReliability, nodeStatusMap, connectivityStatusMap);
        } catch (Exception e) {
            log.error("getClusterStatus[{}]: cluster status unavailable. Exception: {}",
                    this.runtime.getParameters().getClientId(), e);
            return getUnavailableClusterStatusReport(layoutServers);
        }
    }

    private ClusterStatusReport getUnavailableClusterStatusReport(List<String> layoutServers) {
        return new ClusterStatusReport(null,
                ClusterStatus.UNAVAILABLE,
                ClusterStatusReliability.UNAVAILABLE,
                layoutServers.stream().collect(Collectors.toMap(
                        endpoint -> endpoint,
                        node -> NodeStatus.DOWN)),
                layoutServers.stream().collect(Collectors.toMap(
                endpoint -> endpoint,
                responsiveness -> ConnectivityStatus.UNRESPONSIVE)));
    }

    private Map<String, Layout> getLayouts(List<String> layoutServers) {
        Map<String, CompletableFuture<Layout>> layoutFuturesMap = getLayoutFutures(layoutServers);
        Map<String, Layout> layoutMap = new HashMap<>();

        // Wait on the Completable futures
        for (Entry<String, CompletableFuture<Layout>> entry : layoutFuturesMap.entrySet()) {
            try {
                Layout nodeLayout = entry.getValue().get();
                log.debug("Server:{} responded with layout: {}", entry.getKey(), nodeLayout);
                layoutMap.put(entry.getKey(), nodeLayout);
            } catch (Exception e) {
                log.error("getClusterStatus: Error while fetching layout from {}. Exception: {}",
                        entry.getKey(), e);
            }
        }

        return layoutMap;
    }

    private Layout getLayoutFromQuorum(Map<String, Layout> layoutMap, int quorum) {
        Layout layout = null;
        Map<Layout, Integer> uniqueLayoutMap = null;

        // Find if layouts are the same in all nodes (at least quorum nodes should agree in the
        // same layout) - count occurrences of each layout in cluster nodes.
        for (Entry<String, Layout> entry : layoutMap.entrySet()) {
            if (uniqueLayoutMap == null) {
                uniqueLayoutMap = new HashMap<Layout, Integer>() {{
                    put(entry.getValue(), 1);
                }};
            } else {
                Boolean sameLayoutFound = false;
                for (Layout uniqueLayout : uniqueLayoutMap.keySet()) {
                    if (entry.getValue().equals(uniqueLayout)) {
                        uniqueLayoutMap.merge(uniqueLayout, 1, Integer::sum);
                        sameLayoutFound = true;
                        break;
                    }
                }
                if (!sameLayoutFound) {
                    uniqueLayoutMap.put(entry.getValue(), 1);
                }
            }
        }

        // Filter layouts present in quorum number of nodes
        Map<Layout, Integer> uniqueLayoutsQuorumMap = uniqueLayoutMap.entrySet().stream()
                .filter(x -> x.getValue() >= quorum)
                .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));

        // Select layout with greater number of occurrences.
        if (uniqueLayoutsQuorumMap.size() > 0) {
            Map.Entry<Layout, Integer> maxEntry = null;
            for (Map.Entry<Layout, Integer> entry : uniqueLayoutsQuorumMap.entrySet()) {
                if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0) {
                    maxEntry = entry;
                }
            }
            layout = maxEntry.getKey();
        }

        return layout;
    }

    private Map<String, CompletableFuture<Layout>> getLayoutFutures(List<String> layoutServers) {

        Map<String, CompletableFuture<Layout>> layoutFuturesMap = new HashMap<>();

        // Get layout futures for layout requests from all layout servers.
        for (String server : layoutServers) {
            try {
                // Router creation can throw a NetworkException.
                IClientRouter router = runtime.getRouter(server);
                layoutFuturesMap.put(server,
                        new LayoutClient(router, Layout.INVALID_EPOCH).getLayout());
            } catch (NetworkException e) {
                log.error("getClusterStatus: Exception encountered connecting to {}. ",
                        server, e);
                CompletableFuture<Layout> cf = new CompletableFuture<>();
                cf.completeExceptionally(e);
                layoutFuturesMap.put(server, cf);
            }
        }

        return layoutFuturesMap;
    }

    private Map<String, ClusterStatusReport.ConnectivityStatus> getConnectivityStatusMap(Layout layout) {
        Map<String, ClusterStatusReport.ConnectivityStatus> connectivityStatusMap = new HashMap<>();

        // Initialize connectivity status map to all servers as unresponsive
        for (String serverEndpoint : layout.getAllServers()) {
            connectivityStatusMap.put(serverEndpoint, ClusterStatusReport.ConnectivityStatus.UNRESPONSIVE);
        }

        RuntimeLayout runtimeLayout = new RuntimeLayout(layout, runtime);
        Map<String, CompletableFuture<Boolean>> pingFutureMap = new HashMap<>();


        for (int i = 0; i < CLUSTER_STATUS_QUERY_ATTEMPTS; i++) {
            // If a server is unresponsive attempt to ping for cluster_status_query_attempts
            if(connectivityStatusMap.containsValue(ConnectivityStatus.UNRESPONSIVE)) {
                // Ping only unresponsive endpoints
                List<String> endpoints = connectivityStatusMap.entrySet()
                        .stream()
                        .filter(entry -> entry.getValue().equals(ClusterStatusReport.ConnectivityStatus.UNRESPONSIVE))
                        .map(Entry::getKey)
                        .collect(Collectors.toList());

                for (String serverEndpoint : endpoints) {
                    // Ping all nodes asynchronously
                    CompletableFuture<Boolean> cf = runtimeLayout.getBaseClient(serverEndpoint).ping();
                    pingFutureMap.put(serverEndpoint, cf);
                }

                // Accumulate all responses.
                pingFutureMap.forEach((endpoint, pingFuture) -> {
                    try {
                        ClusterStatusReport.ConnectivityStatus connectivityStatus = CFUtils.getUninterruptibly(pingFuture,
                                WrongEpochException.class) ? ConnectivityStatus.RESPONSIVE : ConnectivityStatus.UNRESPONSIVE;
                        connectivityStatusMap.put(endpoint, connectivityStatus);
                    } catch (WrongEpochException wee) {
                        connectivityStatusMap.put(endpoint, ConnectivityStatus.RESPONSIVE);
                    } catch (Exception e) {
                        connectivityStatusMap.put(endpoint, ConnectivityStatus.UNRESPONSIVE);
                    }
                });
            }
        }

        return connectivityStatusMap;
    }

    private Map<String, NodeStatus> getNodeStatusMap(Layout layout) {
        Map<String, NodeStatus> nodeStatusMap = new HashMap<>();
        for (String endpoint: layout.getAllServers()) {
            if (layout.getUnresponsiveServers().contains(endpoint)) {
                nodeStatusMap.put(endpoint, NodeStatus.DOWN);
            } else if (layout.getSegments().size() != layout.getSegmentsForEndpoint(endpoint).size()) {
                nodeStatusMap.put(endpoint, NodeStatus.DB_SYNCING);
            } else {
                nodeStatusMap.put(endpoint, NodeStatus.UP);
            }
        }

        return nodeStatusMap;
    }
}
