package org.corfudb.runtime.view;

import java.util.Map;

import lombok.Data;
import lombok.Getter;

/**
 * Status report of the connectivity of the client to the cluster and the health of the cluster
 * based on the views of the management agents on the Corfu nodes.
 *
 * <p>Created by zlokhandwala on 5/7/18.
 */
@Data
public class ClusterStatusReport {

    /**
     * Connectivity to the node.
     */
    public enum NodeStatus {

        /**
         * Node is reachable.
         */
        UP,

        /**
         * Node is reachable but does not have complete data redundancy.
         */
        DB_SYNCING,

        /**
         * Node is not reachable.
         */
        DOWN
    }

    /**
     * Health of the cluster.
     */
    public enum ClusterStatus {

        /**
         * The cluster is stable and all nodes are operational.
         */
        STABLE(0),

        /**
         * The cluster is operational but one or several nodes are syncing in the background.
         * i.e., (replica catch up)
         */
        DB_SYNCING(1),

        /**
         * The cluster is operational but working with reduced redundancy.
         */
        DEGRADED(2),

        /**
         * The cluster is not operational.
         */
        UNAVAILABLE(3);

        @Getter
        final int healthValue;

        ClusterStatus(int healthValue) {
            this.healthValue = healthValue;
        }
    }

    /**
     * Represents the connectivity status of this runtime to a node.
     */
    public enum ConnectivityStatus {
        /**
         * The node responds to pings from client.
         */
        RESPONSIVE(true),

        /**
         * The node does not respond to pings from client.
         */
        UNRESPONSIVE(false);

        @Getter
        final boolean ping;

        ConnectivityStatus(boolean ping) {
            this.ping = ping;
        }
    }

    /**
     * Represents the sources for cluster status computation.
     * This aims to provide an indicative of status reliability.
     */
    public enum ClusterStatusReliability {
        /**
         * Cluster Status reported based on quorum.
         */
        STRONG_QUORUM,

        /**
         * Cluster Status reported based on highest layout in the system.
         */
        WEAK_SINGLE_NODE,

        /**
         * Unavailable
         */
        UNAVAILABLE
    }

    /**
     * Layout at which the report was generated.
     */
    private final Layout layout;

    /**
     * Cluster health.
     */
    private final ClusterStatus clusterStatus;

    /**
     * Cluster Status Reliability (source of information)
     */
    private final ClusterStatusReliability clusterStatusReliability;

    /**
     * Individual Node Status (within cluster view).
     */
    private final Map<String, NodeStatus> clusterNodeStatusMap;

    /**
     * Map of connectivity of the report generator client to the cluster nodes.
     */
    private final Map<String, ConnectivityStatus> clientServerConnectivityStatusMap;
}
