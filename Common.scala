/*
 * ============================================================================
 *  PATCH for com.wells.codi.Common  (Standards/Common.scala)
 * ----------------------------------------------------------------------------
 *  This file is a copy-reference, NOT a stand-alone drop-in. Apply two changes
 *  to your existing Common.scala (same package `com.wells.codi`):
 *
 *    1. ADD the four Neo4J* case classes below (pure additions).
 *    2. REPLACE the existing `TargetTableInfo` case class with the version
 *       below (only change: new optional `neo4j` field at the end, so all
 *       existing positional constructions keep working).
 *
 *  Style follows the existing model: `*Info` suffix, Option for optionals,
 *  defaults on trailing optional fields.
 * ============================================================================
 */
package com.wells.codi

// ----------------------------------------------------------------------------
// (1) Neo4J load data model  -- ADD to Common.scala
// ----------------------------------------------------------------------------

/** One end (source or target) of a relationship: the node it matches/merges. */
case class Neo4JEndpointInfo(labels: String,
                             nodeKeys: String)

/** A node mapping. `properties` (optional) is only used for loadType = "both";
  * for "nodes"-only the whole DataFrame is written. */
case class Neo4JNodeInfo(labels: String,
                         nodeKeys: String,
                         properties: Option[List[String]] = None)

/** A relationship mapping. `properties` (optional) is only used for
  * loadType = "both"; for "relationships"-only the whole DataFrame is written. */
case class Neo4JRelationshipInfo(relationshipType: String,
                                 source: Neo4JEndpointInfo,
                                 target: Neo4JEndpointInfo,
                                 properties: Option[List[String]] = None)

/** Top-level neo4j { } block under tableDetails.
  *   loadType : "nodes" | "relationships" | "both"
  *   saveMode : optional; precedence over TargetTableInfo.saveMode; drives both
  *              nodes and relationships (overwrite/merge | match | append). */
case class Neo4JLoadInfo(loadType: String,
                         saveMode: Option[String] = None,
                         nodes: Option[List[Neo4JNodeInfo]] = None,
                         relationships: Option[List[Neo4JRelationshipInfo]] = None)

// ----------------------------------------------------------------------------
// (2) REPLACE the existing TargetTableInfo with this version
//     (adds the trailing optional `neo4j` field)
// ----------------------------------------------------------------------------
case class TargetTableInfo(targetTableInfo: TableMetaInfo,
                           saveMode: String,
                           customKey: String,
                           customBusinessDate: Option[String] = None,
                           customIngestionTS: Option[String] = None,
                           cdmNext: Option[CdmNextInfo] = None,
                           neo4j: Option[Neo4JLoadInfo] = None)
