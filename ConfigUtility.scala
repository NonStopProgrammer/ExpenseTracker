/*
 * ============================================================================
 *  PATCH for com.wells.codi.Utils.ConfigUtility  (Standards/ConfigUtility.scala)
 * ----------------------------------------------------------------------------
 *  Copy-reference, NOT a stand-alone drop-in. Apply to your existing
 *  ConfigUtility object:
 *
 *    1. REPLACE the existing `setTargetTableInfo` with the version below
 *       (only change: parse the optional `neo4j` block and pass it through).
 *    2. ADD the three new methods: setNeo4JLoadInfo, setNeo4JEndpointInfo,
 *       getNeo4JProperties.
 *
 *  Required imports already present in ConfigUtility.scala:
 *      import com.typesafe.config._            // Config, ConfigValueType
 *      import com.wells.codi._                 // CODIException, Neo4J* models
 *      import scala.collection.JavaConverters._ // asScala
 *  Style mirrors the existing setTargetTableInfo / setExportInfo parsing.
 * ============================================================================
 */
package com.wells.codi.Utils

import com.typesafe.config._
import com.wells.codi._
import scala.collection.JavaConverters._

object ConfigUtilityNeo4JPatch {

  // ----------------------------------------------------------------------------
  // (1) REPLACE existing setTargetTableInfo with this version
  // ----------------------------------------------------------------------------
  def setTargetTableInfo(config: Config): TargetTableInfo = {
    val targetTableInfo = setTableInfo(config)
    val saveMode = config.getString("saveMode")
    val customKey = if (config.hasPath("customKey")) config.getString("customKey") else "NA"
    val customBusinessDate = if (config.hasPath("customBusinessDate")) {
      Converters.APP_BUSINESS_DATE_KEYWORD = config.getString("customBusinessDate")
      Some(Converters.APP_BUSINESS_DATE_KEYWORD)
    } else None
    val customIngestionTS = if (config.hasPath("customIngestionTS")) {
      Converters.APP_INGESTION_TS_KEYWORD = config.getString("customIngestionTS")
      Some(Converters.APP_INGESTION_TS_KEYWORD)
    } else None
    val cdmNextInfo = if (config.hasPath("cdm_process_id")) {
      val cdmProcessId = config.getString("cdm_process_id").trim
      if (cdmProcessId.nonEmpty && cdmProcessId != "NA") {
        val appId = config.getString("app_id").trim
        val drtId = config.getString("drt_id").trim
        val writeDisposition = config.getString("write_disposition").trim
        if (appId.nonEmpty && appId != "NA" &&
            drtId.nonEmpty && drtId != "NA" &&
            writeDisposition.nonEmpty && writeDisposition != "NA") {
          Some(CdmNextInfo(
            cdmProcessId = cdmProcessId,
            appId = appId,
            drtId = drtId,
            writeDisposition = writeDisposition
          ))
        } else {
          throw CODIException("When cdm_process_id is provided, app_id, drt_id, and write_disposition must be valid (not blank or NA)")
        }
      } else {
        None
      }
    } else {
      None
    }
    val neo4jInfo = if (config.hasPath("neo4j")) {
      Some(setNeo4JLoadInfo(config.getConfig("neo4j")))
    } else None
    TargetTableInfo(targetTableInfo, saveMode, customKey, customBusinessDate, customIngestionTS, cdmNextInfo, neo4jInfo)
  }

  // ----------------------------------------------------------------------------
  // (2) ADD these new methods
  // ----------------------------------------------------------------------------
  def setNeo4JLoadInfo(config: Config): Neo4JLoadInfo = {
    val loadType = config.getString("loadType")
    val saveMode = if (config.hasPath("saveMode")) Some(config.getString("saveMode")) else None
    val nodes = if (config.hasPath("nodes")) {
      val nodeList = config.getConfigList("nodes").asScala.map { nodeConfig =>
        Neo4JNodeInfo(
          labels = nodeConfig.getString("labels"),
          nodeKeys = nodeConfig.getString("nodeKeys"),
          properties = getNeo4JProperties(nodeConfig)
        )
      }.toList
      Some(nodeList)
    } else None
    val relationships = if (config.hasPath("relationships")) {
      val relList = config.getConfigList("relationships").asScala.map { relConfig =>
        Neo4JRelationshipInfo(
          relationshipType = relConfig.getString("relationshipType"),
          source = setNeo4JEndpointInfo(relConfig.getConfig("source")),
          target = setNeo4JEndpointInfo(relConfig.getConfig("target")),
          properties = getNeo4JProperties(relConfig)
        )
      }.toList
      Some(relList)
    } else None
    loadType.trim.toLowerCase match {
      case "nodes" =>
        if (nodes.isEmpty) throw CODIException("Neo4J loadType 'nodes' requires a non-empty 'nodes' block")
      case "relationships" =>
        if (relationships.isEmpty) throw CODIException("Neo4J loadType 'relationships' requires a non-empty 'relationships' block")
      case "both" =>
        if (nodes.isEmpty || relationships.isEmpty)
          throw CODIException("Neo4J loadType 'both' requires both 'nodes' and 'relationships' blocks")
      case other =>
        throw CODIException(s"Invalid Neo4J loadType '$other'. Expected nodes, relationships or both")
    }
    Neo4JLoadInfo(loadType, saveMode, nodes, relationships)
  }

  def setNeo4JEndpointInfo(config: Config): Neo4JEndpointInfo = {
    Neo4JEndpointInfo(
      labels = config.getString("labels"),
      nodeKeys = config.getString("nodeKeys")
    )
  }

  /** Reads `properties` as either a comma-string ("a,b,c") or a HOCON list. */
  def getNeo4JProperties(config: Config): Option[List[String]] = {
    if (config.hasPath("properties")) {
      val items = config.getValue("properties").valueType() match {
        case ConfigValueType.LIST => config.getStringList("properties").asScala.toList
        case _ => config.getString("properties").split(",").toList
      }
      val cleaned = items.map(_.trim).filter(_.nonEmpty)
      if (cleaned.isEmpty) None else Some(cleaned)
    } else None
  }

  // setTableInfo(...) already exists in ConfigUtility; referenced above.
  def setTableInfo(config: Config): TableMetaInfo = ???
}
