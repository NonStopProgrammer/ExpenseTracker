package com.wells.codi.Load

import com.wells.codi.CommonFunctions.ThrowableOps
import com.wells.codi.Providers.WithSparkSession
import com.wells.codi.Utils.ETLStatus
import com.wells.codi._
import com.typesafe.config.{Config, ConfigValueType}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.collection.JavaConverters._

/**
 * Neo4J target writer that conforms to the standards target-writer interface
 * (see LoadHelperHadoop.loadParquetTable) while reusing the node / relationship
 * load logic from the working Neo4J product (Neo4JWriter / Neo4JNodeLoader /
 * Neo4JRelationshipLoader).
 *
 * Design (confirmed):
 *   - CONFIG-DRIVEN. The DataFrame carries only the final business columns;
 *     the graph mapping is declared in the `neo4j { }` block of the target.
 *
 *   - saveMode: a single `neo4j.saveMode` variable drives BOTH nodes and
 *     relationships and takes PRECEDENCE over tableDetails.saveMode.
 *       overwrite/merge -> MERGE (keyed upsert / create-if-missing endpoints)
 *       match           -> MATCH existing endpoints (relationships)
 *       append          -> CREATE
 *
 *   - loadType:
 *       "nodes"         -> whole DataFrame (all columns + audit) written per node spec.
 *       "relationships" -> whole DataFrame (all columns + audit) written per rel spec.
 *       "both"          -> single DataFrame split by COLUMN PROJECTION: each spec
 *                          takes its configured `properties` + key columns + audit
 *                          columns (or the whole DataFrame when `properties` absent).
 *
 *   - Audit timestamp handling is identical to the working sample: distinct
 *     values of the audit ts column (customIngestionTS) are normalized into
 *     tokens and used to scope the post-write count read-back.
 *
 *   - NO duplicate elimination, NO distinct on the data. Writes use MERGE/MATCH
 *     per saveMode. Only COUNT VALIDATION is performed (written rows vs Neo4J
 *     read-back). NO rollback: on any failure the job is marked failure and any
 *     partial graph is left in place.
 *
 * Wiring points (marked WIRING POINT) bind this class to your model.
 *
 * Entry point:
 *     val out: TESLA_DF = Neo4JLoadHelper.loadNeo4J(target)(testlaDF)
 */
object Neo4JLoadHelper extends Logging with WithSparkSession {

  // ---------------------------------------------------------------------------
  // Constants
  // ---------------------------------------------------------------------------
  private val TARGET_NODES = "Neo4J:nodes"
  private val TARGET_RELATIONSHIPS = "Neo4J:relationships"
  private val TARGET_BOTH = "Neo4J:nodes+relationships"

  private val LOAD_TYPE_NODES = "nodes"
  private val LOAD_TYPE_RELATIONSHIPS = "relationships"
  private val LOAD_TYPE_BOTH = "both"

  private val DEFAULT_TS_COL = "last_updated_ts"
  private val NEO4J_FORMAT = "org.neo4j.spark.DataSource"

  // ---------------------------------------------------------------------------
  // Config specs (parsed from the neo4j { } block)
  // ---------------------------------------------------------------------------
  private case class EndpointSpec(labels: String, nodeKeys: String)
  private case class NodeSpec(labels: String, nodeKeys: String, properties: Option[List[String]])
  private case class RelationshipSpec(
    relationshipType: String,
    source: EndpointSpec,
    target: EndpointSpec,
    properties: Option[List[String]]
  )

  // ---------------------------------------------------------------------------
  // Wiring points - adjust to your TargetInfo / TableInfo schema
  // ---------------------------------------------------------------------------

  /** WIRING POINT (connection): TableMetaInfo(url,user,enpw,db) resolved from connectionName. */
  private def neo4jInfoFrom(target: TargetInfo): TableMetaInfo =
    target.tableMetaInfo

  /** WIRING POINT (mapping): the parsed `neo4j { }` HOCON block as a typesafe Config. */
  private def neo4jConfigFrom(target: TargetInfo): Config =
    target.tableInfo.get.neo4j

  /** WIRING POINT (audit ts): the timestamp column used to scope count read-back. */
  private def ingestionTsColumn(target: TargetInfo): String =
    Option(target.tableInfo.get.customIngestionTS).map(_.trim).filter(_.nonEmpty).getOrElse(DEFAULT_TS_COL)

  // ---------------------------------------------------------------------------
  // Standards entry point: (target)(testlaDF) => TESLA_DF
  // ---------------------------------------------------------------------------
  def loadNeo4J(target: TargetInfo)(testlaDF: TESLA_DF): TESLA_DF = {
    val tableInfo = target.tableInfo.get
    val defaultSaveMode = tableInfo.saveMode.toLowerCase
    val targetTable = tableInfo.targetTableInfo.db + "." + tableInfo.targetTableInfo.table
    val successOnEmptyExtract = target.successOnEmptyExtract

    var loadTestlaDF = testlaDF
    val df = testlaDF.df

    val keywordMap = loadTestlaDF.SEE_RESERVED_KEYWORDS
    val recordCount = testlaDF.transformedRecordCount
    keywordMap += (Converters.RECORD_COUNT_KEYWORD -> recordCount.toString)
    loadTestlaDF = loadTestlaDF.copy(SEE_RESERVED_KEYWORDS = keywordMap)

    var insertedRecordCount = 0L

    if (recordCount > 0) {
      try {
        val neo4jInfo = neo4jInfoFrom(target)
        val cfg = neo4jConfigFrom(target)
        val tsCol = ingestionTsColumn(target)

        // neo4j.saveMode takes precedence over tableDetails.saveMode; applies to nodes + relationships.
        val effectiveMode = optString(cfg, "saveMode").getOrElse(defaultSaveMode)
        val loadType = cfg.getString("loadType").trim.toLowerCase
        logger.info(s"Neo4JLoadHelper.loadNeo4J - loadType=$loadType effectiveSaveMode=$effectiveMode tsCol=$tsCol")

        val (resolvedTarget, inserted) = loadType match {
          case LOAD_TYPE_NODES =>
            val nodeSpecs = parseNodeSpecs(cfg)
            (TARGET_NODES,
              loadNodesInternal(spark, neo4jInfo, tsCol, effectiveMode, nodeSpecs, df, projectColumns = false))

          case LOAD_TYPE_RELATIONSHIPS =>
            val relSpecs = parseRelationshipSpecs(cfg)
            (TARGET_RELATIONSHIPS,
              loadRelationshipsInternal(spark, neo4jInfo, tsCol, effectiveMode, relSpecs, df, projectColumns = false))

          case LOAD_TYPE_BOTH =>
            // Single DataFrame -> nodes first (so endpoints exist), then relationships.
            val nodeSpecs = parseNodeSpecs(cfg)
            val relSpecs = parseRelationshipSpecs(cfg)
            val nodesInserted =
              loadNodesInternal(spark, neo4jInfo, tsCol, effectiveMode, nodeSpecs, df, projectColumns = true)
            val relsInserted =
              loadRelationshipsInternal(spark, neo4jInfo, tsCol, effectiveMode, relSpecs, df, projectColumns = true)
            (TARGET_BOTH, nodesInserted + relsInserted)

          case other =>
            throw CODIException(
              s"Unsupported Neo4J loadType '$other'. Expected '$LOAD_TYPE_NODES', '$LOAD_TYPE_RELATIONSHIPS' or '$LOAD_TYPE_BOTH'."
            )
        }
        insertedRecordCount = inserted

        // Counts validated inside the loaders; reaching here means success.
        loadTestlaDF = loadTestlaDF.copy(
          extractStatus = true,
          tableName = resolvedTarget,
          finalRecordCount = insertedRecordCount,
          comments = ETLStatus.EXTRACT_SUCCESS.label
        )
      } catch {
        case e: Exception =>
          // No rollback: any partial write is left in place; mark as failure.
          logger.error(s"Error during Neo4J Write - ${e.getFullStackTraceAsString()}")
          loadTestlaDF = loadTestlaDF.copy(
            extractStatus = false,
            tableName = targetTable,
            finalRecordCount = insertedRecordCount,
            comments = ETLStatus.EXTRACT_FAILURE.label + " - " + s"${e.getMessage}"
          )
      }
    } else {
      logger.warn("Zero Record Ingestion.")
      loadTestlaDF = loadTestlaDF.copy(
        extractStatus = successOnEmptyExtract,
        tableName = targetTable,
        finalRecordCount = recordCount,
        comments = ETLStatus.EXTRACT_SUCCESS.label + " - " + s"Zero Record Ingestion"
      )
    }

    logger.info(
      s"Neo4J Loader --> ${targetTable}, " +
        s"recordsToInsert --> ${recordCount}, " +
        s"insertedRecordCount --> ${insertedRecordCount}, " +
        s"${loadTestlaDF.extractStatus}"
    )
    loadTestlaDF
  }

  // ---------------------------------------------------------------------------
  // Config parsing
  // ---------------------------------------------------------------------------
  private def parseNodeSpecs(cfg: Config): List[NodeSpec] = {
    if (!cfg.hasPath("nodes")) {
      throw CODIException("Neo4J node load requested but no 'nodes = [ ... ]' block found in config.")
    }
    val specs = cfg.getConfigList("nodes").asScala.toList.map { c =>
      NodeSpec(
        labels = c.getString("labels"),
        nodeKeys = c.getString("nodeKeys"),
        properties = optCsvList(c, "properties")
      )
    }
    if (specs.isEmpty) {
      throw CODIException("Neo4J node load requested but 'nodes' block is empty.")
    }
    specs
  }

  private def parseRelationshipSpecs(cfg: Config): List[RelationshipSpec] = {
    if (!cfg.hasPath("relationships")) {
      throw CODIException("Neo4J relationship load requested but no 'relationships = [ ... ]' block found in config.")
    }
    val specs = cfg.getConfigList("relationships").asScala.toList.map { c =>
      RelationshipSpec(
        relationshipType = c.getString("relationshipType"),
        source = parseEndpoint(c.getConfig("source")),
        target = parseEndpoint(c.getConfig("target")),
        properties = optCsvList(c, "properties")
      )
    }
    if (specs.isEmpty) {
      throw CODIException("Neo4J relationship load requested but 'relationships' block is empty.")
    }
    specs
  }

  private def parseEndpoint(c: Config): EndpointSpec =
    EndpointSpec(labels = c.getString("labels"), nodeKeys = c.getString("nodeKeys"))

  private def optString(c: Config, path: String): Option[String] =
    if (c.hasPath(path)) Option(c.getString(path)).map(_.trim).filter(_.nonEmpty) else None

  /** Parses a property list given either as a comma-string ("a,b,c") or a HOCON list. */
  private def optCsvList(c: Config, path: String): Option[List[String]] = {
    if (!c.hasPath(path)) None
    else {
      val items =
        if (c.getValue(path).valueType == ConfigValueType.LIST) c.getStringList(path).asScala.toList
        else c.getString(path).split(",").toList
      val cleaned = items.map(_.trim).filter(_.nonEmpty)
      if (cleaned.isEmpty) None else Some(cleaned)
    }
  }

  // ---------------------------------------------------------------------------
  // Node load (count-validation only; no dedup; no rollback)
  // ---------------------------------------------------------------------------
  private def loadNodesInternal(
    spark: SparkSession,
    neo4jInfo: TableMetaInfo,
    tsCol: String,
    effectiveMode: String,
    specs: List[NodeSpec],
    df: DataFrame,
    projectColumns: Boolean
  ): Long = {
    logger.info(s"Neo4JLoadHelper.loadNodesInternal - started, specs=${specs.length}, projectColumns=$projectColumns")
    requireColumn(df, tsCol, "node")

    var insertedRecordCount = 0L
    specs.foreach { spec =>
      val neo4jLabels = formatLabels(spec.labels)
      val keyColumns = dfKeyColumns(spec.nodeKeys)
      requireColumns(df, keyColumns, s"node labels=$neo4jLabels")

      // Whole DF for node-only; projected (properties + keys + audit) for "both".
      val nodeDf = projectFor(df, projectColumns, spec.properties, keyColumns, tsCol, s"node labels=$neo4jLabels")

      val expectedCount = nodeDf.count()
      logger.info(s"Neo4JLoadHelper.loadNodesInternal - writing $expectedCount nodes labels=$neo4jLabels keys=${spec.nodeKeys} saveMode=$effectiveMode")
      writeNodesToNeo4J(
        df = nodeDf,
        labels = neo4jLabels,
        nodeKeys = spec.nodeKeys,
        saveMode = effectiveMode,
        neo4jInfo = neo4jInfo
      )

      val tsFilter = buildTsFilter("n", tsCol, resolveTsTokens(nodeDf, tsCol, "node"))
      val countQuery = s"MATCH (n$neo4jLabels) WHERE $tsFilter RETURN count(n) AS count"
      val insertedForGroup = readCountFromNeo4J(spark, neo4jInfo, countQuery)
      insertedRecordCount += insertedForGroup
      logger.info(s"Neo4JLoadHelper.loadNodesInternal - validation labels=$neo4jLabels inserted=$insertedForGroup expected=$expectedCount")
      if (insertedForGroup != expectedCount) {
        throw CODIException(
          s"Neo4J node count mismatch for labels=$neo4jLabels; inserted=$insertedForGroup expected=$expectedCount"
        )
      }
      logger.info(s"Neo4JLoadHelper.loadNodesInternal - completed labels=$neo4jLabels ($expectedCount nodes)")
    }

    logger.info(s"Neo4JLoadHelper.loadNodesInternal - all specs complete, inserted count = $insertedRecordCount")
    insertedRecordCount
  }

  // ---------------------------------------------------------------------------
  // Relationship load (count-validation only; no dedup; no rollback)
  // ---------------------------------------------------------------------------
  private def loadRelationshipsInternal(
    spark: SparkSession,
    neo4jInfo: TableMetaInfo,
    tsCol: String,
    effectiveMode: String,
    specs: List[RelationshipSpec],
    df: DataFrame,
    projectColumns: Boolean
  ): Long = {
    logger.info(s"Neo4JLoadHelper.loadRelationshipsInternal - started, specs=${specs.length}, projectColumns=$projectColumns")
    requireColumn(df, tsCol, "relationship")

    val edgeSaveMode = effectiveRelationshipSaveMode(effectiveMode)
    val endpointSaveMode = endpointSaveModeFrom(effectiveMode)

    var insertedRecordCount = 0L
    specs.foreach { spec =>
      val relType = spec.relationshipType
      val sourceLabels = formatLabels(spec.source.labels)
      val targetLabels = formatLabels(spec.target.labels)
      val sourceKeyColumns = dfKeyColumns(spec.source.nodeKeys)
      val targetKeyColumns = dfKeyColumns(spec.target.nodeKeys)
      requireColumns(df, sourceKeyColumns ++ targetKeyColumns, s"relationship rel=$relType")

      // Whole DF for rel-only; projected (properties + src/tgt keys + audit) for "both".
      val relDf = projectFor(df, projectColumns, spec.properties, sourceKeyColumns ++ targetKeyColumns, tsCol, s"relationship rel=$relType")

      val expectedCount = relDf.count()
      logger.info(
        s"Neo4JLoadHelper.loadRelationshipsInternal - writing $expectedCount relationships rel=$relType " +
          s"src=$sourceLabels(keys=${spec.source.nodeKeys}) tgt=$targetLabels(keys=${spec.target.nodeKeys}) " +
          s"edgeSaveMode=$edgeSaveMode endpointSaveMode=$endpointSaveMode"
      )
      writeRelationshipsToNeo4J(
        df = relDf,
        relationshipType = relType,
        sourceLabels = sourceLabels,
        targetLabels = targetLabels,
        sourceNodeKeys = spec.source.nodeKeys,
        targetNodeKeys = spec.target.nodeKeys,
        sourceSaveMode = endpointSaveMode,
        targetSaveMode = endpointSaveMode,
        saveMode = edgeSaveMode,
        neo4jInfo = neo4jInfo
      )

      val tsFilter = buildTsFilter("r", tsCol, resolveTsTokens(relDf, tsCol, "relationship"))
      val countQuery = s"MATCH ()-[r:$relType]->() WHERE $tsFilter RETURN count(r) AS count"
      val insertedForGroup = readCountFromNeo4J(spark, neo4jInfo, countQuery)
      insertedRecordCount += insertedForGroup
      logger.info(s"Neo4JLoadHelper.loadRelationshipsInternal - validation rel=$relType inserted=$insertedForGroup expected=$expectedCount")
      if (insertedForGroup != expectedCount) {
        throw CODIException(
          s"Neo4J relationship count mismatch for rel=$relType; inserted=$insertedForGroup expected=$expectedCount"
        )
      }
      logger.info(s"Neo4JLoadHelper.loadRelationshipsInternal - completed rel=$relType ($expectedCount relationships)")
    }

    logger.info(s"Neo4JLoadHelper.loadRelationshipsInternal - all specs complete, inserted count = $insertedRecordCount")
    insertedRecordCount
  }

  // ---------------------------------------------------------------------------
  // Spark -> Neo4J writers (from Neo4JWriter, password decrypted)
  // ---------------------------------------------------------------------------
  private def writeNodesToNeo4J(
    df: DataFrame,
    labels: String,
    nodeKeys: String,
    saveMode: String,
    neo4jInfo: TableMetaInfo
  ): Unit = {
    val url = neo4jInfo.url
    val user = neo4jInfo.user
    val pw = password(neo4jInfo)
    val db = neo4jInfo.db
    logger.info(s"Neo4JLoadHelper.writeNodesToNeo4J - url=$url db=$db labels=$labels nodeKeys=$nodeKeys mode=$saveMode")
    df.write
      .format(NEO4J_FORMAT)
      .mode(saveModeFrom(saveMode))
      .option("url", url)
      .option("authentication.basic.username", user)
      .option("authentication.basic.password", pw)
      .option("database", db)
      .option("labels", labels)
      .option("node.keys", nodeKeys)
      .option("schema.optimization.type", "NODE_CONSTRAINTS")
      .save()
    logger.info(s"Neo4JLoadHelper.writeNodesToNeo4J - completed: labels=$labels")
  }

  private def writeRelationshipsToNeo4J(
    df: DataFrame,
    relationshipType: String,
    sourceLabels: String,
    targetLabels: String,
    sourceNodeKeys: String,
    targetNodeKeys: String,
    sourceSaveMode: String,
    targetSaveMode: String,
    saveMode: String,
    neo4jInfo: TableMetaInfo
  ): Unit = {
    val url = neo4jInfo.url
    val user = neo4jInfo.user
    val pw = password(neo4jInfo)
    val db = neo4jInfo.db
    logger.info(s"Neo4JLoadHelper.writeRelationshipsToNeo4J - url=$url db=$db rel=$relationshipType")
    logger.info(s" source: $sourceLabels (key=$sourceNodeKeys saveMode=$sourceSaveMode)")
    logger.info(s" target: $targetLabels (key=$targetNodeKeys saveMode=$targetSaveMode)")
    logger.info(s" relationship saveMode=$saveMode")
    df.write
      .format(NEO4J_FORMAT)
      .mode(saveModeFrom(saveMode))
      .option("url", url)
      .option("authentication.basic.username", user)
      .option("authentication.basic.password", pw)
      .option("database", db)
      .option("relationship", relationshipType)
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.labels", sourceLabels)
      .option("relationship.source.save.mode", sourceSaveMode)
      .option("relationship.source.node.keys", sourceNodeKeys)
      .option("relationship.target.labels", targetLabels)
      .option("relationship.target.save.mode", targetSaveMode)
      .option("relationship.target.node.keys", targetNodeKeys)
      .save()
    logger.info(s"Neo4JLoadHelper.writeRelationshipsToNeo4J - completed: rel=$relationshipType")
  }

  // ---------------------------------------------------------------------------
  // Neo4J read-back count
  // ---------------------------------------------------------------------------
  private def readCountFromNeo4J(spark: SparkSession, neo4jInfo: TableMetaInfo, cypher: String): Long = {
    val outDf = spark.read
      .format(NEO4J_FORMAT)
      .option("uri", neo4jInfo.url)
      .option("authentication.basic.username", neo4jInfo.user)
      .option("authentication.basic.password", password(neo4jInfo))
      .option("database", neo4jInfo.db)
      .option("query", cypher)
      .load()
    if (!outDf.head(1).isEmpty) outDf.collect()(0).get(0).toString.toLong else 0L
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Returns the DataFrame to write for a spec.
   *   - projectColumns=false (node-only / relationship-only): whole DataFrame.
   *   - projectColumns=true  ("both") with properties supplied: project
   *     properties + key columns + audit ts column.
   *   - projectColumns=true with no properties: whole DataFrame.
   * No distinct / dedup is performed.
   */
  private def projectFor(
    df: DataFrame,
    projectColumns: Boolean,
    properties: Option[List[String]],
    keyColumns: Seq[String],
    tsCol: String,
    context: String
  ): DataFrame = {
    import org.apache.spark.sql.functions.col
    (projectColumns, properties) match {
      case (true, Some(props)) =>
        val wanted = (props ++ keyColumns ++ Seq(tsCol)).distinct
        requireColumns(df, wanted, context)
        df.select(wanted.map(col): _*)
      case _ =>
        df
    }
  }

  private def resolveTsTokens(df: DataFrame, tsCol: String, kind: String): List[String] = {
    val tsValues =
      df.select(tsCol)
        .distinct()
        .collect()
        .map(_.get(0))
        .filter(_ != null)
        .map(_.toString)
        .filter(_.trim.nonEmpty)
        .toList
    if (tsValues.isEmpty) {
      logger.error(s"Neo4JLoadHelper - '$tsCol' missing/empty in source; cannot validate inserted $kind counts")
      throw CODIException(s"Missing $tsCol values for Neo4J $kind validation")
    }
    val tsTokens = tsValues.map(normalizeTsToken).filter(_.nonEmpty).distinct
    if (tsTokens.isEmpty) {
      logger.error(s"Neo4JLoadHelper - '$tsCol' resolved to empty timestamp tokens; cannot validate inserted $kind counts")
      throw CODIException(s"Invalid $tsCol values for Neo4J $kind validation")
    }
    tsTokens
  }

  private def buildTsFilter(alias: String, tsCol: String, tsTokens: List[String]): String =
    s"replace(substring(toString($alias.$tsCol), 0, 19), ' ', 'T') IN [${tsTokens.map(cypherQuote).mkString(", ")}]"

  private def normalizeTsToken(value: String): String =
    value.replace(" ", "T").replaceAll("\\.\\d+", "").trim

  private def cypherQuote(value: String): String =
    "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

  /** Returns the DataFrame column names from a (possibly "dfCol:nodeProp") comma-separated key spec. */
  private def dfKeyColumns(nodeKeys: String): Seq[String] =
    nodeKeys.split(",").map(_.split(":")(0).trim).filter(_.nonEmpty).toSeq

  private def formatLabels(raw: String): String = {
    val cleaned = raw.trim.replace("|", ":")
    if (cleaned.startsWith(":")) cleaned else ":" + cleaned
  }

  private def requireColumn(df: DataFrame, column: String, kind: String): Unit = {
    if (!df.columns.contains(column)) {
      throw CODIException(s"Neo4J $kind load - required column '$column' not found in DataFrame [${df.columns.mkString(",")}]")
    }
  }

  private def requireColumns(df: DataFrame, columns: Seq[String], context: String): Unit = {
    val missing = columns.distinct.filterNot(df.columns.contains)
    if (missing.nonEmpty) {
      throw CODIException(s"Neo4J $context - missing required column(s) [${missing.mkString(",")}] in DataFrame [${df.columns.mkString(",")}]")
    }
  }

  private def password(neo4jInfo: TableMetaInfo): String =
    EncryptDecryptUtil.decrypt(neo4jInfo.enpw)

  private def saveModeFrom(mode: String): SaveMode = mode.toLowerCase match {
    case "overwrite" => SaveMode.Overwrite
    case "merge" => SaveMode.Overwrite
    case "ignore" => SaveMode.Ignore
    case "error" => SaveMode.ErrorIfExists
    case _ => SaveMode.Append
  }

  /** Relationship (edge) save mode -> connector mode string (merge vs create). */
  private def effectiveRelationshipSaveMode(requestedMode: String): String =
    Option(requestedMode).map(_.trim.toLowerCase) match {
      case Some("ignore") => "ignore"
      case Some("error") => "error"
      case Some("errorifexists") => "error"
      case Some("append") => "overwrite"
      case _ => "overwrite"
    }

  /**
   * Relationship endpoint-node save mode (uses merge or match based on saveMode):
   *   match            -> Match     (endpoints must already exist)
   *   overwrite/merge  -> Overwrite (MERGE: create endpoint nodes if missing)
   *   append           -> Append    (CREATE endpoint nodes)
   */
  private def endpointSaveModeFrom(mode: String): String = mode.trim.toLowerCase match {
    case "match" => "Match"
    case "append" => "Append"
    case "overwrite" => "Overwrite"
    case "merge" => "Overwrite"
    case _ => "Overwrite"
  }
}
