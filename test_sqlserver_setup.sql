/* ============================================================================
 *  Neo4J loader test data  (SQL Server / T-SQL)
 *  Source tables for 3 scenarios moved SQL Server -> Neo4J:
 *     1. NODE ONLY            -> CODI.SEE_TEST_CUSTOMER_NODES
 *     2. RELATIONSHIP ONLY    -> CODI.SEE_TEST_ORDER_REL
 *     3. NODES + RELATIONSHIP -> CODI.SEE_TEST_ORDER_CUSTOMER
 *
 *  Notes:
 *   - `codi_ingestion_ts` is the audit timestamp column (config customIngestionTS).
 *     It MUST be present and is used to scope the post-write Neo4J count read-back.
 *     One batch value is used per table so the count filter has a single token.
 *   - Validation is COUNT-ONLY with NO dedup, so keys are unique per row. In the
 *     "both" table each order has a UNIQUE customer (1:1) so distinct Customer
 *     count == row count and count validation passes.
 * ==========================================================================*/

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'CODI')
    EXEC('CREATE SCHEMA CODI');
GO

/* --------------------------------------------------------------------------
 * 1) NODE ONLY  ->  :Customer  (nodeKeys = customer_id)
 * ------------------------------------------------------------------------*/
IF OBJECT_ID('CODI.SEE_TEST_CUSTOMER_NODES', 'U') IS NOT NULL
    DROP TABLE CODI.SEE_TEST_CUSTOMER_NODES;
GO

CREATE TABLE CODI.SEE_TEST_CUSTOMER_NODES (
    customer_id        VARCHAR(20)   NOT NULL,
    customer_name      VARCHAR(100)  NOT NULL,
    customer_segment   VARCHAR(30)   NULL,
    city               VARCHAR(50)   NULL,
    codi_ingestion_ts  DATETIME2(0)  NOT NULL,
    CONSTRAINT PK_SEE_TEST_CUSTOMER_NODES PRIMARY KEY (customer_id)
);
GO

INSERT INTO CODI.SEE_TEST_CUSTOMER_NODES
    (customer_id, customer_name, customer_segment, city, codi_ingestion_ts)
VALUES
    ('C001', 'Acme Corp',        'Enterprise', 'New York',   '2026-06-26 10:00:00'),
    ('C002', 'Globex LLC',       'SMB',        'Chicago',    '2026-06-26 10:00:00'),
    ('C003', 'Initech',          'Enterprise', 'Austin',     '2026-06-26 10:00:00'),
    ('C004', 'Umbrella Inc',     'SMB',        'Boston',     '2026-06-26 10:00:00'),
    ('C005', 'Stark Industries', 'Enterprise', 'Seattle',    '2026-06-26 10:00:00'),
    ('C006', 'Wayne Enterprises','Enterprise', 'San Diego',  '2026-06-26 10:00:00'),
    ('C007', 'Soylent Co',       'SMB',        'Denver',     '2026-06-26 10:00:00');
GO
-- expected :Customer nodes = 7

/* --------------------------------------------------------------------------
 * 2) RELATIONSHIP ONLY  ->  (:Order)-[:PLACED_BY]->(:Customer)
 *    source key = order_id, target key = customer_id.
 *    Endpoints are created if missing (saveMode overwrite => Merge).
 * ------------------------------------------------------------------------*/
IF OBJECT_ID('CODI.SEE_TEST_ORDER_REL', 'U') IS NOT NULL
    DROP TABLE CODI.SEE_TEST_ORDER_REL;
GO

CREATE TABLE CODI.SEE_TEST_ORDER_REL (
    order_id           VARCHAR(20)   NOT NULL,
    customer_id        VARCHAR(20)   NOT NULL,
    order_channel      VARCHAR(30)   NULL,   -- becomes a relationship property
    codi_ingestion_ts  DATETIME2(0)  NOT NULL,
    CONSTRAINT PK_SEE_TEST_ORDER_REL PRIMARY KEY (order_id, customer_id)
);
GO

INSERT INTO CODI.SEE_TEST_ORDER_REL
    (order_id, customer_id, order_channel, codi_ingestion_ts)
VALUES
    ('O1001', 'C001', 'WEB',    '2026-06-26 10:05:00'),
    ('O1002', 'C002', 'MOBILE', '2026-06-26 10:05:00'),
    ('O1003', 'C003', 'WEB',    '2026-06-26 10:05:00'),
    ('O1004', 'C001', 'STORE',  '2026-06-26 10:05:00'),
    ('O1005', 'C004', 'WEB',    '2026-06-26 10:05:00'),
    ('O1006', 'C005', 'MOBILE', '2026-06-26 10:05:00');
GO
-- expected [:PLACED_BY] relationships = 6
-- (each (order_id, customer_id) pair is unique)

/* --------------------------------------------------------------------------
 * 3) NODES + RELATIONSHIP (single DataFrame, split by column projection)
 *    :Order (key order_id), :Customer (key customer_id),
 *    (:Order)-[:PLACED_BY]->(:Customer).
 *    1:1 order-to-customer so all count validations pass.
 * ------------------------------------------------------------------------*/
IF OBJECT_ID('CODI.SEE_TEST_ORDER_CUSTOMER', 'U') IS NOT NULL
    DROP TABLE CODI.SEE_TEST_ORDER_CUSTOMER;
GO

CREATE TABLE CODI.SEE_TEST_ORDER_CUSTOMER (
    order_id           VARCHAR(20)    NOT NULL,
    order_amount       DECIMAL(12,2)  NOT NULL,
    order_status       VARCHAR(20)    NULL,
    customer_id        VARCHAR(20)    NOT NULL,
    customer_name      VARCHAR(100)   NOT NULL,
    customer_city      VARCHAR(50)    NULL,
    codi_ingestion_ts  DATETIME2(0)   NOT NULL,
    CONSTRAINT PK_SEE_TEST_ORDER_CUSTOMER PRIMARY KEY (order_id)
);
GO

INSERT INTO CODI.SEE_TEST_ORDER_CUSTOMER
    (order_id, order_amount, order_status, customer_id, customer_name, customer_city, codi_ingestion_ts)
VALUES
    ('O2001', 1200.00, 'SHIPPED',   'D001', 'Northwind Traders', 'New York',  '2026-06-26 11:00:00'),
    ('O2002',  450.50, 'PENDING',   'D002', 'Contoso Ltd',       'Chicago',   '2026-06-26 11:00:00'),
    ('O2003',  980.75, 'SHIPPED',   'D003', 'Fabrikam Inc',      'Austin',    '2026-06-26 11:00:00'),
    ('O2004',  250.00, 'CANCELLED', 'D004', 'Adventure Works',   'Boston',    '2026-06-26 11:00:00'),
    ('O2005', 3100.25, 'SHIPPED',   'D005', 'Tailspin Toys',     'Seattle',   '2026-06-26 11:00:00'),
    ('O2006',  675.00, 'PENDING',   'D006', 'Wingtip Toys',      'San Diego', '2026-06-26 11:00:00');
GO
-- expected :Order nodes = 6, :Customer nodes = 6, [:PLACED_BY] relationships = 6
