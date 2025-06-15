-- Data Lineage Documentation Queries (FIXED)
-- Purpose: Generate comprehensive lineage reports and visual documentation

-- Query 1: Complete Data Lineage Flow (Source to Target)
CREATE OR REPLACE VIEW `data-demo-etl.data_warehouse.v_complete_lineage` AS
WITH lineage_flow AS (
  SELECT 
    lr.relationship_id,
    ls.source_name,
    ls.source_type,
    ls.source_location,
    lt.target_name,
    lt.target_type,
    CONCAT(lt.dataset_name, '.', lt.table_name) as target_full_name,
    tr.transformation_name,
    tr.transformation_type,
    tr.transformation_logic,
    tr.business_rule,
    lr.relationship_type,
    lr.confidence_score,
    lr.pipeline_name,
    lr.dag_id,
    lr.task_id,
    lr.is_active
  FROM `data-demo-etl.data_warehouse.lineage_relationships` lr
  JOIN `data-demo-etl.data_warehouse.lineage_sources` ls ON lr.source_id = ls.source_id
  JOIN `data-demo-etl.data_warehouse.lineage_targets` lt ON lr.target_id = lt.target_id
  LEFT JOIN `data-demo-etl.data_warehouse.lineage_transformations` tr ON lr.transformation_id = tr.transformation_id
  WHERE lr.is_active = TRUE
)
SELECT * FROM lineage_flow
ORDER BY pipeline_name, target_type, target_name;

-- Query 2: Field-Level Lineage Mapping
CREATE OR REPLACE VIEW `data-demo-etl.data_warehouse.v_field_lineage` AS
SELECT 
  tr.transformation_id,
  tr.transformation_name,
  tr.source_field,
  tr.target_field,
  tr.transformation_logic,
  tr.business_rule,
  tr.dag_id,
  tr.task_id,
  lr.pipeline_name,
  ls.source_name,
  lt.target_name,
  lr.confidence_score
FROM `data-demo-etl.data_warehouse.lineage_transformations` tr
JOIN `data-demo-etl.data_warehouse.lineage_relationships` lr ON tr.transformation_id = lr.transformation_id
JOIN `data-demo-etl.data_warehouse.lineage_sources` ls ON lr.source_id = ls.source_id
JOIN `data-demo-etl.data_warehouse.lineage_targets` lt ON lr.target_id = lt.target_id
WHERE lr.is_active = TRUE
ORDER BY lr.pipeline_name, tr.transformation_type, tr.transformation_name;

-- Query 3: Data Quality Impact Analysis
CREATE OR REPLACE VIEW `data-demo-etl.data_warehouse.v_lineage_quality_impact` AS
WITH quality_lineage AS (
  SELECT 
    lt.target_name,
    CONCAT(lt.dataset_name, '.', lt.table_name) as target_full_name,
    COUNT(tr.transformation_id) as transformation_count,
    COUNT(CASE WHEN tr.transformation_type = 'CLEANING' THEN 1 END) as cleaning_transformations,
    COUNT(CASE WHEN tr.transformation_type = 'FILTERING' THEN 1 END) as filtering_transformations,
    AVG(lr.confidence_score) as avg_confidence_score,
    STRING_AGG(DISTINCT lr.pipeline_name) as contributing_pipelines,
    MAX(lr.execution_date) as last_updated
  FROM `data-demo-etl.data_warehouse.lineage_relationships` lr
  JOIN `data-demo-etl.data_warehouse.lineage_targets` lt ON lr.target_id = lt.target_id
  LEFT JOIN `data-demo-etl.data_warehouse.lineage_transformations` tr ON lr.transformation_id = tr.transformation_id
  WHERE lr.is_active = TRUE
  GROUP BY lt.target_name, lt.dataset_name, lt.table_name
)
SELECT 
  target_name,
  target_full_name,
  transformation_count,
  cleaning_transformations,
  filtering_transformations,
  ROUND(avg_confidence_score, 3) as avg_confidence_score,
  contributing_pipelines,
  last_updated,
  CASE 
    WHEN avg_confidence_score >= 0.95 THEN 'HIGH_CONFIDENCE'
    WHEN avg_confidence_score >= 0.85 THEN 'MEDIUM_CONFIDENCE'
    ELSE 'LOW_CONFIDENCE'
  END as quality_rating
FROM quality_lineage
ORDER BY avg_confidence_score DESC;

-- Query 4: Pipeline Dependency Analysis
CREATE OR REPLACE VIEW `data-demo-etl.data_warehouse.v_pipeline_dependencies` AS
WITH pipeline_deps AS (
  SELECT 
    lr.pipeline_name as downstream_pipeline,
    lr.dag_id as downstream_dag,
    ls.source_type as upstream_source_type,
    ls.source_name as upstream_source,
    COUNT(*) as dependency_count,
    STRING_AGG(DISTINCT lt.target_name) as affected_targets,
    AVG(lr.confidence_score) as avg_confidence
  FROM `data-demo-etl.data_warehouse.lineage_relationships` lr
  JOIN `data-demo-etl.data_warehouse.lineage_sources` ls ON lr.source_id = ls.source_id
  JOIN `data-demo-etl.data_warehouse.lineage_targets` lt ON lr.target_id = lt.target_id
  WHERE lr.is_active = TRUE
  GROUP BY lr.pipeline_name, lr.dag_id, ls.source_type, ls.source_name
)
SELECT 
  downstream_pipeline,
  downstream_dag,
  upstream_source_type,
  upstream_source,
  dependency_count,
  affected_targets,
  ROUND(avg_confidence, 3) as avg_confidence,
  CASE 
    WHEN dependency_count = 1 THEN 'SIMPLE'
    WHEN dependency_count <= 3 THEN 'MODERATE'
    ELSE 'COMPLEX'
  END as complexity_level
FROM pipeline_deps
ORDER BY downstream_pipeline, dependency_count DESC;

-- Query 5: Lineage Summary Report
SELECT 
  'SOURCES' as category,
  COUNT(*) as count,
  STRING_AGG(DISTINCT source_type) as types
FROM `data-demo-etl.data_warehouse.lineage_sources`

UNION ALL

SELECT 
  'TARGETS' as category,
  COUNT(*) as count,
  STRING_AGG(DISTINCT target_type) as types
FROM `data-demo-etl.data_warehouse.lineage_targets`

UNION ALL

SELECT 
  'TRANSFORMATIONS' as category,
  COUNT(*) as count,
  STRING_AGG(DISTINCT transformation_type) as types
FROM `data-demo-etl.data_warehouse.lineage_transformations`

UNION ALL

SELECT 
  'RELATIONSHIPS' as category,
  COUNT(*) as count,
  STRING_AGG(DISTINCT relationship_type) as types
FROM `data-demo-etl.data_warehouse.lineage_relationships`
WHERE is_active = TRUE

ORDER BY category;