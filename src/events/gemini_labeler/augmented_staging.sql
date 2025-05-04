CREATE TABLE IF NOT EXISTS `poised-space-456813-t0.linkedin.linkedin_augmented_staging`
(
  -- Job Summary Section (Matches template)
  job_summary STRUCT<
    role_title STRING,
    role_objective STRING,
    role_seniority STRING,
    visa_sponsorship BOOL
  >,

  -- Company Information Section (Matches template)
  company_information STRUCT<
    company_type STRING,
    company_values_keywords ARRAY<STRING>
  >,

  -- Location and Work Model Section (Matches template)
  location_and_work_model STRUCT<
    specification_level STRING,
    remote_status STRING,
    flexibility ARRAY<STRING>,
    locations ARRAY<STRING>
  >,

  -- Required Qualifications Section (Matches template)
  required_qualifications STRUCT<
    experience_years_min INT64,
    experience_years_max INT64,
    experience_description STRING,
    education_requirements STRING,
    technical_skills STRUCT<
      programming_languages STRUCT<
        general_purpose ARRAY<STRING>,
        scripting_frontend ARRAY<STRING>,
        query ARRAY<STRING>,
        data_ml_libs ARRAY<STRING>,
        platform_runtime ARRAY<STRING>,
        configuration ARRAY<STRING>,
        other_specialized ARRAY<STRING>
      >,
      -- Fields below match the template's technical_skills structure:
      cloud_services_tools ARRAY<STRING>, -- Included as per template
      cloud_platforms ARRAY<STRING>,
      database_technologies ARRAY<STRING>,
      data_architecture_concepts STRUCT<
        data_modeling ARRAY<STRING>,
        data_storage_paradigms ARRAY<STRING>,
        etl_elt_pipelines ARRAY<STRING>,
        data_governance_quality ARRAY<STRING>,
        architecture_patterns ARRAY<STRING>,
        big_data_concepts ARRAY<STRING>,
        cloud_data_architecture ARRAY<STRING>,
        ml_ai_data_concepts ARRAY<STRING>,
        core_principles_optimization ARRAY<STRING>
      >,
      etl_integration_tools ARRAY<STRING>,
      data_visualization_bi_tools ARRAY<STRING>,
      devops_mlops_ci_cd_tools ARRAY<STRING>,
      orchestration_workflow_tools ARRAY<STRING>,
      other_tools ARRAY<STRING>
    >,
    methodologies_practices ARRAY<STRING>,
    soft_skills_keywords ARRAY<STRING>
  >,

  -- Preferred Qualifications Section (Matches template)
  preferred_qualifications STRUCT<
    skills_keywords ARRAY<STRING>,
    other_notes STRING
  >,

  -- Role Context Section (Matches template)
  role_context STRUCT<
    collaboration_with ARRAY<STRING>,
    team_structure STRING,
    project_scope STRING,
    key_responsibilities ARRAY<STRING>
  >,

  -- Benefits Section (Matches template)
  benefits STRUCT<
    training_development STRING,
    learning_platforms ARRAY<STRING>,
    paid_time_off_days INT64,
    other_benefits_keywords ARRAY<STRING>
  >,

  -- Job Identifier (Assumed needed, matches previous DDL)
  job_id STRING
);