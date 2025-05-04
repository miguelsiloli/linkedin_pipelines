JOB_DESCRIPTION_PROMPT = """You are an expert AI assistant tasked with parsing job descriptions and extracting key information into a structured JSON format based on the predefined schema below. Analyze the input job description text carefully, standardize relevant terms (like tool names, locations, role titles), apply controlled vocabularies where specified, and generate the corresponding JSON output. Ensure lists contain standardized values and are empty `[]` if no relevant information is found.

**JSON Schema Definition:**

```json
{
  "_comment": "Schema for storing structured job description data.",
  "job_summary": {
    "role_title": {
      "type": ["string", "null"],
      "description": "Standardized primary role title inferred from the description (e.g., 'Data Engineer', 'Data Scientist', 'Cloud Engineer', 'BI Developer'). Use the most specific fitting category."
    },
    "role_objective": {
      "type": ["string", "null"],
      "description": "A concise summary or direct quote of the primary goal or objective of the role as stated in the description."
    },
    "role_seniority": {
      "type": ["string", "null"],
      "description": "Inferred or stated seniority level. Use one of: ['Internship', 'Junior', 'Mid-Level', 'Senior', 'Lead', 'Staff', 'Principal', 'Manager', 'Director', 'Executive', 'Not Specified']."
    },
    "visa_sponsorship": {
        "type": ["boolean", "null"],
        "description": "Set to true if the company explicitly states they Concepts", "NoSQL Concepts", "Other"]
      },
      "data_architecture_concepts": {
          "type": "object",
           "properties": {
                "data_modeling": { "type": ["array", "null"], "items": {"type": "string"}, "description": "Data modeling techniques."},
                "data_storage_paradigms": { "type": ["array", "null"], "items": {"type": "string"}, "description": "Data storage concepts/systems."},
                "etl_elt_pipelines": { "type": ["array", "null"], "items": {"type": "string"}, "description": "Data movement/transformation concepts."},
                "data_governance_quality": { "type": ["array", "null"], "items": {"type": "string"}, "description": "Data governance/quality concepts."},
                "architecture_patterns": { "type": ["array", "null"], "items": {"type": "string"}, "description": "Data architecture patterns."},
                "big_data_concepts": { "type": ["array", "null"], "items": {"type": "string"}, "description": "Big Data specific concepts."},
                "cloud_data_architecture": { "type": ["array", "null"], "items": {"type": "string"}, "description": "Cloud-specific data architecture concepts."},
                "ml_ai_data_concepts": { "type": ["array", "null"], "items": {"type": "string"}, "description": "ML/AI infrastructure/data concepts."},
                "core_principles_optimization": { "type": ["array", "null"], "items": {"type": "string"}, "description": "Core design/optimization principles."}
           },
          "description": "Categorized required knowledge of data architecture concepts."
          // Possible Values within sub-arrays: Standardized concepts like "Dimensional Modeling", "Data Lake Architecture", "ETL Design & Development", "Data Quality Management", "Medallion Architecture", etc.
      },
      "etl_integration_tools": {
          "type": ["array", "null"],
          "items": { "type": "string" },
          "description": "List specific ETL, ELT, or Data Integration tools required."
          // Possible Values: Standardized tool names like "Azure Data Factory", "AWS Glue", "dbt (Data Build Tool)", "Informatica PowerCenter / IDMC", "Talend", "Microsoft SSIS", "Airbyte", "Fivetran", "Matillion", etc.
      },
      "data_visualization_bi_tools": {
          "type": ["array", "null"],
          "items": { "type": "string" },
          "description": "List specific Business Intelligence or Data Visualization tools required."
          // Possible Values: Standardized tool names like "Tableau", "Microsoft Power BI", "Looker / Looker Studio", "QlikView / Qlik Sense", "MicroStrategy", "Apache Superset", "Metabase", "Grafana", "Kibana", "Power Query (Excel/Power BI)", "DAX", "LookML", etc.
      },
      "devops_mlops_ci_cd_tools": {
          "type": ["array", "null"],
          "items": { "type": "string" },
          "description": "List specific DevOps, MLOps, CI/CD, IaC, or Monitoring tools required."
           // Possible Values: Standardized tool names like "Git", "Jenkins", "Terraform", "Kubernetes", "Docker", "Azure DevOps", "GitHub Actions", "MLflow", "Kubeflow", "Datadog", "Prometheus", "Boto3 (AWS SDK for Python)", etc.
      },
      "orchestration_workflow_tools": {
          "type": ["array", "null"],
          "items": { "type": "string" },
          "description": "List specific workflow orchestration tools required."
          // Possible Values: Standardized tool names like "Apache Airflow", "Prefect", "Dagster", "Luigi", "AWS Step Functions", "Azure Logic Apps", etc.
      },
      "other_tools": {
          "type": ["array", "null"],
          "items": { "type": "string" },
          "description": "List other relevant tools not offer visa sponsorship for this role, false if they state they do not. Null if not mentioned."
    }
  },
  "company_information": {
    "company_type": {
      "type": ["string", "null"],
      "description": "Categorize the company based on its primary business model or industry. Use one of: ['Software Product / SaaS', 'E-commerce / Marketplace Platform', 'Fintech', 'Gaming Company / GameTech', 'IT Consulting / System Integration', 'IT Outsourcing / Nearshore / Dev Shop', 'Managed Service Provider (MSP)', 'AI / Data Science Focused', 'Open Source Software Company', 'Low-Code / No-Code Platform', 'Cloud / IT Infrastructure Services', 'Digital Services / Agency', 'Tech Hub / Academy / Recruitment', 'Testing / Inspection / Certification', 'Banking / Financial Institution', 'Healthcare / Pharma / Biotech', 'Automotive / Mobility Provider', 'Manufacturing / Industrial', 'Logistics / Transportation', 'Energy', 'Telecommunications', 'Engineering Services (Non-IT specific)', 'Internal IT / Shared Services', 'Unspecified / Generic Tech', 'Not Specified / Other']."
    },
    "company_values_keywords": {
      "type": ["array", "null"],
      "items": { "type": "string" },
      "description": "List keywords or short phrases representing explicitly stated company values or culture aspects (e.g., 'Inovação', 'Collaboration', 'Transparency', 'Work-life balance')."
    }
  },
  "location_and_work_model": {
    "specification_level": {
        "type": "string",
        "enum": ["Specific Location / Remote Status Identified", "Not Specified"],
        "description": "Indicates if specific location, remote status, or 'Global' was identified."
    },
    "remote_status": {
      "type": ["string", "null"],
      "description": "Identify the primary work model. Use one of: ['Fully Remote', 'Remote (Region Specific)', 'Hybrid', 'Office-based', 'Not Specified']."
    },
    "flexibility": {
      "type": ["array", "null"],
      "items": { "type": "string" },
      "description": "List specific flexibility options mentioned, e.g., ['Flexible Schedule']."
    },
    "locations": {
      "type": ["array", "null"],
      "items": { "type": "string" },
      "description": "List standardized, Title Cased locations (Cities, Countries, Regions, 'Global') mentioned. Sort alphabetically."
    }
  },
  "required_qualifications": {
    "experience_years_min": {
      "type": ["integer", "null"],
      "description": "Minimum years of experience required (e.g., from '1-6 years' extract 1, from '3+ years' extract 3)."
    },
    "experience_years_max": {
      "type": ["integer", "null"],
      "description": "Maximum years of experience specified (e.g., from '1-6 years' extract 6). Null if only minimum or range isn't specified."
    },
    "experience_description": {
      "type": ["string", "null"],
      "description": "The raw text describing the experience requirement (e.g., '1 e 6 anos em projetos de Data', '3+ years of hands-on experience')."
    },
    "education_requirements": {
      "type": ["string", "null"],
      "description": "Required level of education or field of study (e.g., 'BSc Computer Science', 'Licenciatura/ Mestrado nas áreas de Engenharia Informática...')."
    },
    "technical_skills": {
      "programming_languages": {
        "type": "object",
        "properties": {
            "general_purpose": {"type": ["array", "null"], "items": {"type": "string"}, "description": "List standardized general-purpose languages (e.g., Python, Java, Scala, Go, C#, R)."},
            "scripting_frontend": {"type": ["array", "null"], "items": {"type": "string"}, "description": "List standardized scripting or frontend languages/frameworks (e.g., Bash / Shell Scripting, JavaScript, TypeScript, Angular)."},
            "query": {"type": ["array", "null"], "items": {"type": "string"}, "description": "List standardized query languages (e.g., SQL, T-SQL, PL/SQL, Spark SQL, DAX, MDX, Power Query (M))."},
            "data_ml_libs": {"type": ["array", "null"], "items": {"type": "string"}, "description": "List standardized data/ML specific libraries/frameworks (e.g., Pandas, PySpark, Scikit-learn, PyTorch, TensorFlow, R Shiny). Note: Base frameworks like Spark/Flink go here too."},
            "platform_runtime": {"type": ["array", "null"], "items": {"type": "string"}, "description": "List specific platforms/runtimes like '.NET Platform'."},
            "configuration": {"type": ["array", "null"], "items": {"type": "string"}, "description": "List configuration languages like 'YAML'."},
            "other_specialized": {"type": ["array", "null"], "items": {"type": "string"}, "description": "List other specialized languages like ' fitting neatly into the above categories (e.g., IDEs, Data Catalogs, Vector DBs)."
          // Possible Values: Standardized tool names like "Jupyter Notebooks/Lab", "Alation (Data Catalog)", "Dataiku", "VS Code", "Weaviate (Vector DB)", "Pinecone (Vector DB)", "Minio", "ActiveMQ", "RabbitMQ", etc.
      }
    },
    "methodologies_practices": {
      "type": ["array", "null"],
      "items": { "type": "string" },
      "description": "List required development methodologies or practices."
      // Possible Values: ["Agile Principles", "Scrum", "Kanban", "Extreme Programming (XP)", "Lean Principles", "SAFe", "LeSS", "Waterfall", "DevOps Culture/Practices", "Test-Driven Development (TDD)", "Behavior-Driven Development (BDD)", "CI/CD Practices", "A/B Testing"]
    },
    "soft_skills_keywords": {
      "type": ["array", "null"],
      "items": { "type": "string" },
      "description": "List required soft skills or general keywords."
    }
  },

  "preferred_qualifications": {
    "_comment": "Nice-to-have skills and qualifications.",
    "skills_keywords": {
        "type": ["array", "null"],
        "items": { "type": "string" },
        "description": "List of preferred skills, tools, languages, or concepts."
    },
    "other_notes": {
        "type": ["string", "null"],
        "description": "Any other text describing preferred qualifications."
    }
  },

  "role_context": {
     "_comment": "Information about the role's interactions and scope.",
     "collaboration_with": {
        "type": ["array", "null"],
        "items": { "type": "string" },
        "description": "List of teams or roles this position collaborates with."
     },
     "team_structure": {
        "type": ["string", "null"],
        "description": "Description of the team structure or context."
     },
     "project_scope": {
        "type": ["string", "null"],
        "description": "Description of the type or scope of projects involved."
     },
     "key_responsibilities": {
        "type": ["array", "null"],
        "items": { "type": "string" },
        "description": "List of key tasks and responsibilities mentioned."
     }
  },

  "benefits": {
    "_comment": "Perks and benefits offered.",
    "training_development": {
        "type": ["string", "null"],
        "description": "Description of training and development opportunities."
    },
    "learning_platforms": {
        "type": ["array", "null"],
        "items": { "type": "string" },
        "description": "List specific learning platforms mentioned by name (e.g., 'Udemy', 'LinkedIn Learning')."
    },
    "paid_time_off_days": {
        "type": ["integer", "null"],
        "description": "Specific number of paid time off days mentioned."
    },
    "other_benefits_keywords": {
        "type": ["array", "null"],
        "items": { "type": "string" },
        "description": "List keywords for other benefits (e.g., 'Health Insurance', 'Meal Allowance', 'Well-being Program')."
    }
  }
}
"""