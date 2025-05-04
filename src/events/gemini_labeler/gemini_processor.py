# job-parser-project/src/gemini_processor.py
import os
import json
from google import genai
from google.genai import types
from google.api_core import exceptions as google_exceptions
from tqdm import tqdm # For logging within the function
from src.events.gemini_labeler.prompt import JOB_DESCRIPTION_PROMPT

# Import necessary config and potentially schema definition if moved
from . import config # Use relative import within the package
# If JOB_RESPONSE_SCHEMA is complex and defined elsewhere (e.g., data_models.py) import it
# from .data_models import JOB_RESPONSE_SCHEMA # Example if schema moved

# --- Gemini Client Setup (Helper Function) ---
def create_gemini_client():
    """Configures the global genai object with the API key."""
    client = genai.Client(
        api_key=os.environ.get("GEMINI_API_KEY"),
    )
    print(f"Successfully configured Gemini API key.")
    return client


JOB_RESPONSE_SCHEMA = genai.types.Schema(
    type=genai.types.Type.OBJECT,
    properties={
        # === Job Summary ===
        'job_summary': genai.types.Schema(
            type=genai.types.Type.OBJECT,
            description="High-level information about the role.",
            properties={
                'role_title': genai.types.Schema(
                    type=genai.types.Type.STRING,
                    description="Standardized primary role title inferred from the description (e.g., 'Data Engineer', 'Data Scientist')."
                ),
                'role_objective': genai.types.Schema(
                    type=genai.types.Type.STRING,
                    description="A concise summary or direct quote of the primary goal or objective of the role."
                ),
                'role_seniority': genai.types.Schema(
                    type=genai.types.Type.STRING,
                    description="Inferred or stated seniority level (e.g., 'Internship', 'Junior', 'Mid-Level', 'Senior', 'Lead', 'Manager', 'Not Specified')."
                    # Enum constraint typically handled by validation after extraction or in the prompt
                ),
                 'visa_sponsorship': genai.types.Schema(
                    type=genai.types.Type.BOOLEAN,
                    description="Set to true if the company explicitly states they offer visa sponsorship for this role, false if they state they do not. Null if not mentioned."
                 )
            }
            # Optionality implied by not being in a top-level 'required' list if one were defined
        ),

        # === Company Information ===
        'company_information': genai.types.Schema(
            type=genai.types.Type.OBJECT,
            description="Details about the hiring company.",
            properties={
                'company_type': genai.types.Schema(
                    type=genai.types.Type.STRING,
                    description="Categorization of the company's primary business model or industry."
                    # Enum constraint handled by validation/prompt.
                ),
                'company_values_keywords': genai.types.Schema(
                    type=genai.types.Type.ARRAY,
                    description="List of explicitly stated company values or cultural keywords.",
                    items=genai.types.Schema(type=genai.types.Type.STRING)
                )
            }
        ),

        # === Location and Work Model ===
        'location_and_work_model': genai.types.Schema(
            type=genai.types.Type.OBJECT,
            description="Where the role is based and the work model.",
            properties={
                'specification_level': genai.types.Schema(
                    type=genai.types.Type.STRING,
                    description="Indicates if specific location or remote status was found ('Specific Location / Remote Status Identified' or 'Not Specified')."
                    # Enum constraint handled by validation/prompt.
                ),
                'remote_status': genai.types.Schema(
                    type=genai.types.Type.STRING,
                    description="Primary work model regarding location ('Fully Remote', 'Remote (Region Specific)', 'Hybrid', 'Office-based', 'Not Specified')."
                    # Enum constraint handled by validation/prompt.
                ),
                'flexibility': genai.types.Schema(
                    type=genai.types.Type.ARRAY,
                    description="List flags indicating schedule flexibility (e.g., ['Flexible Schedule']).",
                    items=genai.types.Schema(type=genai.types.Type.STRING)
                ),
                'locations': genai.types.Schema(
                    type=genai.types.Type.ARRAY,
                    description="Standardized list of mentioned Cities, Countries, or Regions (e.g., ['Lisbon', 'Portugal', 'EMEA', 'Global']). Sorted alphabetically.",
                    items=genai.types.Schema(type=genai.types.Type.STRING)
                )
            }
        ),

        # === Required Qualifications ===
        'required_qualifications': genai.types.Schema(
            type=genai.types.Type.OBJECT,
            description="Mandatory requirements for the role.",
            properties={
                'experience_years_min': genai.types.Schema(
                    type=genai.types.Type.INTEGER,
                    description="Minimum years of experience required."
                ),
                'experience_years_max': genai.types.Schema(
                    type=genai.types.Type.INTEGER,
                    description="Maximum years of experience specified."
                ),
                'experience_description': genai.types.Schema(
                    type=genai.types.Type.STRING,
                    description="Raw text describing the experience requirement."
                ),
                'education_requirements': genai.types.Schema(
                    type=genai.types.Type.STRING,
                    description="Required level of education or field of study text."
                ),
                'technical_skills': genai.types.Schema(
                    type=genai.types.Type.OBJECT,
                    description="Specific technical tools, platforms, languages, and concepts.",
                    properties={
                        'programming_languages': genai.types.Schema(
                            type=genai.types.Type.OBJECT,
                            description="Categorized required programming languages and related libraries/frameworks.",
                            properties={
                                'general_purpose': genai.types.Schema(type=genai.types.Type.ARRAY, items=genai.types.Schema(type=genai.types.Type.STRING), description="List of general-purpose backend/versatile languages."),
                                'scripting_frontend': genai.types.Schema(type=genai.types.Type.ARRAY, items=genai.types.Schema(type=genai.types.Type.STRING), description="List of scripting/frontend languages or frameworks."),
                                'query': genai.types.Schema(type=genai.types.Type.ARRAY, items=genai.types.Schema(type=genai.types.Type.STRING), description="List of query languages."),
                                'data_ml_libs': genai.types.Schema(type=genai.types.Type.ARRAY, items=genai.types.Schema(type=genai.types.Type.STRING), description="List of data/ML libraries or related frameworks."),
                                'platform_runtime': genai.types.Schema(type=genai.types.Type.ARRAY, items=genai.types.Schema(type=genai.types.Type.STRING), description="List of specific platforms/runtimes like .NET."),
                                'configuration': genai.types.Schema(type=genai.types.Type.ARRAY, items=genai.types.Schema(type=genai.types.Type.STRING), description="List of configuration/markup languages."),
                                'other_specialized': genai.types.Schema(type=genai.types.Type.ARRAY, items=genai.types.Schema(type=genai.types.Type.STRING), description="List of other specialized languages.")
                            }
                        ),
                        'cloud_platforms': genai.types.Schema(
                            type=genai.types.Type.ARRAY,
                            description="List of major cloud providers or core data platforms mentioned (e.g., AWS, Azure, GCP, Snowflake, Databricks).",
                            items=genai.types.Schema(type=genai.types.Type.STRING)
                        ),
                        'cloud_services_tools': genai.types.Schema(
                            type=genai.types.Type.ARRAY,
                            description="List of specific cloud services or tools identified (e.g., S3, ADF, Glue, Lambda, GCS, BigQuery).",
                            items=genai.types.Schema(type=genai.types.Type.STRING)
                        ),
                        'database_technologies': genai.types.Schema(
                            type=genai.types.Type.ARRAY,
                            description="List of specific database technologies or general concepts required.",
                            items=genai.types.Schema(type=genai.types.Type.STRING)
                        ),
                        'data_architecture_concepts': genai.types.Schema(
                            type=genai.types.Type.OBJECT,
                            description="Categorized required knowledge of data architecture concepts.",
                            properties={
                                'data_modeling': genai.types.Schema(type=genai.types.Type.ARRAY, items=genai.types.Schema(type=genai.types.Type.STRING), description="Data modeling techniques."),
                                'data_storage_paradigms': genai.types.Schema(type=genai.types.Type.ARRAY, items=genai.types.Schema(type=genai.types.Type.STRING), description="Data storage concepts/systems."),
                                'etl_elt_pipelines': genai.types.Schema(type=genai.types.Type.ARRAY, items=genai.types.Schema(type=genai.types.Type.STRING), description="Data movement/transformation concepts."),
                                'data_governance_quality': genai.types.Schema(type=genai.types.Type.ARRAY, items=genai.types.Schema(type=genai.types.Type.STRING), description="Data governance/quality concepts."),
                                'architecture_patterns': genai.types.Schema(type=genai.types.Type.ARRAY, items=genai.types.Schema(type=genai.types.Type.STRING), description="Data architecture patterns."),
                                'big_data_concepts': genai.types.Schema(type=genai.types.Type.ARRAY, items=genai.types.Schema(type=genai.types.Type.STRING), description="Big Data specific concepts."),
                                'cloud_data_architecture': genai.types.Schema(type=genai.types.Type.ARRAY, items=genai.types.Schema(type=genai.types.Type.STRING), description="Cloud-specific data architecture concepts."),
                                'ml_ai_data_concepts': genai.types.Schema(type=genai.types.Type.ARRAY, items=genai.types.Schema(type=genai.types.Type.STRING), description="ML/AI infrastructure/data concepts."),
                                'core_principles_optimization': genai.types.Schema(type=genai.types.Type.ARRAY, items=genai.types.Schema(type=genai.types.Type.STRING), description="Core design/optimization principles.")
                            }
                        ),
                        'etl_integration_tools': genai.types.Schema(
                            type=genai.types.Type.ARRAY,
                            description="List specific ETL, ELT, or Data Integration tools required.",
                            items=genai.types.Schema(type=genai.types.Type.STRING)
                        ),
                        'data_visualization_bi_tools': genai.types.Schema(
                            type=genai.types.Type.ARRAY,
                            description="List specific Business Intelligence or Data Visualization tools required.",
                            items=genai.types.Schema(type=genai.types.Type.STRING)
                        ),
                        'devops_mlops_ci_cd_tools': genai.types.Schema(
                            type=genai.types.Type.ARRAY,
                            description="List specific DevOps, MLOps, CI/CD, IaC, or Monitoring tools required.",
                            items=genai.types.Schema(type=genai.types.Type.STRING)
                        ),
                        'orchestration_workflow_tools': genai.types.Schema(
                            type=genai.types.Type.ARRAY,
                            description="List specific workflow orchestration tools required.",
                            items=genai.types.Schema(type=genai.types.Type.STRING)
                        ),
                        'other_tools': genai.types.Schema(
                            type=genai.types.Type.ARRAY,
                            description="List other relevant tools not fitting neatly into the above categories (e.g., IDEs, Data Catalogs, Vector DBs).",
                            items=genai.types.Schema(type=genai.types.Type.STRING)
                        )
                    }
                ),
                'methodologies_practices': genai.types.Schema(
                    type=genai.types.Type.ARRAY,
                    description="List required development methodologies or practices (e.g., 'Agile Principles', 'Scrum', 'TDD').",
                    items=genai.types.Schema(type=genai.types.Type.STRING)
                ),
                'soft_skills_keywords': genai.types.Schema(
                    type=genai.types.Type.ARRAY,
                    description="List required soft skills or general keywords (e.g., 'Communication', 'Teamwork', 'English').",
                    items=genai.types.Schema(type=genai.types.Type.STRING)
                )
            }
        ),

        # === Preferred Qualifications ===
        'preferred_qualifications': genai.types.Schema(
            type=genai.types.Type.OBJECT,
            description="Nice-to-have skills and qualifications.",
            properties={
                'skills_keywords': genai.types.Schema(
                    type=genai.types.Type.ARRAY,
                    description="List of preferred skills, tools, languages, or concepts (e.g., 'French', 'Certifications').",
                    items=genai.types.Schema(type=genai.types.Type.STRING)
                ),
                'other_notes': genai.types.Schema(
                    type=genai.types.Type.STRING,
                    description="Any other text describing preferred qualifications."
                )
            }
        ),

        # === Role Context ===
        'role_context': genai.types.Schema(
            type=genai.types.Type.OBJECT,
            description="Information about the role's interactions and scope.",
            properties={
                'collaboration_with': genai.types.Schema(
                    type=genai.types.Type.ARRAY,
                    description="List of teams or roles this position collaborates with (e.g., 'Stakeholders', 'Data Scientists').",
                    items=genai.types.Schema(type=genai.types.Type.STRING)
                ),
                'team_structure': genai.types.Schema(
                    type=genai.types.Type.STRING,
                    description="Description of the team structure or context."
                ),
                'project_scope': genai.types.Schema(
                    type=genai.types.Type.STRING,
                    description="Description of the type or scope of projects involved."
                ),
                'key_responsibilities': genai.types.Schema(
                    type=genai.types.Type.ARRAY,
                    description="List of key tasks and responsibilities mentioned.",
                    items=genai.types.Schema(type=genai.types.Type.STRING)
                )
            }
        ),

        # === Benefits ===
        'benefits': genai.types.Schema(
            type=genai.types.Type.OBJECT,
            description="Perks and benefits offered.",
            properties={
                'training_development': genai.types.Schema(
                    type=genai.types.Type.STRING,
                    description="Description of training and development opportunities."
                ),
                'learning_platforms': genai.types.Schema(
                    type=genai.types.Type.ARRAY,
                    description="List specific learning platforms mentioned by name (e.g., 'Udemy', 'LinkedIn Learning').",
                    items=genai.types.Schema(type=genai.types.Type.STRING)
                ),
                'paid_time_off_days': genai.types.Schema(
                    type=genai.types.Type.INTEGER,
                    description="Specific number of paid time off days mentioned."
                ),
                'other_benefits_keywords': genai.types.Schema(
                    type=genai.types.Type.ARRAY,
                    description="List keywords for other benefits (e.g., 'Health Insurance', 'Meal Allowance', 'Well-being Program').",
                    items=genai.types.Schema(type=genai.types.Type.STRING)
                )
            }
        )
    }
    # Note: genai.types.Schema does not have a direct equivalent for JSON Schema's top-level "required" list.
    # Optionality is generally handled by checking if a field exists in the response or is null.
)

# You can now use JOB_RESPONSE_SCHEMA with the Google Generative AI client library
# print(JOB_RESPONSE_SCHEMA)
def process_job_description(client: object, job_desc_text: str):
    """
    Process a single job description using a provided Gemini model instance
    and return structured JSON data.

    Args:
        model: An initialized google.generativeai.GenerativeModel instance.
        job_desc_text: The raw text of the job description.

    Returns:
        A dictionary containing the structured data, or None if processing fails.
    """
    if not job_desc_text or not job_desc_text.strip():
        tqdm.write("  Warning: Received empty or whitespace-only job description.")
        return None

    response = None # Define response variable outside try for potential use in except block
    accumulated_response = ''
    try:
        # Define contents for the API call
        full_prompt = JOB_DESCRIPTION_PROMPT + job_desc_text
        contents = [ full_prompt ]

        # Define Generation Config (ensure JOB_RESPONSE_SCHEMA is defined)
        generation_config = types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=JOB_RESPONSE_SCHEMA
        )

        # Make the API call using the provided model instance
        # request_options = types.RequestOptions(timeout=120) # Optional: 120 seconds timeout

        # Make the API call with streaming
        stream = client.models.generate_content_stream(
            model="gemini-2.0-flash",
            contents=contents,
            config=generation_config,
        )

        # Process the streaming response
        for chunk in stream:
            if chunk.text:
                accumulated_response += chunk.text
                # Optional: Print progress
                # tqdm.write(".", end="", flush=True)
        
        # Parse the completed JSON response
        if accumulated_response:
            try:
                parsed_data = json.loads(accumulated_response)
                return parsed_data
            except json.JSONDecodeError as json_err:
                tqdm.write(f" Error: Failed to parse complete response as JSON: {json_err}")
                tqdm.write(f" Raw response (start): {accumulated_response[:500]}...")
                return None
        else:
            tqdm.write(" Warning: Received empty response from the API")
            return None

    # --- Simplified Error Handling ---
    except json.JSONDecodeError as json_err:
        tqdm.write(f"  Error: Failed to parse response as JSON: {json_err}")
        raw_text = "N/A"
        try:
            # Try to get raw text if response object exists
            if response and response.candidates and response.candidates[0].content.parts:
                raw_text = response.candidates[0].content.parts[0].text[:500] + "..."
        except Exception:
            pass # Ignore errors trying to get raw text
        tqdm.write(f"  Raw response (start): {raw_text}")
        return None
    except google_exceptions.GoogleAPIError as api_err:
        # Catch broad Google API errors (includes ResourceExhausted, InvalidArgument, PermissionDenied, etc.)
        tqdm.write(f"  Error: Google API Error ({type(api_err).__name__}): {api_err}")
        if "token limit" in str(api_err).lower() and len(job_desc_text) > 10000: # Heuristic
             tqdm.write(f"  Input job description might be too long ({len(job_desc_text)} chars).")
        elif "API key" in str(api_err):
             tqdm.write(f"  Check API key configuration and permissions.")
        return None

