# Project Folder Structure

## Overview

This document describes the organization of the mysimpledatahub project.

---

## Directory Layout

```
mysimpledatahub/
│
├── jobs/                                    # AWS Glue ETL Jobs
│   ├── README.md                            # Job documentation
│   ├── glue_csv_to_iceberg.py              # Data ingestion job
│   └── glue_create_normal_views.py    # View creation job
│
├── scripts/                                 # Helper & Utility Scripts
│   ├── generate_sample_csv.py              # Generate test data
│   ├── generate_sample_csv_mdrm.py         # Generate MDRM test data
│   ├── setup_lakeformation_complete.py     # Lake Formation setup
│   ├── grant_athena_user_permissions.py    # Grant permissions
│   ├── register_table_with_lakeformation.py # Register tables
│   ├── lambda_lakeformation_setup.py       # Lambda setup
│   ├── test_views_in_spark.py              # Test views
│   ├── diagnose_view.ps1                   # Diagnose views
│   ├── drop_all_views.ps1                  # Drop all views
│   ├── drop_all_views.sh                   # Drop all views (bash)
│   ├── recreate_views.bat                  # Recreate views (Windows)
│   ├── recreate_views.sh                   # Recreate views (bash)
│   ├── test_views.bat                      # Test views (Windows)
│   ├── test_views.sh                       # Test views (bash)
│   ├── upload_and_process.sh               # Upload and process
│   ├── test_pipeline_locally.sh            # Test pipeline
│   └── verify_view_format.ps1              # Verify view format
│
├── terraform/                               # Infrastructure as Code
│   ├── main.tf                             # Main configuration
│   ├── variables.tf                        # Variable definitions
│   ├── outputs.tf                          # Output values
│   ├── terraform.tfvars                    # Configuration values
│   ├── lakeformation.tf                    # Lake Formation setup
│   ├── views_dual_engine_job.tf            # Views job configuration
│   ├── lambda_function.zip                 # Lambda deployment package
│   ├── terraform.tfstate                   # Terraform state
│   ├── terraform.tfstate.backup            # State backup
│   └── .terraform/                         # Terraform plugins
│
├── docs/                                    # Documentation
│   ├── naming-conventions-best-practices.md # Naming guidelines
│   ├── materialized-views-vs-iceberg-tables.md # View comparison
│   ├── lake-formation-permissions.md       # Lake Formation guide
│   ├── troubleshooting-lakeformation.md    # Troubleshooting
│   ├── athena-permissions-setup.md         # Athena setup
│   ├── automatic-lakeformation-setup.md    # Auto setup
│   ├── fix-multi-dialect-view-error.md     # View fixes
│   ├── querying-views-in-athena.md         # Query guide
│   ├── running-scripts-in-aws.md           # AWS scripts
│   ├── testing-views-in-spark.md           # Spark testing
│   └── verifying-views-in-catalog.md       # Catalog verification
│
├── data/                                    # Sample Data
│   └── ingest_ts=1770609249/
│       └── mdrm_data_1770609249.csv        # Sample MDRM data
│
├── mdrm/                                    # MDRM Output Files
│   ├── output_fry15.txt                    # FRY-15 output
│   └── output_fry9c.txt                    # FRY-9C output
│
├── ARCHITECTURE.md                          # System architecture
├── ENTITY-DIAGRAMS.md                       # Entity diagrams
├── README.md                                # Main documentation
├── QUICK-START.md                           # Quick start guide
├── normal-SOLUTION.md                  # normal views
├── MIGRATION-SUMMARY.md                     # Naming migration
├── MIGRATION-JOBS-FOLDER.md                 # Jobs folder migration
├── UPDATES-REGION-AND-VIEWS.md              # Recent updates
├── BUGFIX-INGEST-TIMESTAMP.md               # Timestamp fix
├── BUGFIX-VIEW-PREFIX-ARGUMENT.md           # View prefix fix
├── FOLDER-STRUCTURE.md                      # This file
├── prompt.md                                # Original prompt
└── .gitignore                               # Git ignore rules
```

---

## Folder Purposes

### 📁 jobs/

**Purpose**: AWS Glue ETL job scripts

**Contents**: Python scripts that run in AWS Glue environment

**Deployment**: Uploaded to S3 via Terraform, executed by Glue

**Key Files**:
- `glue_csv_to_iceberg.py` - Ingests CSV data into Iceberg tables
- `glue_create_normal_views.py` - Creates multi-dialect views

**When to Add Files Here**:
- New Glue ETL jobs
- Jobs that process data in Glue Spark
- Jobs that use awsglue libraries

### 📁 scripts/

**Purpose**: Helper and utility scripts

**Contents**: Scripts for setup, testing, and maintenance

**Deployment**: Run locally or in Lambda, not deployed to Glue

**Key Files**:
- Setup scripts (Lake Formation, permissions)
- Test scripts (view testing, pipeline testing)
- Data generation scripts
- Diagnostic scripts

**When to Add Files Here**:
- Local utility scripts
- Setup/configuration scripts
- Testing scripts
- Lambda functions
- Shell scripts for automation

### 📁 terraform/

**Purpose**: Infrastructure as Code

**Contents**: Terraform configuration files

**Deployment**: Applied via `terraform apply`

**Key Files**:
- `main.tf` - Main infrastructure (S3, Glue, IAM)
- `lakeformation.tf` - Lake Formation configuration
- `views_dual_engine_job.tf` - Views job configuration
- `variables.tf` - Variable definitions
- `terraform.tfvars` - Configuration values

**When to Add Files Here**:
- New AWS resources
- Infrastructure changes
- Job configurations
- IAM policies

### 📁 docs/

**Purpose**: Documentation and guides

**Contents**: Markdown documentation files

**Key Files**:
- Best practices guides
- Troubleshooting guides
- Setup instructions
- Comparison documents

**When to Add Files Here**:
- New documentation
- How-to guides
- Architecture decisions
- Troubleshooting guides

### 📁 data/

**Purpose**: Sample and test data

**Contents**: CSV files for testing

**When to Add Files Here**:
- Sample datasets
- Test data
- Reference data

### 📁 mdrm/

**Purpose**: MDRM-specific output files

**Contents**: Output files from MDRM processing

**When to Add Files Here**:
- MDRM output files
- Series-specific data

---

## File Naming Conventions

### Python Scripts

```
Pattern: <purpose>_<action>.py

Examples:
✅ glue_csv_to_iceberg.py
✅ generate_sample_csv.py
✅ setup_lakeformation_complete.py
✅ grant_athena_user_permissions.py

❌ script1.py
❌ MyScript.py
❌ test-script.py
```

### Shell Scripts

```
Pattern: <action>_<target>.<sh|bat|ps1>

Examples:
✅ drop_all_views.sh
✅ recreate_views.bat
✅ test_pipeline_locally.sh
✅ diagnose_view.ps1

❌ script.sh
❌ run.bat
```

### Documentation

```
Pattern: <TOPIC>-<SUBTOPIC>.md or <TOPIC>.md

Examples:
✅ ARCHITECTURE.md
✅ naming-conventions-best-practices.md
✅ materialized-views-vs-iceberg-tables.md
✅ BUGFIX-INGEST-TIMESTAMP.md

❌ doc1.md
❌ readme.txt
```

### Terraform Files

```
Pattern: <resource_type>.tf or main.tf

Examples:
✅ main.tf
✅ variables.tf
✅ outputs.tf
✅ lakeformation.tf
✅ views_dual_engine_job.tf

❌ config.tf
❌ terraform.tf
```

---

## Adding New Files

### Adding a New Glue Job

1. Create script in `jobs/` folder:
   ```bash
   touch jobs/glue_new_job.py
   ```

2. Add Terraform configuration:
   ```hcl
   # In terraform/new_job.tf
   resource "aws_s3_object" "new_job_script" {
     bucket = aws_s3_bucket.iceberg_data_bucket.id
     key    = "scripts/glue_new_job.py"
     source = "../jobs/glue_new_job.py"
     etag   = filemd5("../jobs/glue_new_job.py")
   }
   
   resource "aws_glue_job" "new_job" {
     name     = "new-job-name"
     role_arn = aws_iam_role.glue_service_role.arn
     ...
   }
   ```

3. Update `jobs/README.md` with job documentation

4. Deploy:
   ```bash
   cd terraform
   terraform apply
   ```

### Adding a New Helper Script

1. Create script in `scripts/` folder:
   ```bash
   touch scripts/new_helper_script.py
   ```

2. Add documentation comment at top of file

3. Update this file (`FOLDER-STRUCTURE.md`) if significant

4. No deployment needed (runs locally)

### Adding New Documentation

1. Create markdown file in `docs/` folder:
   ```bash
   touch docs/new-guide.md
   ```

2. Follow markdown best practices

3. Link from main `README.md` if appropriate

---

## Best Practices

### 1. Keep Jobs and Scripts Separate

```
✅ DO:
jobs/       → Glue ETL jobs only
scripts/    → Helper scripts only

❌ DON'T:
scripts/    → Mix Glue jobs and helper scripts
```

### 2. Document Everything

```
✅ DO:
- Add README to each major folder
- Document job parameters
- Include usage examples

❌ DON'T:
- Leave undocumented scripts
- Skip parameter descriptions
```

### 3. Use Consistent Naming

```
✅ DO:
- Follow naming conventions
- Use descriptive names
- Be consistent across files

❌ DON'T:
- Use generic names (script1.py)
- Mix naming styles
- Use abbreviations
```

### 4. Organize by Purpose

```
✅ DO:
- Group related files
- Separate concerns
- Clear folder purposes

❌ DON'T:
- Put everything in root
- Mix unrelated files
- Create deep nesting
```

---

## Quick Reference

### Where Does This Go?

| File Type | Folder | Example |
|-----------|--------|---------|
| Glue ETL job | `jobs/` | `glue_csv_to_iceberg.py` |
| Helper script | `scripts/` | `generate_sample_csv.py` |
| Terraform config | `terraform/` | `main.tf` |
| Documentation | `docs/` | `naming-conventions.md` |
| Sample data | `data/` | `sample.csv` |
| Root-level docs | `.` (root) | `ARCHITECTURE.md` |

### Common Tasks

| Task | Command |
|------|---------|
| Deploy infrastructure | `cd terraform && terraform apply` |
| Run ingestion job | `aws glue start-job-run --job-name csv-to-iceberg-ingestion` |
| Run views job | `aws glue start-job-run --job-name create-views-normal` |
| Generate sample data | `python scripts/generate_sample_csv_mdrm.py` |
| Setup Lake Formation | `python scripts/setup_lakeformation_complete.py` |
| Test views | `python scripts/test_views_in_spark.py` |

---

## Maintenance

### Regular Updates

- ✅ Keep documentation in sync with code
- ✅ Update README files when adding new files
- ✅ Review and clean up unused files
- ✅ Update this structure document as needed

### Version Control

- ✅ Commit related changes together
- ✅ Use meaningful commit messages
- ✅ Tag releases
- ✅ Keep .gitignore updated

---

**Last Updated**: February 11, 2026  
**Status**: Current ✅
