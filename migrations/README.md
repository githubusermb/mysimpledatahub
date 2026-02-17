# Migrations and Change History

This folder contains documentation about migrations, bugfixes, and changes made to the Simple Data Hub project over time.

## Contents

### Migration Documents
- **MIGRATION-SUMMARY.md** - Summary of table and view naming convention changes
- **MIGRATION-JOBS-FOLDER.md** - Documentation of moving Glue jobs to dedicated folder
- **RENAME-BUCKET-DATABASE.md** - Guide for renaming S3 bucket and Glue database
- **UPDATES-REGION-AND-VIEWS.md** - Documentation of region externalization and view updates

### Bugfix Documents
- **BUGFIX-INGEST-TIMESTAMP.md** - Fix for ingest_timestamp showing "unknown"
- **BUGFIX-VIEW-PREFIX-ARGUMENT.md** - Fix for GlueArgumentError with view_prefix parameter

### Solution Documents
- **DUAL-ENGINE-SOLUTION.md** - Documentation of dual-engine (Spark + Athena) view solution
- **FOLDER-STRUCTURE.md** - Project folder structure documentation

### Other
- **prompt.md** - Original project prompt and requirements

## Purpose

These documents are kept for:
- Historical reference
- Understanding why certain decisions were made
- Troubleshooting similar issues in the future
- Onboarding new team members

## Current Documentation

For current project documentation, see the root folder:
- **README.md** - Main project documentation
- **QUICK-START.md** - Quick start guide
- **ARCHITECTURE.md** - System architecture overview
- **ENTITY-DIAGRAMS.md** - Data model and entity diagrams

For technical documentation, see:
- **docs/** - Detailed technical guides
- **jobs/README.md** - Glue jobs documentation
