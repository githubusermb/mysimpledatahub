# Quick Fix: EntityAlreadyExists Errors

## The Error

```
Error: EntityAlreadyExists: Role with name GlueServiceRole already exists.
Error: EntityAlreadyExists: A policy called GlueS3Access already exists.
```

## The Fix (2 Steps)

### Step 1: Import Existing Resources

**Windows:**
```powershell
cd mysimpledatahub\terraform
.\import_existing_resources.ps1
```

**Linux/Mac:**
```bash
cd mysimpledatahub/terraform
chmod +x import_existing_resources.sh
./import_existing_resources.sh
```

### Step 2: Apply Terraform

```bash
terraform plan    # Review changes
terraform apply   # Apply changes
```

## What This Does

The import script:
1. Finds existing IAM roles and policies in your AWS account
2. Imports them into Terraform state
3. Allows Terraform to manage them going forward

## If Import Fails

See `IMPORT-EXISTING-RESOURCES.md` for:
- Manual import steps
- Troubleshooting guide
- Alternative solutions

## Need Help?

Check the detailed guide:
```bash
cat IMPORT-EXISTING-RESOURCES.md
```
