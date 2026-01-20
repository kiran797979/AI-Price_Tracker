---
title: "🚨 Automated Price Check Failed"
labels: ["bug", "automated"]
---

## Workflow Run Failed

The automated price checking workflow has failed.

**Run ID**: {{ env.GITHUB_RUN_ID }}
**Run URL**: {{ env.GITHUB_SERVER_URL }}/{{ env.GITHUB_REPOSITORY }}/actions/runs/{{ env.GITHUB_RUN_ID }}
**Triggered By**: {{ env.GITHUB_ACTOR }}
**Timestamp**: {{ date | date('YYYY-MM-DD HH:mm:ss') }}

## Logs

Please check the workflow logs for details about the failure.

## Common Issues
- Missing or invalid environment variables
- Database connection problems
- Network/API rate limiting

## Action Required
Please investigate the workflow logs and fix the underlying issue.
