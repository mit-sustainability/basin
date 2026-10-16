"""Reviewed knowledge-source configuration. Credentials stay in the environment."""

import os

S3_BUCKET = os.getenv("KNOWLEDGE_S3_BUCKET", "mitos-resources")
S3_PREFIX = os.getenv("KNOWLEDGE_S3_PREFIX", "knowledgebase").strip("/")

MKDOCS_REPO_URL = os.getenv("KNOWLEDGE_MKDOCS_REPO_URL", "https://github.com/mit-sustainability/portfolio")
MKDOCS_REF = os.getenv("KNOWLEDGE_MKDOCS_REF", "main")
MKDOCS_SITE_URL = os.getenv("KNOWLEDGE_MKDOCS_SITE_URL", "https://mit-sustainability.github.io/portfolio/")

# Contribution Guide, Gen AI Tools, Tableau. Each root is exported with its descendants.
CONFLUENCE_ROOT_PAGE_IDS = ("319732366", "319723988", "319724724")
