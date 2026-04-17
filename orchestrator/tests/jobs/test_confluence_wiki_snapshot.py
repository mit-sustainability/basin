from orchestrator.jobs.confluence_wiki_snapshot import confluence_wiki_snapshot_job


def test_confluence_wiki_snapshot_job_exists():
    assert confluence_wiki_snapshot_job is not None
