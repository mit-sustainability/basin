from orchestrator.jobs.website_content_health import website_content_health_link_check_job
from orchestrator.schedules.mitos_warehouse import website_content_health_schedule


def test_website_content_health_schedule_exists():
    assert website_content_health_schedule is not None


def test_website_content_health_link_check_job_exists():
    assert website_content_health_link_check_job is not None
