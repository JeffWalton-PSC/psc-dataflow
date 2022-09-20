# deployment.py

from starfish_flow import starfish_flow
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import RRuleSchedule


deployment = Deployment.build_from_flow(
    flow=starfish_flow,
    name="starfish",
    parameters={"academic_year": "2018",
                "academic_term": "FALL"},
    infra_overrides={
        "env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}
        },
    work_queue_name="production",
    schedule=(RRuleSchedule(rrule="DTSTART:20220919T150000\nFREQ=HOURLY;INTERVAL=1", timezone="America/New_York")
        ),
)


if __name__ == "__main__":
    deployment.apply()