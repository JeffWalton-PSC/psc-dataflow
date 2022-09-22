# deployment.py

from src.registrar.flow import registrar_flow
from src.starfish.flow import starfish_flow
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import RRuleSchedule


registrar_deployment = Deployment.build_from_flow(
    flow=registrar_flow,
    name="registrar",
    parameters={"academic_year": "2018",
                "academic_term": "FALL"},
    infra_overrides={
        "env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}
        },
    work_queue_name="production",
    schedule=(RRuleSchedule(rrule="DTSTART:20220921T190000\nFREQ=DAILY;INTERVAL=1", timezone="America/New_York")
        ),
)

starfish_deployment = Deployment.build_from_flow(
    flow=starfish_flow,
    name="starfish",
    parameters={"academic_year": "2018",
                "academic_term": "FALL"},
    infra_overrides={
        "env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}
        },
    work_queue_name="production",
    schedule=(RRuleSchedule(rrule="DTSTART:20220921T193000\nFREQ=DAILY;INTERVAL=1", timezone="America/New_York")
        ),
)


if __name__ == "__main__":
    registrar_deployment.apply()    
    starfish_deployment.apply()
