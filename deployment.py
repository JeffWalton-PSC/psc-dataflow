# deployment.py

from src.registrar.flow import registrar_flow
from src.starfish.exporter import exporter_flow
from src.starfish.flow import starfish_flow
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import RRuleSchedule
from prefect.filesystems import LocalFileSystem
# from prefect.infrastructure import Process


local_storage = LocalFileSystem(
    basepath="F:\Applications\psc-dataflow\data\storage"
    )
local_storage.save(
    "psc-dataflow-local-storage-block", 
#     overwrite=True
    )

# process_infrastructure = Process(
#     working_dir="F:\Applications\psc-dataflow\data\work"
#     )
# process_infrastructure.save(
#     "psc-dataflow-process-infrastructure", 
#     overwrite=True
#     )

exporter_deployment = Deployment.build_from_flow(
    flow=exporter_flow,
    name="exporter-deployment",
    parameters={},
    infra_overrides={
        "env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}
        },
    work_queue_name="production",
    storage=local_storage,
    schedule=(RRuleSchedule(rrule="DTSTART:20221120T041500\nFREQ=DAILY;INTERVAL=1", timezone="America/New_York")
        ),
)

registrar_deployment = Deployment.build_from_flow(
    flow=registrar_flow,
    name="registrar-deployment",
    parameters={},
    infra_overrides={
        "env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}
        },
    work_queue_name="production",
    storage=local_storage,
    schedule=(RRuleSchedule(rrule="DTSTART:20221109T170000\nFREQ=DAILY;INTERVAL=1", timezone="America/New_York")
        ),
)

starfish_deployment = Deployment.build_from_flow(
    flow=starfish_flow,
    name="starfish-deployment",
    parameters={"academic_year": "2022",
                "academic_term": "FALL"},
    infra_overrides={
        "env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}
        },
    work_queue_name="production",
    storage=local_storage,
    schedule=(RRuleSchedule(rrule="DTSTART:20221109T193000\nFREQ=DAILY;INTERVAL=1", timezone="America/New_York")
        ),
)


if __name__ == "__main__":
    exporter_deployment.apply()
    registrar_deployment.apply()    
    starfish_deployment.apply()
