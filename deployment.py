# deployment.py

from src.canvas.flow import canvas_data_flow, canvas_hourly_flow
from src.registrar.flow import registrar_flow
from src.starfish.exporter import exporter_flow
from src.starfish.flow import starfish_flow
from prefect.deployments import Deployment
from prefect.client.schemas.schedules import RRuleSchedule
from prefect.filesystems import LocalFileSystem
from prefect.infrastructure import Process


local_storage = LocalFileSystem(
    basepath="F:\Applications\psc-dataflow\data\storage"
    )
local_storage.save(
    "psc-dataflow-local-storage-block", 
#     overwrite=True
    )

process_infrastructure = Process(
    # working_dir="F:\Applications\psc-dataflow\data\work"
    working_dir="F:\Applications\psc-dataflow"
    )
process_infrastructure.save(
    "psc-dataflow-process-infrastructure", 
#     overwrite=True
    )

canvas_data_deployment = Deployment.build_from_flow(
    flow=canvas_data_flow,
    name="canvas-data-deployment",
    tags=['canvas', 'canvas_data'],
    parameters={},
    infrastructure=process_infrastructure,
    infra_overrides={
        "env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}
        },
    work_queue_name="production",
    work_pool_name="psc-data-pool",
    skip_upload=True,
    # storage=local_storage,
    schedule=(RRuleSchedule(rrule="DTSTART:20240221T041500\nFREQ=HOURLY;INTERVAL=8", timezone="America/New_York")
        ),
)

canvas_hourly_deployment = Deployment.build_from_flow(
    flow=canvas_hourly_flow,
    name="canvas-hourly-deployment",
    tags=['canvas', 'canvas_hourly'],
    parameters={},
    infrastructure=process_infrastructure,
    infra_overrides={
        "env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}
        },
    work_queue_name="production",
    work_pool_name="psc-data-pool",
    skip_upload=True,
    # storage=local_storage,
    schedule=(RRuleSchedule(rrule="DTSTART:20240221T043000\nFREQ=HOURLY;INTERVAL=1", timezone="America/New_York")
        ),
)

exporter_deployment = Deployment.build_from_flow(
    flow=exporter_flow,
    name="exporter-deployment",
    tags=['exporter', 'starfish'],
    parameters={},
    infrastructure=process_infrastructure,
    infra_overrides={
        "env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}
        },
    work_queue_name="production",
    work_pool_name="psc-data-pool",
    skip_upload=True,
    # storage=local_storage,
    schedule=(RRuleSchedule(rrule="DTSTART:20240221T041500\nFREQ=DAILY;INTERVAL=1", timezone="America/New_York")
        ),
)

registrar_deployment = Deployment.build_from_flow(
    flow=registrar_flow,
    name="registrar-deployment",
    tags=['registrar'],
    parameters={},
    infrastructure=process_infrastructure,
    infra_overrides={
        "env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}
        },
    work_queue_name="production",
    work_pool_name="psc-data-pool",
    skip_upload=True,
    # storage=local_storage,
    schedule=(RRuleSchedule(rrule="DTSTART:20240221T170000\nFREQ=DAILY;INTERVAL=1", timezone="America/New_York")
        ),
)

starfish_deployment = Deployment.build_from_flow(
    flow=starfish_flow,
    name="starfish-deployment",
    tags=['starfish'],
    parameters={"academic_year": "2024",
                "academic_term": "SPRING"},
    infrastructure=process_infrastructure,
    infra_overrides={
        "env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}
        },
    work_queue_name="production",
    work_pool_name="psc-data-pool",
    skip_upload=True,
    # storage=local_storage,
    schedule=(RRuleSchedule(rrule="DTSTART:20240221T193000\nFREQ=DAILY;INTERVAL=1", timezone="America/New_York")
        ),
)


if __name__ == "__main__":
    canvas_data_deployment.apply()
    canvas_hourly_deployment.apply()
    exporter_deployment.apply()
    registrar_deployment.apply()    
    starfish_deployment.apply()
