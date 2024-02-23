# flow_deploy.py

from src.canvas.flow import canvas_data_flow, canvas_hourly_flow
from src.registrar.flow import registrar_flow
from src.starfish.exporter import exporter_flow
from src.starfish.flow import starfish_flow
from prefect import serve, deploy
# from prefect.filesystems import LocalFileSystem
# from prefect.infrastructure import Process


canvas_data_deployment = canvas_data_flow.to_deployment(
    name="canvas-data-deployment",
    tags=['canvas', 'canvas_data'],
    parameters={},
    work_queue_name="production",
    work_pool_name="psc-data-pool",
    rrule="DTSTART:20240222T041500\nFREQ=HOURLY;INTERVAL=8",
    # timezone="America/New_York",
)

canvas_hourly_deployment = canvas_hourly_flow.to_deployment(
    name="canvas-hourly-deployment",
    tags=['canvas', 'canvas_hourly'],
    parameters={},
    work_queue_name="production",
    work_pool_name="psc-data-pool",
    rrule="DTSTART:20240222T043000\nFREQ=HOURLY;INTERVAL=1", 
    # timezone="America/New_York",
)

exporter_deployment = exporter_flow.to_deployment(
    name="exporter-deployment",
    tags=['exporter', 'starfish'],
    parameters={},
    work_queue_name="production",
    work_pool_name="psc-data-pool",
    rrule="DTSTART:20240222T041500\nFREQ=DAILY;INTERVAL=1", 
    # timezone="America/New_York",
)

registrar_deployment = registrar_flow.to_deployment(
    name="registrar-deployment",
    tags=['registrar'],
    parameters={},
    work_queue_name="production",
    work_pool_name="psc-data-pool",
    rrule="DTSTART:20240222T170000\nFREQ=DAILY;INTERVAL=1", 
    # timezone="America/New_York",
)

starfish_deployment = starfish_flow.to_deployment(
    name="starfish-deployment",
    tags=['starfish'],
    parameters={"academic_year": "2024",
                "academic_term": "SPRING"},
    work_queue_name="production",
    work_pool_name="psc-data-pool",
    rrule="DTSTART:20240222T193000\nFREQ=DAILY;INTERVAL=1", 
    # timezone="America/New_York",
)


if __name__ == "__main__":
    print("before serve()")
    serve(canvas_data_deployment, canvas_hourly_deployment, exporter_deployment, registrar_deployment, starfish_deployment)
    print("after serve()")
    
