@echo on
echo "***** batch file running" >> F:\Applications\psc-dataflow\logs\orion_out.log
date /T >> F:\Applications\psc-dataflow\logs\orion_out.log
time /T >> F:\Applications\psc-dataflow\logs\orion_out.log

whoami 1>> F:\Applications\psc-dataflow\logs\orion_out.log 2>&1
echo "whoami" 1>> F:\Applications\psc-dataflow\logs\orion_out.log 2>&1

F:
echo "F:" 1>> F:\Applications\psc-dataflow\logs\orion_out.log 2>&1
cd F:\Applications\psc-dataflow
echo "cd F:\Applications\psc-dataflow" 1>> F:\Applications\psc-dataflow\logs\orion_out.log 2>&1

echo "Set environment variables" 1>> F:\Applications\psc-dataflow\logs\orion_out.log 2>&1
set "PREFECT_API_URL=http://psc-data:4200/api"
set "PREFECT_HOME=F:\Applications\psc-dataflow\data\.prefect"
set "PREFECT_LOCAL_STORAGE_PATH=F:\Applications\psc-dataflow\data\storage"
set "PREFECT_LOGGING_LEVEL=DEBUG"
set "PREFECT_ORION_API_HOST=psc-data"
set "PREFECT_ORION_DATABASE_CONNECTION_URL=sqlite+aiosqlite:////Applications/psc-dataflow/data/orion.db"

call C:\ProgramData\Anaconda3\condabin\activate.bat py310prefect 1>> F:\Applications\psc-dataflow\logs\orion_out.log 2>&1
echo "call activate" 1>> F:\Applications\psc-dataflow\logs\orion_out.log 2>&1
rem call conda list 1>> F:\Applications\psc-dataflow\logs\orion_out.log 2>&1

echo "prefect orion start " >> F:\Applications\psc-dataflow\logs\orion_out.log
prefect orion start 1>> F:\Applications\psc-dataflow\logs\orion_out.log 2>&1

rem pause
time /T >> F:\Applications\psc-dataflow\logs\orion_out.log
echo "***** batch file exiting" >> F:\Applications\psc-dataflow\logs\orion_out.log
echo "****************************************************************" >> F:\Applications\psc-dataflow\logs\orion_out.log
