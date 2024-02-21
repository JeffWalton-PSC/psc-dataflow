@echo on
echo "***** batch file running" >> F:\Applications\psc-dataflow\logs\server_out.log
date /T >> F:\Applications\psc-dataflow\logs\server_out.log
time /T >> F:\Applications\psc-dataflow\logs\server_out.log

whoami 1>> F:\Applications\psc-dataflow\logs\server_out.log 2>&1
echo "whoami" 1>> F:\Applications\psc-dataflow\logs\server_out.log 2>&1

F:
echo "F:" 1>> F:\Applications\psc-dataflow\logs\server_out.log 2>&1
cd F:\Applications\psc-dataflow
echo "cd F:\Applications\psc-dataflow" 1>> F:\Applications\psc-dataflow\logs\server_out.log 2>&1

echo "Set environment variables" 1>> F:\Applications\psc-dataflow\logs\server_out.log 2>&1
set "PREFECT_HOME=F:\Applications\psc-dataflow\data\.prefect"

call C:\ProgramData\Anaconda3\condabin\activate.bat F:\Applications\psc-dataflow\envs\py311prefect 1>> F:\Applications\psc-dataflow\logs\server_out.log 2>&1
echo "call activate" 1>> F:\Applications\psc-dataflow\logs\server_out.log 2>&1
call conda list 1>> F:\Applications\psc-dataflow\logs\server_out.log 2>&1

echo "prefect server start " >> F:\Applications\psc-dataflow\logs\server_out.log
prefect server start 1>> F:\Applications\psc-dataflow\logs\server_out.log 2>&1

rem pause
time /T >> F:\Applications\psc-dataflow\logs\server_out.log
echo "***** batch file exiting" >> F:\Applications\psc-dataflow\logs\server_out.log
echo "****************************************************************" >> F:\Applications\psc-dataflow\logs\server_out.log
