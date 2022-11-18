@echo on
echo "***** batch file running" >> F:\Applications\psc-dataflow\logs\out.log
time /T >> F:\Applications\psc-dataflow\logs\out.log

whoami 1>> F:\Applications\psc-dataflow\logs\out.log 2>&1
echo "whoami" 1>> F:\Applications\psc-dataflow\logs\out.log 2>&1

F:
echo "F:" 1>> F:\Applications\psc-dataflow\logs\out.log 2>&1
cd F:\Applications\psc-dataflow
echo "cd F:\Applications\psc-dataflow" 1>> F:\Applications\psc-dataflow\logs\out.log 2>&1

set PREFECT_ORION_API_HOST=psc-data

call C:\ProgramData\Anaconda3\condabin\activate.bat py310prefect 1>> F:\Applications\psc-dataflow\logs\out.log 2>&1
echo "call activate" 1>> F:\Applications\psc-dataflow\logs\out.log 2>&1
rem call conda list 1>> F:\Applications\psc-dataflow\logs\out.log 2>&1

echo "prefect orion start " >> F:\Applications\psc-dataflow\logs\out.log
prefect orion start 1>> F:\Applications\psc-dataflow\logs\out.log 2>&1

rem pause
time /T >> F:\Applications\psc-dataflow\logs\out.log
echo "***** batch file exiting" >> F:\Applications\psc-dataflow\logs\out.log
echo "****************************************************************" >> F:\Applications\psc-dataflow\logs\out.log
