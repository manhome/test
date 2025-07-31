@echo off
SET arg1=%1
SET arg2=%2
SET arg3=%3
FOR /f %%i in ('powershell -command "& { (Get-Date).ToString("""yyyyMMdd""")}"') DO (SET TODAY=%%i)
SET errorlog=errorlog_qGetSecPrice_%arg1%_%TODAY%.txt

Rscript.exe --vanilla --encoding=UTF-8 qGetSecPrice.R %arg1% %arg2% %arg3% > %errorlog% 2>&1

REM IF %ERRORLEVEL% NEQ 0 GOTO :ERROR
REM GOTO :END
REM :ERROR
REM ECHO "There was an error."
REM EXIT 1
REM :END
REM ECHO "End."
REM EXIT 0
