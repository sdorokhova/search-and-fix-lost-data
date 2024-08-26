@echo off
setlocal enabledelayedexpansion

if "%1"=="" (
    echo No snapshot path provided.
	rem set snapshot=C:/Users/SvetlanaDorokhova/Documents/operate/SUPPORT-22102/cancel/20240812-zeebe-data/usr/local/zeebe/data/35/snapshots/898004999-163-3074324877-3074322796
) else (
    set snapshot=%1
)

if "%2"=="" (
    echo No output file path provided.
	rem set snapshot=process-instances
) else (
    set outputfile=%2
)

set zdbjar=C:/programs/zdb/zdb.jar

echo %snapshot%

for /f "tokens=*" %%p in ('java -jar %zdbjar% process list -p %snapshot% ^| jq ".data[].key"') do (
    echo Process definition key %%p
	set "currentProcess=%%p"
    java -jar "%zdbjar%" process instances "!currentProcess!" -p "%snapshot%" | jq ".data[].elementRecord.processInstanceRecord.processInstanceKey" >> %outputfile%
)
	
endlocal

