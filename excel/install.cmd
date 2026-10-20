@echo off
rem Template: the API serves this at /excel/install.cmd with its own address, the add-in id and the file name filled in.
rem Installs the Event Risk Excel add-in for the current Windows user (no admin rights needed):
rem downloads the manifest to C:\addins and registers it as a developer add-in.
title Event Risk for Excel - installer
echo.
echo Installing the Event Risk add-in for Excel...
echo.
if not exist "C:\addins" mkdir "C:\addins"
powershell -NoProfile -Command "try { Invoke-WebRequest -UseBasicParsing '{BASE}/excel/manifest.xml' -OutFile 'C:\addins\{FILE}' } catch { exit 1 }"
if errorlevel 1 goto failed
reg add "HKCU\Software\Microsoft\Office\16.0\WEF\Developer" /v "{ID}" /t REG_SZ /d "C:\addins\{FILE}" /f >nul
if errorlevel 1 goto failed
echo Done! The add-in is installed.
echo.
echo Next: open Excel (if it is already open, close it and open it again),
echo then click the Event Risk button on the Home tab.
echo.
pause
exit /b 0

:failed
echo Sorry - the installation did not finish.
echo Check that this computer can open {BASE} in a web browser, then try again.
echo.
pause
exit /b 1
