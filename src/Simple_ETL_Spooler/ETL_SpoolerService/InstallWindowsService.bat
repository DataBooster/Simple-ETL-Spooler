@Echo off
C:\Windows\Microsoft.NET\Framework64\v4.0.30319\InstallUtil.exe %* "%~dp0Simple_ETL_Spooler.exe"
If Not "%1"=="" Goto :EOF
sc failure Simple_ETL_Task_Spooler reset= 0 actions= restart/300000
sc start Simple_ETL_Task_Spooler
