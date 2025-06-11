@echo off
echo Iniciando GeoPark Data Pipeline...
echo.

REM Verificar si Python está instalado
where python >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo Error: Python no está instalado o no está en el PATH.
    echo Por favor, instale Python 3.8 o superior.
    pause
    exit /b 1
)

REM Verificar si las dependencias están instaladas
echo Verificando dependencias...
pip show pymongo pandas schedule requests openpyxl >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo Instalando dependencias...
    pip install -r requirements.txt
    if %ERRORLEVEL% NEQ 0 (
        echo Error: No se pudieron instalar las dependencias.
        pause
        exit /b 1
    )
)

REM Inicializar la base de datos MongoDB
echo Inicializando base de datos MongoDB...
python init_db.py
if %ERRORLEVEL% NEQ 0 (
    echo Error: No se pudo inicializar la base de datos MongoDB.
    echo Asegúrese de que MongoDB está instalado y en ejecución.
    pause
    exit /b 1
)

REM Ejecutar el pipeline
echo Ejecutando GeoPark Data Pipeline...
python geopark_data_pipeline.py

pause 