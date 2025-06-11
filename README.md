# GeoPark Data Pipeline

Este programa automatiza la extracción, transformación y almacenamiento de datos financieros de GeoPark, siguiendo el flujo de trabajo especificado en el diagrama.

## Características

- Extracción de datos diarios de GeoPark desde Alpha Vantage API
- Obtención de precios del petróleo Brent
- Captura de datos de capitalización de mercado
- Almacenamiento en base de datos NoSQL (MongoDB Atlas)
- Generación automática de reportes en Excel
- Notificación por correo electrónico al equipo
- Programación de ejecución diaria automática
- Despliegue en la nube con Render

## Estructura de documentos en MongoDB

```json
{
  "fecha": "2023-10-10",
  "precio_geo": 10.25,
  "volumen": 150000,
  "apertura": 10.15,
  "maximo": 10.35,
  "minimo": 10.05,
  "brent": 85.75,
  "market_cap": "500000000",
  "timestamp": "2023-10-10T18:00:00.000Z"
}
```

## Requisitos

- Python 3.10 o superior
- MongoDB Atlas (cuenta gratuita)
- Conexión a internet para acceder a las APIs
- Cuenta de correo electrónico para enviar notificaciones (opcional)
- Cuenta en Render.com para despliegue en la nube (opcional)

## Instalación local

1. Clonar este repositorio
2. Instalar las dependencias:

```bash
pip install -r requirements.txt
```

3. Configurar el archivo `config.json` con sus credenciales:
   - Clave API de Alpha Vantage
   - Conexión a MongoDB Atlas
   - Credenciales de correo electrónico
   - Horario de ejecución

## Uso local

Para ejecutar el programa una vez:

```bash
python geopark_data_pipeline.py
```

O simplemente ejecute el archivo batch:

```bash
run_geopark_pipeline.bat
```

El programa se ejecutará inmediatamente y luego se programará para ejecutarse diariamente a la hora configurada en `config.json`.

## Despliegue en Render

Para desplegar la aplicación en Render, siga estos pasos:

1. Cree una cuenta en [Render](https://render.com/) si aún no tiene una.

2. Desde el Dashboard de Render, haga clic en "New" y seleccione "Blueprint".

3. Conecte su repositorio de GitHub o GitLab que contiene este código.

4. Render detectará automáticamente el archivo `render.yaml` y configurará el servicio.

5. Haga clic en "Apply" para iniciar el despliegue.

6. Una vez desplegado, puede acceder a la aplicación a través de la URL proporcionada por Render.

7. Para ejecutar manualmente el pipeline, visite `https://su-aplicacion.onrender.com/run`.

### Variables de entorno en Render

El archivo `render.yaml` ya contiene las variables de entorno necesarias:

- `MONGODB_URI`: URI de conexión a MongoDB Atlas
- `MONGODB_DB`: Nombre de la base de datos
- `MONGODB_COLLECTION`: Nombre de la colección
- `ALPHA_VANTAGE_API_KEY`: Clave API de Alpha Vantage
- `SCHEDULE_TIME`: Hora programada para la ejecución diaria (formato "HH:MM")
- `EMAIL_SENDER`: (Opcional) Dirección de correo electrónico del remitente
- `EMAIL_PASSWORD`: (Opcional) Contraseña de la cuenta de correo
- `EMAIL_RECIPIENTS`: (Opcional) Lista de destinatarios separados por comas

## Archivos del proyecto

- `geopark_data_pipeline.py`: Script principal que ejecuta todo el pipeline de datos
- `excel_generator.py`: Módulo para generar reportes en Excel con gráficos
- `init_db.py`: Script para inicializar la base de datos MongoDB
- `test_mongodb.py`: Utilidad para probar la conexión a MongoDB
- `config.json`: Archivo de configuración con credenciales y ajustes
- `requirements.txt`: Lista de dependencias de Python
- `run_geopark_pipeline.bat`: Script batch para ejecutar el pipeline en Windows
- `Procfile`: Archivo de configuración para Render
- `runtime.txt`: Especificación de la versión de Python para Render
- `render.yaml`: Configuración del servicio para despliegue en Render

## Flujo de datos

```
1. Extracción de datos desde Alpha Vantage API
2. Transformación y formato de los datos
3. Almacenamiento en MongoDB Atlas
4. Generación de reporte en Excel con gráficos
5. Envío de notificación por correo electrónico (opcional)
6. Programación de la próxima ejecución
```

## Configuración

Edite el archivo `config.json` para personalizar:

- Clave API de Alpha Vantage
- Conexión a MongoDB Atlas
- Configuración de correo electrónico
- Horario de ejecución diaria

## Logs

El programa genera logs detallados en el archivo `geopark_pipeline.log` para facilitar el seguimiento y la solución de problemas.

## Solución de problemas

Si tiene problemas con la conexión a MongoDB, ejecute:

```bash
python test_mongodb.py
```

Para inicializar la base de datos manualmente:

```bash
python init_db.py