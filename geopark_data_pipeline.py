import requests
import json
import pandas as pd
from datetime import datetime
import time
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import pymongo
import schedule
import logging
import sys

# Import custom modules
import excel_generator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("geopark_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load configuration
def load_config():
    # First try to load from environment variables (for Render deployment)
    if os.environ.get("MONGODB_URI"):
        logger.info("Loading configuration from environment variables")
        return {
            "api": {
                "alpha_vantage_key": os.environ.get("ALPHA_VANTAGE_API_KEY", "BCCMWJX0WL7IQYVE")
            },
            "mongodb": {
                "connection_string": os.environ.get("MONGODB_URI"),
                "database": os.environ.get("MONGODB_DB", "market_data"),
                "collection": os.environ.get("MONGODB_COLLECTION", "geopark_daily")
            },
            "email": {
                "sender": os.environ.get("EMAIL_SENDER", ""),
                "password": os.environ.get("EMAIL_PASSWORD", ""),
                "recipients": os.environ.get("EMAIL_RECIPIENTS", "").split(",") if os.environ.get("EMAIL_RECIPIENTS") else []
            },
            "schedule": {
                "time": os.environ.get("SCHEDULE_TIME", "18:00")
            }
        }
    
    # Otherwise, load from config.json file
    try:
        logger.info("Loading configuration from config.json file")
        with open('config.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        return None

CONFIG = load_config()
if not CONFIG:
    raise Exception("Failed to load configuration. Please check config.json file or environment variables.")

# API Configuration
ALPHA_VANTAGE_API_KEY = CONFIG["api"]["alpha_vantage_key"]
GEOPARK_SYMBOL = "GPRK"

# MongoDB Configuration
MONGO_CONNECTION_STRING = CONFIG["mongodb"]["connection_string"]
DB_NAME = CONFIG["mongodb"]["database"]
COLLECTION_NAME = CONFIG["mongodb"]["collection"]

# Email Configuration
EMAIL_SENDER = CONFIG["email"]["sender"]
EMAIL_PASSWORD = CONFIG["email"]["password"]
EMAIL_RECIPIENTS = CONFIG["email"]["recipients"]

def connect_to_mongodb():
    """Connect to MongoDB and return the collection object"""
    try:
        client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        logger.info("Successfully connected to MongoDB")
        return collection
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}")
        return None

def fetch_geopark_data():
    """Fetch GeoPark daily stock data from Alpha Vantage API"""
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={GEOPARK_SYMBOL}&apikey={ALPHA_VANTAGE_API_KEY}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for HTTP errors
        data = response.json()
        
        if "Error Message" in data:
            logger.error(f"API Error: {data['Error Message']}")
            return None
        
        logger.info("Successfully fetched GeoPark data from Alpha Vantage")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching GeoPark data: {e}")
        return None

def fetch_brent_price():
    """Fetch Brent crude oil price from Alpha Vantage API"""
    url = f"https://www.alphavantage.co/query?function=WTI&interval=daily&apikey={ALPHA_VANTAGE_API_KEY}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if "Error Message" in data:
            logger.error(f"API Error: {data['Error Message']}")
            return None
        
        logger.info("Successfully fetched Brent price data")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching Brent price: {e}")
        return None

def fetch_market_cap():
    """Fetch GeoPark market cap from Alpha Vantage API"""
    url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={GEOPARK_SYMBOL}&apikey={ALPHA_VANTAGE_API_KEY}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if "Error Message" in data:
            logger.error(f"API Error: {data['Error Message']}")
            return None
        
        logger.info("Successfully fetched market cap data")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching market cap: {e}")
        return None

def transform_data(geopark_data, brent_data, market_cap_data):
    """Transform and combine data from different sources"""
    try:
        # Extract GeoPark daily data
        time_series = geopark_data.get("Time Series (Daily)", {})
        
        # Get the latest date (first key in the time series)
        if not time_series:
            logger.error("No time series data available")
            return None
            
        latest_date = list(time_series.keys())[0]
        latest_data = time_series[latest_date]
        
        # Extract Brent price data
        brent_series = brent_data.get("data", [])
        latest_brent = next((item for item in brent_series if item["date"] == latest_date), None)
        brent_price = latest_brent["value"] if latest_brent else None
        
        # Extract market cap
        market_cap = market_cap_data.get("MarketCapitalization", "N/A")
        
        # Create document structure
        document = {
            "fecha": latest_date,
            "precio_geo": float(latest_data["4. close"]),
            "volumen": int(latest_data["5. volume"]),
            "apertura": float(latest_data["1. open"]),
            "maximo": float(latest_data["2. high"]),
            "minimo": float(latest_data["3. low"]),
            "brent": brent_price,
            "market_cap": market_cap,
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"Successfully transformed data for date: {latest_date}")
        return document
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        return None

def store_in_mongodb(collection, document):
    """Store the document in MongoDB"""
    try:
        # Check if document for this date already exists
        existing = collection.find_one({"fecha": document["fecha"]})
        
        if existing:
            # Update existing document
            collection.update_one(
                {"fecha": document["fecha"]},
                {"$set": document}
            )
            logger.info(f"Updated document for date: {document['fecha']}")
        else:
            # Insert new document
            collection.insert_one(document)
            logger.info(f"Inserted new document for date: {document['fecha']}")
            
        return True
    except Exception as e:
        logger.error(f"Error storing data in MongoDB: {e}")
        return False

def generate_excel_report(collection):
    """Generate Excel report from MongoDB data"""
    try:
        # Get the last 30 days of data
        cursor = collection.find().sort("fecha", -1).limit(30)
        data = list(cursor)
        
        if not data:
            logger.error("No data available for Excel report")
            return None
        
        # Use the excel generator module
        report_path = excel_generator.generate_excel_report(data)
        
        logger.info(f"Excel report generated: {report_path}")
        return report_path
    except Exception as e:
        logger.error(f"Error generating Excel report: {e}")
        return None

def send_email_notification(report_path):
    """Send email notification with the Excel report attached"""
    # Check if email credentials are set to real values
    if not EMAIL_SENDER or not EMAIL_PASSWORD or EMAIL_SENDER == "your_real_email@gmail.com" or EMAIL_PASSWORD == "your_app_password":
        logger.warning("Email credentials not configured. Skipping email notification.")
        return False
        
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_SENDER
        msg['To'] = ", ".join(EMAIL_RECIPIENTS)
        msg['Subject'] = f"GeoPark Daily Report - {datetime.now().strftime('%Y-%m-%d')}"
        
        body = """
        <html>
        <body>
            <p>Estimado equipo,</p>
            <p>Adjunto encontrarán el reporte diario de GeoPark con la información actualizada.</p>
            <p>Saludos cordiales,</p>
            <p>Sistema Automatizado de Reportes</p>
        </body>
        </html>
        """
        
        msg.attach(MIMEText(body, 'html'))
        
        # Attach Excel report
        with open(report_path, 'rb') as file:
            attachment = MIMEApplication(file.read(), _subtype="xlsx")
            attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(report_path))
            msg.attach(attachment)
        
        # Connect to SMTP server and send email
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        
        logger.info(f"Email notification sent to {', '.join(EMAIL_RECIPIENTS)}")
        return True
    except Exception as e:
        logger.error(f"Error sending email notification: {e}")
        return False

def run_pipeline():
    """Run the complete data pipeline"""
    logger.info("Starting GeoPark data pipeline")
    
    # Connect to MongoDB
    collection = connect_to_mongodb()
    if collection is None:  # Changed from "if not collection:"
        logger.error("Failed to connect to MongoDB")
        return
    
    # Fetch data from APIs
    geopark_data = fetch_geopark_data()
    if not geopark_data:
        return
        
    # Add delay to avoid API rate limits
    time.sleep(2)
    
    brent_data = fetch_brent_price()
    if not brent_data:
        return
        
    time.sleep(2)
    
    market_cap_data = fetch_market_cap()
    if not market_cap_data:
        return
    
    # Transform data
    document = transform_data(geopark_data, brent_data, market_cap_data)
    if not document:
        return
    
    # Store in MongoDB
    success = store_in_mongodb(collection, document)
    if not success:
        return
    
    # Generate Excel report
    report_path = generate_excel_report(collection)
    if not report_path:
        return
    
    # Send email notification (optional)
    email_sent = send_email_notification(report_path)
    if not email_sent:
        logger.warning("Email notification was not sent, but the pipeline completed successfully")
    
    logger.info("GeoPark data pipeline completed successfully")
    logger.info(f"Excel report saved at: {os.path.abspath(report_path)}")
    
    return report_path

def schedule_daily_run():
    """Schedule the pipeline to run daily at a specific time"""
    # Run daily at the configured time
    schedule_time = CONFIG["schedule"]["time"]
    schedule.every().day.at(schedule_time).do(run_pipeline)
    
    logger.info(f"Pipeline scheduled to run daily at {schedule_time}")
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

def web_server():
    """Simple web server for Render"""
    import http.server
    import socketserver
    
    PORT = int(os.environ.get("PORT", 10000))
    
    class MyHandler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/' or self.path == '':
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                html_content = """
                <html>
                <head>
                    <title>GeoPark Data Pipeline</title>
                    <style>
                        body {
                            font-family: Arial, sans-serif;
                            margin: 40px;
                            line-height: 1.6;
                        }
                        h1 {
                            color: #2c3e50;
                        }
                        .status {
                            padding: 10px;
                            background-color: #e8f5e9;
                            border-left: 5px solid #4caf50;
                            margin-bottom: 20px;
                        }
                        .button {
                            display: inline-block;
                            padding: 10px 20px;
                            background-color: #4caf50;
                            color: white;
                            text-decoration: none;
                            border-radius: 4px;
                        }
                    </style>
                </head>
                <body>
                    <h1>GeoPark Data Pipeline</h1>
                    <div class="status">
                        <p><strong>Status:</strong> Running</p>
                    </div>
                    <p>Esta aplicacion extrae datos de GeoPark desde Alpha Vantage API y los almacena en MongoDB Atlas.</p>
                    <p>Para ejecutar el pipeline manualmente, haga clic en el siguiente boton:</p>
                    <a href="/run" class="button">Ejecutar Pipeline</a>
                </body>
                </html>
                """
                self.wfile.write(html_content.encode('utf-8'))
            elif self.path == '/run':
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                try:
                    result = run_pipeline()
                    if result:
                        html_content = f"""
                        <html>
                        <head>
                            <title>Pipeline Ejecutado</title>
                            <style>
                                body {{
                                    font-family: Arial, sans-serif;
                                    margin: 40px;
                                    line-height: 1.6;
                                }}
                                h1 {{
                                    color: #2c3e50;
                                }}
                                .success {{
                                    padding: 10px;
                                    background-color: #e8f5e9;
                                    border-left: 5px solid #4caf50;
                                    margin-bottom: 20px;
                                }}
                                .button {{
                                    display: inline-block;
                                    padding: 10px 20px;
                                    background-color: #2196f3;
                                    color: white;
                                    text-decoration: none;
                                    border-radius: 4px;
                                }}
                            </style>
                        </head>
                        <body>
                            <h1>Pipeline ejecutado correctamente</h1>
                            <div class="success">
                                <p>Reporte guardado en: {result}</p>
                            </div>
                            <p>Los datos han sido extraidos, transformados y almacenados en MongoDB Atlas.</p>
                            <a href="/" class="button">Volver al inicio</a>
                        </body>
                        </html>
                        """
                        self.wfile.write(html_content.encode('utf-8'))
                    else:
                        html_content = """
                        <html>
                        <head>
                            <title>Error en Pipeline</title>
                            <style>
                                body {
                                    font-family: Arial, sans-serif;
                                    margin: 40px;
                                    line-height: 1.6;
                                }
                                h1 {
                                    color: #e53935;
                                }
                                .error {
                                    padding: 10px;
                                    background-color: #ffebee;
                                    border-left: 5px solid #e53935;
                                    margin-bottom: 20px;
                                }
                                .button {
                                    display: inline-block;
                                    padding: 10px 20px;
                                    background-color: #2196f3;
                                    color: white;
                                    text-decoration: none;
                                    border-radius: 4px;
                                }
                            </style>
                        </head>
                        <body>
                            <h1>Error al ejecutar el pipeline</h1>
                            <div class="error">
                                <p>Consulte los logs para obtener mas detalles.</p>
                            </div>
                            <a href="/" class="button">Volver al inicio</a>
                        </body>
                        </html>
                        """
                        self.wfile.write(html_content.encode('utf-8'))
                except Exception as e:
                    html_content = f"""
                    <html>
                    <head>
                        <title>Error en Pipeline</title>
                        <style>
                            body {{
                                font-family: Arial, sans-serif;
                                margin: 40px;
                                line-height: 1.6;
                            }}
                            h1 {{
                                color: #e53935;
                            }}
                            .error {{
                                padding: 10px;
                                background-color: #ffebee;
                                border-left: 5px solid #e53935;
                                margin-bottom: 20px;
                            }}
                            .button {{
                                display: inline-block;
                                padding: 10px 20px;
                                background-color: #2196f3;
                                color: white;
                                text-decoration: none;
                                border-radius: 4px;
                            }}
                        </style>
                    </head>
                    <body>
                        <h1>Error al ejecutar el pipeline</h1>
                        <div class="error">
                            <p>Error: {str(e)}</p>
                        </div>
                        <a href="/" class="button">Volver al inicio</a>
                    </body>
                    </html>
                    """
                    self.wfile.write(html_content.encode('utf-8'))
            else:
                self.send_response(404)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                html_content = """
                <html>
                <head>
                    <title>404 Not Found</title>
                    <style>
                        body {
                            font-family: Arial, sans-serif;
                            margin: 40px;
                            line-height: 1.6;
                        }
                        h1 {
                            color: #e53935;
                        }
                        .error {
                            padding: 10px;
                            background-color: #ffebee;
                            border-left: 5px solid #e53935;
                            margin-bottom: 20px;
                        }
                        .button {
                            display: inline-block;
                            padding: 10px 20px;
                            background-color: #2196f3;
                            color: white;
                            text-decoration: none;
                            border-radius: 4px;
                        }
                    </style>
                </head>
                <body>
                    <h1>404 - Pagina no encontrada</h1>
                    <div class="error">
                        <p>La pagina que esta buscando no existe.</p>
                    </div>
                    <a href="/" class="button">Volver al inicio</a>
                </body>
                </html>
                """
                self.wfile.write(html_content.encode('utf-8'))
    
    logger.info(f"Starting web server on port {PORT}")
    try:
        # Ensure we bind to 0.0.0.0 to listen on all network interfaces
        httpd = socketserver.TCPServer(("0.0.0.0", PORT), MyHandler)
        logger.info(f"Server running at http://0.0.0.0:{PORT}")
        httpd.serve_forever()
    except Exception as e:
        logger.error(f"Error starting web server: {e}")
        # Try alternate port if specified port fails
        alt_port = 8080
        logger.info(f"Trying alternate port {alt_port}")
        try:
            httpd = socketserver.TCPServer(("0.0.0.0", alt_port), MyHandler)
            logger.info(f"Server running at http://0.0.0.0:{alt_port}")
            httpd.serve_forever()
        except Exception as e:
            logger.error(f"Error starting web server on alternate port: {e}")
            raise

if __name__ == "__main__":
    # Check if running in Render
    if os.environ.get("RENDER"):
        logger.info("Running in Render environment")
        # Run the pipeline once at startup
        run_pipeline()
        # Start web server
        web_server()
    else:
        # Run once immediately
        run_pipeline()
        
        # Then schedule for daily execution
        schedule_daily_run() 