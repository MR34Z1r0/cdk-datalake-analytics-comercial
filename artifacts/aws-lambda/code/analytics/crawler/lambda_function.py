import boto3
import logging
from typing import Dict, Any
from enum import Enum

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class CrawlerState(Enum):
    """Estados posibles de un Glue Crawler."""
    READY = "READY"
    RUNNING = "RUNNING" 
    STOPPING = "STOPPING"

client_glue = boto3.client("glue")

def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Verifica el estado de un Glue Crawler y determina si debe esperar.
    """
    try:
        crawler_name = validate_and_get_crawler_name(event)
        crawler_info = get_crawler_info(crawler_name)
        
        return build_response(crawler_name, crawler_info)
        
    except ValueError as e:
        return {"error": True, "msg": f"Error de validación: {str(e)}"}
    except client_glue.exceptions.EntityNotFoundException:
        return {"error": True, "msg": f"Crawler no encontrado: {event.get('CRAWLER_NAME')}"}
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        return {"error": True, "msg": f"Error interno: {str(e)}"}

def validate_and_get_crawler_name(event: Dict[str, Any]) -> str:
    """Valida y extrae el nombre del crawler del evento."""
    if "CRAWLER_NAME" not in event:
        raise ValueError("Campo CRAWLER_NAME es requerido")
    
    crawler_name = event["CRAWLER_NAME"].strip()
    if not crawler_name:
        raise ValueError("CRAWLER_NAME no puede estar vacío")
    
    return crawler_name

def get_crawler_info(crawler_name: str) -> Dict[str, Any]:
    """Obtiene información del crawler desde AWS Glue."""
    logger.info(f"Consultando estado del crawler: {crawler_name}")
    
    response = client_glue.get_crawler(Name=crawler_name)
    return response["Crawler"]

def build_response(crawler_name: str, crawler_info: Dict[str, Any]) -> Dict[str, Any]:
    """Construye la respuesta basada en el estado del crawler."""
    state = crawler_info["State"]
    is_ready = state == CrawlerState.READY.value
    
    logger.info(f"Crawler {crawler_name} - Estado: {state}, Listo: {is_ready}")
    
    return {
        "wait": not is_ready,
        "error": False,
        "crawler_name": crawler_name,
        "crawler_state": state,
        "last_updated": crawler_info.get("LastUpdated"),
        "creation_time": crawler_info.get("CreationTime")
    }