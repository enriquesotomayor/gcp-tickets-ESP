import os
from flask import Flask, request

from google.cloud import documentai
from google.cloud import bigquery
from google.cloud import storage

# --- CONFIGURACIÓN: RELLENA TUS DATOS AQUÍ ---
PROJECT_ID = "bold-rampart-464317-q7"      # Tu Project ID (ej: bold-rampart-...)
LOCATION = "eu"                           # Región de tu procesador (ej: "eu" o "us")
PROCESSOR_ID = "285b1d87177fbb37"            # El ID de tu procesador de Document AI
BQ_DATASET = "Data_Tickets_Restaurantes"
BQ_TABLE_RECIBOS = "Tabla_Tickets_Restaurantes_ESP"
BQ_TABLE_LINE_ITEMS = "line_items_Tickets"
# -------------------------------------------------

# --- Inicialización de la aplicación Flask y clientes de Google ---
app = Flask(__name__)
docai_client = documentai.DocumentProcessorServiceClient()
bq_client = bigquery.Client()

# --- Ruta principal que recibirá los eventos de Eventarc ---
@app.route("/", methods=["POST"])
def procesar_ticket():
    # El evento de Eventarc llega como una petición POST
    event = request.get_json()
    print("Evento recibido:", event)

    # Extraer los datos del archivo del evento
    data = event.get("data", {})
    bucket_name = data.get("bucket")
    file_name = data.get("name")

    if not bucket_name or not file_name:
        print("ERROR: Evento no contenía nombre de bucket o archivo.")
        return "Error en el formato del evento", 400

    print(f"Procesando archivo: {file_name} del bucket: {bucket_name}")
    
    # --- El resto de la lógica es la misma que teníamos ---
    gcs_uri = f"gs://{bucket_name}/{file_name}"
    resource_name = docai_client.processor_path(PROJECT_ID, LOCATION, PROCESSOR_ID)
    request_docai = documentai.ProcessRequest(
        name=resource_name,
        gcs_document=documentai.GcsDocument(gcs_uri=gcs_uri, mime_type="application/pdf"),
    )
    result = docai_client.process_document(request=request_docai)
    document = result.document
    print("Documento procesado con éxito.")
    
    # Preparar datos para BigQuery
    fila_recibo = {}
    filas_line_items = []
    fila_recibo['documento_fuente'] = file_name
    
    for entity in document.entities:
        # Aquí va toda tu lógica de mapeo que definimos antes
        # Ejemplo:
        if entity.type_ == "Empresa_NIF":
            fila_recibo['empresa_nif'] = entity.mention_text
        elif entity.type_ == "Total":
            try:
                # Intentar convertir a número, manejando comas y espacios
                total_text = entity.mention_text.replace('€', '').replace(',', '.').strip()
                fila_recibo['total_recibo'] = float(total_text)
            except (ValueError, TypeError):
                print(f"No se pudo convertir el total '{entity.mention_text}' a número.")
        # ...Añade aquí el resto de tus mapeos...
        
        elif entity.type_ == "Line_Item":
            line_item_row = {"recibo_fuente": file_name}
            for prop in entity.properties:
                if prop.type_ == "Line_Item-Concepto":
                    line_item_row["line_item_concepto"] = prop.mention_text
                elif prop.type_ == "Line_Item-Cantidad":
                    try:
                        line_item_row["line_item_cantidad"] = int(prop.mention_text.replace(',', '.'))
                    except (ValueError, TypeError):
                         print(f"No se pudo convertir la cantidad '{prop.mention_text}' a número.")
                elif prop.type_ == "Line_Item-Subtotal":
                    try:
                        subtotal_text = prop.mention_text.replace('€', '').replace(',', '.').strip()
                        line_item_row["line_item_subtotal"] = float(subtotal_text)
                    except (ValueError, TypeError):
                        print(f"No se pudo convertir el subtotal '{prop.mention_text}' a número.")
            filas_line_items.append(line_item_row)

    # Insertar en BigQuery
    if fila_recibo:
        table_recibos_ref = bq_client.dataset(BQ_DATASET).table(BQ_TABLE_RECIBOS)
        errors = bq_client.insert_rows_json(table_recibos_ref, [fila_recibo])
        if not errors:
            print("Fila insertada correctamente en la tabla de recibos.")
        else:
            print(f"Errores al insertar en tabla de recibos: {errors}")

    if filas_line_items:
        table_lineitems_ref = bq_client.dataset(BQ_DATASET).table(BQ_TABLE_LINE_ITEMS)
        errors = bq_client.insert_rows_json(table_lineitems_ref, filas_line_items)
        if not errors:
            print(f"{len(filas_line_items)} filas insertadas correctamente en la tabla de line items.")
        else:
            print(f"Errores al insertar en tabla de line items: {errors}")
            
    return "Procesado con éxito", 200

# --- Punto de entrada para Gunicorn en Cloud Run ---
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
