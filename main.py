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

app = Flask(__name__)

# Le especificamos al cliente que debe conectarse al 'endpoint' (servidor) europeo
client_options = {"api_endpoint": "eu-documentai.googleapis.com"}

docai_client = documentai.DocumentProcessorServiceClient(client_options=client_options)
bq_client = bigquery.Client()

@app.route("/", methods=["POST"])
def procesar_ticket():
    event = request.get_json()
    print("Evento recibido:", event)

    # --- LÍNEAS CORREGIDAS ---
    # Leemos directamente del objeto 'event', no de un sub-apartado 'data'
    bucket_name = event.get("bucket")
    file_name = event.get("name")
    # -------------------------

    print(f"Valor de bucket_name: {bucket_name}")
    print(f"Valor de file_name: {file_name}")

    if not bucket_name or not file_name:
        print("ERROR: La condición de validación ha fallado. El bucket o el nombre del archivo están vacíos o son None.")
        return "Error en el formato del evento", 400

    gcs_uri = f"gs://{bucket_name}/{file_name}"
    resource_name = docai_client.processor_path(PROJECT_ID, LOCATION, PROCESSOR_ID)
    
# --- INICIO DEL CÓDIGO MEJORADO ---

# Detectar el tipo de archivo a partir de la extensión
file_extension = file_name.lower().split('.')[-1]
if file_extension == "pdf":
    mime_type = "application/pdf"
elif file_extension in ["jpg", "jpeg"]:
    mime_type = "image/jpeg"
elif file_extension == "png":
    mime_type = "image/png"
elif file_extension == "tif" or file_extension == "tiff":
    mime_type = "image/tiff"
else:
    print(f"ERROR: Formato de archivo no compatible: {file_extension}")
    return f"Formato no compatible: {file_extension}", 400

print(f"Detectado mime_type: {mime_type} para el archivo {file_name}")

request_docai = documentai.ProcessRequest(
    name=resource_name,
    gcs_document=documentai.GcsDocument(gcs_uri=gcs_uri, mime_type=mime_type),
)
# --- FIN DEL CÓDIGO MEJORADO ---

    result = docai_client.process_document(request=request_docai)
    document = result.document
    print("Documento procesado con éxito en Document AI.")
    
    fila_recibo = {"documento_fuente": file_name}
    filas_line_items = []
    
    # --- AQUÍ VA TU LÓGICA DE MAPEO DE ENTIDADES ---
    # Este es solo un ejemplo, complétalo con todos tus campos
    for entity in document.entities:
        if entity.type_ == "Empresa_NIF":
            fila_recibo['empresa_nif'] = entity.mention_text
        elif entity.type_ == "Total":
            try:
                total_text = entity.mention_text.replace('€', '').replace(',', '.').strip()
                fila_recibo['total_recibo'] = float(total_text)
            except (ValueError, TypeError):
                print(f"No se pudo convertir el total '{entity.mention_text}' a número.")
        # ...Añade aquí el resto de tus mapeos para la tabla principal...
        
        elif entity.type_ == "Line_Item":
            line_item_row = {"recibo_fuente": file_name}
            for prop in entity.properties:
                if prop.type_ == "Line_Item-Concepto":
                    line_item_row["line_item_concepto"] = prop.mention_text
                # ...Añade el resto de campos de line_item...
            filas_line_items.append(line_item_row)
    # ------------------------------------------------

    if len(fila_recibo) > 1: # Solo insertar si hemos extraído algún dato además del nombre del archivo
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

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
