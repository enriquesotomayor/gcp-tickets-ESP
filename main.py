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
docai_client = documentai.DocumentProcessorServiceClient()
bq_client = bigquery.Client()

@app.route("/", methods=["POST"])
def procesar_ticket():
    event = request.get_json()
    print("--- INICIO DEL PROCESO ---")
    print("Evento recibido:", event)

    data = event.get("data", {})
    bucket_name = data.get("bucket")
    file_name = data.get("name")

    # --- NUEVAS LÍNEAS DE DEPURACIÓN ---
    print("--- VALORES EXTRAÍDOS ---")
    print(f"Valor de bucket_name: {bucket_name}")
    print(f"Tipo de bucket_name: {type(bucket_name)}")
    print(f"Valor de file_name: {file_name}")
    print(f"Tipo de file_name: {type(file_name)}")
    # ------------------------------------

    if not bucket_name or not file_name:
        print("ERROR: La condición de validación ha fallado. El bucket o el nombre del archivo están vacíos o son None.")
        return "Error en el formato del evento", 400

    # --- El resto del código es el mismo ---
    gcs_uri = f"gs://{bucket_name}/{file_name}"
    resource_name = docai_client.processor_path(PROJECT_ID, LOCATION, PROCESSOR_ID)
    request_docai = documentai.ProcessRequest(
        name=resource_name,
        gcs_document=documentai.GcsDocument(gcs_uri=gcs_uri, mime_type="application/pdf"),
    )
    result = docai_client.process_document(request=request_docai)
    document = result.document
    print("Documento procesado con éxito en Document AI.")
    
    fila_recibo = {"documento_fuente": file_name}
    filas_line_items = []
    
    # ... (Aquí va tu lógica de mapeo de entidades como antes) ...
    # Ejemplo:
    for entity in document.entities:
      if entity.type_ == "Empresa_NIF":
        fila_recibo['empresa_nif'] = entity.mention_text

    if fila_recibo:
        errors = bq_client.insert_rows_json(f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_RECIBOS}", [fila_recibo])
        if not errors:
            print("Fila insertada correctamente en la tabla de recibos.")
        else:
            print(f"Errores al insertar en tabla de recibos: {errors}")

    # ... (Lógica para line_items) ...
            
    return "Procesado con éxito", 200

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
