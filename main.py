import functions_framework
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

docai_client = documentai.DocumentProcessorServiceClient()
bq_client = bigquery.Client()

@functions_framework.cloud_event
def procesar_ticket(cloud_event):
    """
    Función que se activa con la subida de un archivo a GCS,
    lo procesa con Document AI y guarda los resultados en BigQuery.
    """
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    print(f"Procesando archivo: {file_name} del bucket: {bucket_name}")

    # 1. Configurar la llamada a Document AI
    gcs_uri = f"gs://{bucket_name}/{file_name}"
    resource_name = docai_client.processor_path(PROJECT_ID, LOCATION, PROCESSOR_ID)

    # Configuración para procesar el documento online
    request = documentai.ProcessRequest(
        name=resource_name,
        gcs_document=documentai.GcsDocument(gcs_uri=gcs_uri, mime_type="image/jpeg"), # Cambia mime_type si usas imagenes (ej: "image/jpeg")
    )

    # 2. Llamar a la API de Document AI
    result = docai_client.process_document(request=request)
    document = result.document

    print("Documento procesado con éxito.")

    # 3. Preparar los datos para BigQuery
    fila_recibo = {}
    filas_line_items = []

    # Extraer entidades principales
    for entity in document.entities:
        # Mapeo de campos de DocAI a columnas de BigQuery
        if entity.type_ == "Empresa_Nombre_Comercial":
            fila_recibo['empresa_nombre_comercial'] = entity.mention_text
        elif entity.type_ == "Empresa_NIF":
            fila_recibo['empresa_nif'] = entity.mention_text
        elif entity.type_ == "Fecha":
            # Aquí podrías hacer una conversión de formato si fuera necesario
            fila_recibo['fecha_recibo'] = entity.mention_text 
        elif entity.type_ == "Total":
            fila_recibo['total_recibo'] = float(entity.mention_text.replace(',', '.')) # Convertir a número
        # ... Añadir aquí el resto de tus campos principales

        # Extraer Line Items
        elif entity.type_ == "Line_Item":
            line_item_row = {"recibo_fuente": file_name}
            for prop in entity.properties:
                if prop.type_ == "Line_Item-Concepto":
                    line_item_row["line_item_concepto"] = prop.mention_text
                elif prop.type_ == "Line_Item-Cantidad":
                    line_item_row["line_item_cantidad"] = int(prop.mention_text)
                elif prop.type_ == "Line_Item-Subtotal":
                    line_item_row["line_item_subtotal"] = float(prop.mention_text.replace(',', '.'))
            filas_line_items.append(line_item_row)

    fila_recibo['documento_fuente'] = file_name

    # 4. Insertar en BigQuery
    # Insertar en la tabla principal de recibos
    if fila_recibo:
        table_recibos_ref = bq_client.dataset(BQ_DATASET).table(BQ_TABLE_RECIBOS)
        errors = bq_client.insert_rows_json(table_recibos_ref, [fila_recibo])
        if errors:
            print(f"Errores al insertar en tabla de recibos: {errors}")
        else:
            print("Fila insertada correctamente en la tabla de recibos.")

    # Insertar en la tabla de line items
    if filas_line_items:
        table_lineitems_ref = bq_client.dataset(BQ_DATASET).table(BQ_TABLE_LINE_ITEMS)
        errors = bq_client.insert_rows_json(table_lineitems_ref, filas_line_items)
        if errors:
            print(f"Errores al insertar en tabla de line items: {errors}")
        else:
            print(f"{len(filas_line_items)} Filas insertadas correctamente en la tabla de line items.")
