import azure.functions as func
import azure.durable_functions as df
from azure.storage.blob import BlobServiceClient
import logging
import os

# Create the main function app
app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# --- 1. HTTP STARTER ---
@app.route(route="orchestrators/MapReduceOrchestrator")
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    instance_id = await client.start_new("MapReduceOrchestrator", None, None)
    logging.info(f"Started orchestration with ID = '{instance_id}'.")
    return client.create_check_status_response(req, instance_id)

# --- 2. ORCHESTRATOR ---
@app.orchestration_trigger(context_name="context")
def MapReduceOrchestrator(context: df.DurableOrchestrationContext):
    # Step 1: Get Input Data (List of <line_number, line_string>) [cite: 106]
    input_data = yield context.call_activity("GetInputDataFn", None)

    # Step 2: Mapper (Fan-Out) [cite: 86, 94]
    # We run the mapper in parallel for every line of input
    map_tasks = []
    for item in input_data:
        map_tasks.append(context.call_activity("Mapper", item))
    
    # Wait for all mappers to finish
    map_results = yield context.task_all(map_tasks)
    
    # Flatten the list of lists (because each mapper returns a list of pairs)
    all_mapped_items = [pair for sublist in map_results for pair in sublist]

    # Step 3: Shuffler [cite: 86, 91]
    # Groups the data: <word, [1, 1, 1...]>
    shuffled_data = yield context.call_activity("Shuffler", all_mapped_items)

    # Step 4: Reducer (Fan-Out) [cite: 86, 88]
    # Run reducer in parallel for each word group
    reduce_tasks = []
    for key, values in shuffled_data.items():
        reduce_tasks.append(context.call_activity("Reducer", (key, values)))
    
    final_results = yield context.task_all(reduce_tasks)

    return final_results

# --- 3. ACTIVITIES ---

@app.activity_trigger(input_name="info")
def GetInputDataFn(info):
    """
    Reads files from Azure Blob Storage and returns list of (offset, line)
    """
    # NOTE: Set 'AzureWebJobsStorage' in your local.settings.json or App Config
    connect_str = os.getenv("AzureWebJobsStorage") 
    container_name = "mrinput" # Ensure this matches your container [cite: 100]
    
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    input_lines = []
    line_offset = 0

    # Iterate over blobs (mrinput-1, mrinput-2, etc.)
    for blob in container_client.list_blobs():
        blob_client = container_client.get_blob_client(blob)
        download_stream = blob_client.download_blob()
        content = download_stream.readall().decode('utf-8')
        
        # Split into lines
        lines = content.split('\n')
        for line in lines:
            if line.strip(): # Ignore empty lines
                input_lines.append((line_offset, line))
                line_offset += 1
                
    return input_lines

@app.activity_trigger(input_name="inp")
def Mapper(inp):
    """
    Input: (offset, line_string)
    Output: List of (word, 1) [cite: 87]
    """
    _, line = inp
    words = line.split()
    results = []
    for word in words:
        # Simple tokenization: remove punctuation, lowercase
        clean_word = word.strip(".,!?;:\"()").lower()
        if clean_word:
            results.append((clean_word, 1))
    return results

@app.activity_trigger(input_name="mappedItems")
def Shuffler(mappedItems):
    """
    Input: List of (word, 1)
    Output: Dictionary { word: [1, 1, 1...] } [cite: 91]
    """
    grouped = {}
    for word, count in mappedItems:
        if word not in grouped:
            grouped[word] = []
        grouped[word].append(count)
    return grouped

@app.activity_trigger(input_name="reduceInput")
def Reducer(reduceInput):
    """
    Input: (word, [1, 1, 1...])
    Output: (word, total_count) [cite: 89]
    """
    word, counts = reduceInput
    return (word, sum(counts))