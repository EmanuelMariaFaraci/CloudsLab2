import azure.functions as func
import logging
import math
import json

app = func.FunctionApp()

@app.route(route="numericalintegral", auth_level=func.AuthLevel.ANONYMOUS)
def numerical_integral(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Get params from query string or route if configured
        lower = float(req.params.get('lower', 0))
        upper = float(req.params.get('upper', 3.14159))
        
        results = {}
        n_values = [10, 100, 1000, 10000, 100000, 1000000]

        for n in n_values:
            dx = (upper - lower) / n
            total = 0.0
            for i in range(n):
                x = lower + i * dx
                total += abs(math.sin(x)) * dx
            results[f"N={n}"] = total

        return func.HttpResponse(
            json.dumps(results),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)