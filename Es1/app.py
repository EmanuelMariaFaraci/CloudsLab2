import math
from flask import Flask, jsonify

app = Flask(__name__)

def numerical_integration(lower, upper, N):
    """
    Computes numerical integration of abs(sin(x)) for a given N.
    """
    dx = (upper - lower) / N
    total_area = 0.0
    for i in range(N):
        x = lower + (i * dx)
        # Compute area of rectangle: height * width
        total_area += abs(math.sin(x)) * dx
    return total_area

@app.route('/numericalintegralservice/<lower>/<upper>', methods=['GET'])
def get_integral(lower, upper):
    """
    Service to calculate integrals for N = 10, 100, ..., 1M.
    """
    try:
        lower = float(lower)
        upper = float(upper)
    except ValueError:
        return "Inputs must be numbers", 400
    results = {}
    # Loop for N = 10, 100, 1000, 10k, 100k, 1M [cite: 12]
    n_values = [10, 100, 1000, 10000, 100000, 1000000]
    
    for n in n_values:
        result = numerical_integration(lower, upper, n)
        results[f"N={n}"] = result
        
    return jsonify({
        "lower": lower,
        "upper": upper,
        "results": results
    })

if __name__ == '__main__':
    # Run on 0.0.0.0 to be accessible externally if needed
    app.run(host='0.0.0.0', port=5000)