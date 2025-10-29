import argparse
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import requests

global args

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("SPEGauge Dashboard"),

    dcc.Interval(
        id='interval-component',
        interval=5*1000,  # in milliseconds
        n_intervals=0
    ),

    dcc.Dropdown(
        id='job-id-dropdown',
        options=[{'label': 'Select a consumer id...', 'value': ''}],
        value='',
        clearable=False
    ),

    html.H2("Status"),
    html.Div(id='display-status'),

    html.H2("Throughput [# Records/s]"),
    html.Div(id='display-throughput'),

    html.H2("Backlog [# Records]"),
    html.Div(id='display-backlog'),
])

@app.callback(
    Output('job-id-dropdown', 'options'),
    Input('interval-component', 'n_intervals')
)
def update_dropdown_options(n_intervals):
    # Make API request to get job IDs
    response = requests.get(f"{args.host}/jobs/")

    if response.status_code == 200:
        # Parse the JSON response
        json = response.json()

        # Format the options for the dropdown
        dropdown_options = [{'label': job_id, 'value': job_id} for job_id in json["jobIds"]]

        return dropdown_options
    else:
        return [{'label': f"Failed to fetch job IDs (Status code: {response.status_code})", 'value': ''}]

@app.callback(
    Output('display-throughput', 'children'),
    Output('display-backlog', 'children'),
    Output('display-status', 'children'),
    Input('interval-component', 'n_intervals'),
    Input('job-id-dropdown', 'value')
)
def update_data(n_intervals, selected_job_id):
    if not selected_job_id:
        return "", "", "Please select a consumer group in the dropdown above."

    # Make API request
    response = requests.get(f"{args.host}/jobs/{selected_job_id}")

    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # Extract the array from the JSON
        backlog = data.get('currentBacklog', [])
        throughput = data.get('currentThroughput', None)
        status = data.get('status', None)

        if status != "RUNNING":
            return html.H3("n/a (not running)"), html.H3("n/a (not running)"), html.H3(f"{status}") if status is not None else "Failed to fetch status"
        else:
            return html.H3(f"{throughput}") if throughput is not None else "Failed to fetch throughput", html.H3(f"{'  |  '.join([f'#{i+1}: {value}' for i, value in enumerate(backlog)])}") if backlog else "No backlog values", html.H3(f"{status}") if status is not None else "Failed to fetch status"
    else:
        return "Failed to fetch data from the API"

def parse_args():
    parser = argparse.ArgumentParser(description="Monitoring script")

    # positional arguments
    parser.add_argument("host", type=str, help="Protocol, hostname + port, e.g., http://localhost:8100")

    return parser.parse_args()

if __name__ == '__main__':
    global args
    args = parse_args()
    app.run_server(debug=True)