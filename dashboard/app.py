import json
import io
import urllib.parse
import pandas as pd
import plotly.express as px
import xlsxwriter
import flask
from flask import Flask
import dash
from dash import Dash
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
from dash import dcc
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from dash import html

with open("awp_map_new.json", "r") as f:
    countries = json.load(f)

df = pd.read_csv("awp_csv_new.csv", dtype={"country_name": str})
#print(df.dtypes)

# Build your components
app = Dash(__name__, external_stylesheets=[dbc.themes.LUX, "https://codepen.io/chriddyp/pen/bWLwgP.css"])

app.title = "WaPOR4Awp: Agricultural Water Productivity Dashboard"

mytitle = dcc.Markdown(children='')
map_graph = dcc.Graph(id='map-graph', figure={}, style={'height': '50vh', 'width':'100%'})
line_chart = dcc.Graph(id='line-chart', figure={}, style={'height': '50vh', 'width':'100%'})

download_button = html.A('Download Data', id='download-button', download='line_chart_data.xlsx', href='', target='_blank')
dropdown = dcc.Dropdown(
    id='dropdown',
    options=[{'label': 'Agricultural water productivity (Awp) [USD/m3]', 'value': 'Awp'},
             {'label': 'Change in agricultural water productivity (cAwp) [%]', 'value': 'cAwp'},
             {'label': 'Trend in agricultural water productivity (tAwp) [%]', 'value': 'tAwp'},               
             {'label': 'Total water consumed by irrigated cropland (Vetb) [Mm3/year]', 'value': 'VEtb'},
             {'label': 'Gross value added by irrigated cropland (GVAa) [USD/year]', 'value': 'GVAa'},
             {'label': 'Area of irrigated crop land (Airr) [km2]', 'value': 'Airr'}],

    value='Awp',  # initial value displayed when page first loads
    clearable=False
)

# Define the layout
# Define the layout
app.layout = dbc.Container([
    
    # Header
    dbc.Row([
        dbc.Col(html.Img(src=app.get_asset_url('header.png'), height="40px", width="100%")),
    ], justify='center', align='center', className='header'),

    dbc.Row([
        dbc.Col(html.Img(src=app.get_asset_url('logo.png'), height="70px"), width=4),
        dbc.Col(html.H2('WaPOR4Awp - Agricultural Water Productivity', style={'text-transform': 'none'}), width=8),
    ], justify='left', align='left', className='header'),

    # # Introduction text
    # dbc.Row([
        # dbc.Col(html.P(''), width=1, className='empty_space'),
        # dbc.Col(html.P(['WaPOR4Awp computes and visualizes agricultural water productivity data for countries in Africa and the Near-East. It supports the monitoring of ',
                        # html.A('SDG indicator 6.4.1', href='https://www.fao.org/3/cb8768en/cb8768en.pdf', target='_blank'),
                        # ' - Change in Water Use Efficiency (CWUE) using remote sensing data. The dashboard offers an alternative approach to estimate water productivity using FAO\'s WaPOR database.',
                        # html.Br(),
                        # html.A('Read more', href='https://www.fao.org/documents/card/en/c/cb8768en', target='_blank'),
                       # ]), width=16, className='address-text'),
    # ], align='left', className='intro_p'),

    # Main content
    dbc.Row([
        dbc.Col(dropdown, width=8, className='text-center'),  
        dbc.Col(html.P(['WaPOR4Awp computes and visualizes agricultural water productivity data for countries in Africa and the Near-East using ',
                html.A('WaPOR data', href='https://wapor.apps.fao.org/catalog/WAPOR_2/1', target='_blank'),
                
             ]), width=4, className='address-text'),
       
              
    ], justify='center', align='center', className='footer'),

    # dbc.Row([
        # dbc.Col(mytitle, width=12, className='text-center'),
    # ], justify='center', align='center', className='header2'),

    dbc.Row([
        dbc.Col(map_graph, width=8, className='text-center'),  
        dbc.Col(line_chart, width=4, className='text-center'),
        dbc.Row([            
            dbc.Col(" ", width=8, className='text-center'),            
            dbc.Col(download_button, width=4, className='text-center'),
            dbc.Row(html.Br(), className='empty_row'),
        ]),
    dbc.Row([   
            dbc.Col(" ", width=8, className='text-center'), 
            dbc.Col(html.P(['WaPOR4Awp supports the monitoring of ',
                html.A('SDG indicator 6.4.1', href='https://www.fao.org/3/cb8768en/cb8768en.pdf', target='_blank'),
                ' - Change in Water Use Efficiency (CWUE). It offers an alternative approach to estimate water use efficiency in the form of water productivity. ',
                #html.Br(),
                html.A('Read more on SDG 6.4.1 methodology', href='https://www.fao.org/documents/card/en/c/cb8768en', target='_blank'),
               ]), width=4, className='address-text'),
            
        ]),
        dbc.Row(html.Br(), className='empty_row'),
             
    ]),   
    
    # Introduction text
    dbc.Row([
        dbc.Row(html.Br(), className='empty_row'),
        dbc.Row(html.Br(), className='empty_row'),
        dbc.Row(html.Br(), className='empty_row'),
        dbc.Row(html.Br(), className='empty_row'),
        dbc.Row(html.Br(), className='empty_row'),
        dbc.Col(html.P(''), width=1, className='empty_space'),

    ], align='left', className='intro_p'),
   

    # Contact us footer
    dbc.Row([
        dbc.Col(html.A('Contact Us', href='mailto:wateraccounting_project@un-ihe.org'), width=12, className='address-text'),
    ], align='right', className='main-content2'),

    # Footer
    dbc.Row([
        dbc.Col(html.Img(src=app.get_asset_url('footer.png'), height="50px", width="100%")),
    ], justify='center', align='center', className='header'),

], fluid=True)#fluid=True, style={'backgroundColor': '#AAD3df'})





# Define the Mapbox access token
mapbox_access_token = "your_mapbox_access_token"

# Define the callback functions
# Define the callback functions

        
@app.callback(
    [Output('map-graph', 'figure'), Output('line-chart', 'figure'), Output("download-button", "href")],
    [Input('map-graph', 'clickData'), Input('dropdown', 'value'), Input('download-button', 'n_clicks')],
    [State('line-chart', 'figure')]
)
def update_graph(click_data, dropdown_value, download_clicks, line_chart_figure):
    column_names = ['Awp', 'cAwp', 'tAwp', 'VEtb', 'GVAa', 'Airr']
    units = {'Awp': 'USD/m³', 'cAwp': '%', 'tAwp': '%', 'VEtb': 'm³', 'GVAa': 'USD', 'Airr': 'km²'}
    filtered_df = df.copy()
    href = ""
    if click_data is not None:
        selected_iso = click_data['points'][0]['location']
        filtered_df = df[df['ISO-3'] == selected_iso]
    if dropdown_value is not None:
        color_column = dropdown_value
    else:
        color_column = column_names[0]
    map_fig = px.choropleth_mapbox(data_frame=df, geojson=countries, locations='ISO-3',
                                   featureidkey='properties.ISO-3',
                                   color_continuous_scale="GnBu", 
                                   height=680, 
                                   width=None,
                                   color=color_column,
                                   animation_frame='year',
                                   hover_data={'country_name': True},
                                   mapbox_style="open-street-map",
                                   center={"lat": 2.7832, "lon": 26.5085},
                                   zoom=1.2,
                                   )
    map_fig.update_layout(legend=dict(        
        yanchor="bottom",
        y=1.0,
        xanchor="left",
        x=0.01
    ), autosize=True, margin=dict(l=50, r=50, t=50, b=50))
    map_fig.update_layout(
        modebar=dict(
            orientation='h',
            bgcolor='rgba(0,0,0,0)',  # make the modebar transparent
            activecolor='#FFFFFF',
            remove=['sendDataToCloud', 'select2d', 'lasso2d', 'resetScale2d', 'toggleSpikelines']
        )
    )

    if click_data is not None:
        line_data = filtered_df.melt(id_vars=['year', 'country_name', 'ISO-3'], value_vars=column_names,
                                     var_name='variable', value_name='value')
        line_fig = px.line(line_data, x='year', y='value', color='variable', line_group='country_name',
                           labels={"variable": "Indicator"},
                           template="simple_white",
                           )
        for trace in line_fig.data:
            trace.update(mode='lines+markers')

        # Apply custom tickformat for different columns
        for column in column_names:
            if column == 'Awp':
                line_fig.update_yaxes(tickformat=".1e", exponentformat='e', selector=dict(variable=column))
            elif column in ['VEtb', 'GVAa', 'Airr']:
                line_fig.update_yaxes(tickformat=",.0f", selector=dict(variable=column))
            else:
                line_fig.update_yaxes(tickformat=",.2f", selector=dict(variable=column))

        line_fig.update_layout(margin=dict(l=50, r=50, t=50, b=50))
    else:
        line_fig = line_chart_figure

    # Check if Download Data button was clicked and filtered_df is not empty
    if download_clicks and not filtered_df.empty:
        # Generate downloadable Excel file
        output = io.BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        filtered_df.to_excel(writer, sheet_name='Sheet1', index=False)
        writer.save()
        output.seek(0)
        href = 'data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;charset=utf-8,' + urllib.parse.quote(
            output.getvalue())

    return [map_fig, line_fig, href or None]





# Run app
if __name__ == '__main__':
    app.run_server(debug=True, port=8056)
