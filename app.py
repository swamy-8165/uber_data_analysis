from flask import Flask, render_template,jsonify
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import json
import os
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)

def create_cassandra_session():
    """Creates a Cassandra session."""
    config = json.loads(f'{{"secure_connect_bundle":"{os.getenv("cloud_config")}"}}')
    auth_provider = PlainTextAuthProvider(
        username=os.getenv("Cassndra_clientId"),
        password=os.getenv("Cassandra_secret")
    )
    cluster = Cluster(cloud=config, auth_provider=auth_provider)
    return cluster.connect()

def fetch_all_data():
    """Fetch all required data from Cassandra and store in DataFrames."""
    session = create_cassandra_session()

    # Fetch fact table data
    fact_query = "SELECT * FROM bigdata_1.fact_table;"
    fact_df = pd.DataFrame(session.execute(fact_query))

    # Fetch dimension tables
    datetime_query = "SELECT * FROM bigdata_1.datetime_dim;"
    datetime_df = pd.DataFrame(session.execute(datetime_query))

    distance_query = "SELECT * FROM bigdata_1.trip_distance_dim;"
    distance_df = pd.DataFrame(session.execute(distance_query))

    passenger_query = "SELECT * FROM bigdata_1.passenger_count_dim;"
    passenger_df = pd.DataFrame(session.execute(passenger_query))

    payment_type_query = "SELECT * FROM bigdata_1.payment_type_dim;"
    payment_type_df = pd.DataFrame(session.execute(payment_type_query))

    session.shutdown()

    return {
        'fact': fact_df,
        'datetime': datetime_df,
        'distance': distance_df,
        'passenger': passenger_df,
        'payment_type': payment_type_df
    }

def get_trip_distance_analysis():
    """Distance analysis with clear categorization"""
    dfs = fetch_all_data()
    fact_df = dfs['fact']
    distance_df = dfs['distance']
    
    merged_df = pd.merge(fact_df, distance_df, on='trip_distance_id')
    
    # Create distance categories
    merged_df['distance_category'] = pd.cut(
        merged_df['trip_distance'],
        bins=[0, 1, 2, 5, 10, float('inf')],
        labels=['0-1 mile', '1-2 miles', '2-5 miles', '5-10 miles', '10+ miles']
    )
    
    # Calculate statistics by category
    distance_stats = merged_df.groupby('distance_category').agg({
        'vendor_id': 'count',
        'total_amount': ['mean', 'std'],
        'trip_distance': 'mean'
    }).reset_index()
    
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Trip Distance Distribution', 
                       'Average Fare by Distance Category'),
        vertical_spacing=0.15
    )
    
    # Bar chart of trip distances by category
    fig.add_trace(
        go.Bar(
            x=distance_stats['distance_category'],
            y=distance_stats[('vendor_id', 'count')],
            name="Number of Trips",
            marker_color='#4F46E5',
            hovertemplate="Category: %{x}<br>" +
                         "Count: %{y}<extra></extra>"
        ),
        row=1, col=1
    )
    
    # Bar chart of average fares
    fig.add_trace(
        go.Bar(
            x=distance_stats['distance_category'],
            y=distance_stats[('total_amount', 'mean')],
            name="Average Fare",
            marker_color='#10B981',
            error_y=dict(
                type='data',
                array=distance_stats[('total_amount', 'std')],
                visible=True
            ),
            hovertemplate="Category: %{x}<br>" +
                         "Avg Fare: $%{y:.2f}<extra></extra>"
        ),
        row=2, col=1
    )
    
    fig.update_layout(
        height=700,
        showlegend=False,
        title={
            'text': "Trip Distance Analysis",
            'y':0.95,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'
        },
        xaxis=dict(showgrid=False),  # Remove x-axis grid for first plot
        yaxis=dict(showgrid=False),  # Remove y-axis grid for first plot
        xaxis2=dict(showgrid=False), # Remove x-axis grid for second plot
        yaxis2=dict(showgrid=False)  # Remove y-axis grid for second plot
    )
    
    # Update axes
    fig.update_xaxes(title_text="Distance Category", row=1, col=1)
    fig.update_xaxes(title_text="Distance Category", row=2, col=1)
    fig.update_yaxes(title_text="Number of Trips", row=1, col=1)
    fig.update_yaxes(title_text="Average Fare ($)", row=2, col=1)
    
    return fig.to_html(full_html=False)

def generate_plots():
    dataframes = fetch_all_data()

    # Join the dataframes
    fact_df = dataframes['fact']
    datetime_df = dataframes['datetime']
    distance_df = dataframes['distance']
    payment_type_df = dataframes['payment_type']

    # Merge for trip and distance analysis
    merged_df = pd.merge(fact_df, datetime_df, on='datetime_id')
    merged_df = pd.merge(merged_df, distance_df, on='trip_distance_id')

    # Calculate count and average of trip_distance per hour
    df_trip = merged_df.groupby('pick_hour').agg(
        trip_count=('trip_distance', 'size'),
        avg_trip_distance=('trip_distance', 'mean')
    ).reset_index()

    # Plot 1: Bar plot for Trip Count
    fig1 = go.Figure()
    fig1.add_trace(
        go.Bar(x=df_trip['pick_hour'], y=df_trip['trip_count'], name='Trip Count', marker_color='blue')
    )
    fig1.update_layout(
        title='Hourly Trip Count',
        xaxis_title='Hour of Day',
        yaxis_title='Trip Count',
        legend=dict(x=0.5, y=-0.2, orientation='h', xanchor='center'),
        height=400,
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=False)
    )

    # Plot 2: Line plot for Average Trip Distance
    fig2 = go.Figure()
    fig2.add_trace(
        go.Scatter(x=df_trip['pick_hour'], y=df_trip['avg_trip_distance'], name='Avg Trip Distance', mode='lines+markers', line=dict(color='green'))
    )
    fig2.update_layout(
        title='Hourly Average Trip Distance',
        xaxis_title='Hour of Day',
        yaxis_title='Avg Trip Distance (miles)',
        legend=dict(x=0.5, y=-0.2, orientation='h', xanchor='center'),
        height=400,
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=False)
    )

    # Hourly analysis on tip_amount
    df_hourly_tip_amount = merged_df.groupby('pick_hour').agg(
        tip_count=('tip_amount', 'sum'),
        avg_tip_amount=('tip_amount', 'mean')
    ).reset_index()

    # Plot 3: Bar chart for Tip Count
    fig3 = go.Figure()
    fig3.add_trace(
        go.Bar(x=df_hourly_tip_amount['pick_hour'], y=df_hourly_tip_amount['tip_count'], name='Tip Count', marker_color='orange')
    )
    fig3.update_layout(
        title='Hourly Tip Count',
        xaxis_title='Hour of Day',
        yaxis_title='Tip Count',
        legend=dict(x=0.5, y=-0.2, orientation='h', xanchor='center'),
        height=400,
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=False)
    )

    # Plot 4: Line chart for Average Amount
    fig4 = go.Figure()
    fig4.add_trace(
        go.Scatter(x=df_hourly_tip_amount['pick_hour'], y=df_hourly_tip_amount['avg_tip_amount'], name='Avg Amount', mode='lines+markers', line=dict(color='purple'))
    )
    fig4.update_layout(
        title='Hourly Average Tip Amount',
        xaxis_title='Hour of Day',
        yaxis_title='Avg Amount ($)',
        legend=dict(x=0.5, y=-0.2, orientation='h', xanchor='center'),
        height=400,
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=False)
    )

    merged_payment_df = pd.merge(fact_df, payment_type_df, on='payment_type_id')

    # Group by payment type and calculate the total amount
    total_amount_by_payment_type = merged_payment_df.groupby('payment_type_desc')['total_amount'].sum()

    # Plot: Pie chart for Total Amount by Payment Type
    fig5 = go.Figure(data=[go.Pie(labels=total_amount_by_payment_type.index, values=total_amount_by_payment_type.values,hole=0.4)])
    fig5.update_layout(
        title='Total Amount by Payment Type',
        legend=dict(x=0.5, y=-0.2, orientation='h', xanchor='center'),
        height=400
    )

    # Convert plots to HTML
    plot1_html = fig1.to_html(full_html=False)
    plot2_html = fig2.to_html(full_html=False)
    plot3_html = fig3.to_html(full_html=False)
    plot4_html = fig4.to_html(full_html=False)
    plot5_html = fig5.to_html(full_html=False)
    plot6_html = get_trip_distance_analysis()

    return plot1_html, plot2_html, plot3_html, plot4_html, plot5_html, plot6_html
@app.route('/api/summary_stats')
def get_summary_stats():
    dfs = fetch_all_data()
    fact_df = dfs['fact']
    distance_df = dfs['distance']
    passenger_df = dfs['passenger']
    
    # Calculate summary statistics
    total_trips = len(fact_df)
    total_revenue = fact_df['total_amount'].sum()
    avg_distance = distance_df['trip_distance'].mean()
    
    return jsonify({
        'total_trips': f"{total_trips:,}",
        'total_revenue': f"${total_revenue:,.2f}",
        'avg_distance': f"{avg_distance:.2f} miles",
    })
@app.route('/')
def index():
    plot1_html, plot2_html, plot3_html, plot4_html, plot5_html, plot6_html = generate_plots()
    return render_template('index.html', plot1=plot1_html, plot2=plot2_html, 
                           plot3=plot3_html, plot4=plot4_html, plot5=plot5_html, plot6=plot6_html)




if __name__ == '__main__':
    app.run(debug=True)