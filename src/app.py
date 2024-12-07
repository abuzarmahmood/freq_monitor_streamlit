import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from glob import glob
import os
import time
import threading
import atexit
import boto3
import io
from botocore.exceptions import ClientError


st.set_page_config(page_title="Frequency Monitor", layout="wide")
st.title("Real-time Frequency Monitoring")

# AWS Configuration
st.sidebar.header("Data Source Configuration")
use_s3 = st.sidebar.checkbox("Use AWS S3")

if use_s3:
    s3_bucket = st.secrets['S3_BUCKET_NAME']
    s3_prefix = "recent_data/"

# Get data directory
script_path = os.path.abspath(__file__)
src_dir = os.path.dirname(script_path)
base_dir = os.path.dirname(src_dir)
artifacts_dir = os.path.join(base_dir, 'artifacts')
recent_data_dir = os.path.join(artifacts_dir, 'recent_data')

def get_s3_client():
    """Create and return an S3 client using provided credentials"""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=st.secrets["S3_KEY"],
            aws_secret_access_key=st.secrets["S3_SECRET"]
        )
        return s3_client
    except Exception as e:
        st.error(f"Failed to create S3 client: {str(e)}")
        return None

def load_s3_data(device_num):
    """Load data from S3 bucket"""
    s3_client = get_s3_client()
    if not s3_client:
        return None, None
    
    try:
        # Construct S3 paths
        # st.write(f'Looking for data in bucket {s3_bucket} with prefix {s3_prefix}')
        data_key = s3_prefix + f"recent_data_device_{device_num}.csv"
        bounds_key = s3_prefix + f"freq_bounds_device_{device_num}.csv"
        # st.write(f"Data key: {data_key}")
        # st.write(f"Bounds key: {bounds_key}")
        
        # Get data file
        response = s3_client.get_object(Bucket=s3_bucket, Key=data_key)
        data = pd.read_csv(io.BytesIO(response['Body'].read()))
        data['time'] = pd.to_datetime(data['time'])
        
        # Try to get bounds file
        try:
            bounds_response = s3_client.get_object(Bucket=s3_bucket, Key=bounds_key)
            bounds = pd.read_csv(io.BytesIO(bounds_response['Body'].read()))
        except ClientError:
            bounds = None
            
        return data, bounds
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None, None
        else:
            st.error(f"Error accessing S3: {str(e)}")
            return None, None
    except Exception as e:
        st.error(f"Error loading S3 data: {str(e)}")
        return None, None

# Function to load and process data
def load_data(device_num):
    if use_s3:
        return load_s3_data(device_num)
    
    try:
        recent_data_file = os.path.join(recent_data_dir, f'recent_data_device_{device_num}.csv')
        bounds_file = os.path.join(recent_data_dir, f'freq_bounds_device_{device_num}.csv')
        
        if not os.path.exists(recent_data_file):
            return None, None
        
        data = pd.read_csv(recent_data_file)
        data['time'] = pd.to_datetime(data['time'])
        
        bounds = None
        if os.path.exists(bounds_file):
            bounds = pd.read_csv(bounds_file)
        
        return data, bounds
    except Exception as e:
        st.error(f"Error loading data for device {device_num}: {str(e)}")
        return None, None

# Function to create plot
def create_plot(data, bounds, device_num):
    fig = go.Figure()
    
    # Add raw frequency data
    fig.add_trace(go.Scatter(
        x=data['time'],
        y=data['freq'],
        mode='lines+markers',
        name='Raw Frequency',
        line=dict(color='blue', width=1),
        marker=dict(size=4)
    ))
    
    # Add filtered frequency data
    fig.add_trace(go.Scatter(
        x=data['time'],
        y=data['freq_filtered'],
        mode='lines',
        name='Filtered Frequency',
        line=dict(color='red', width=2)
    ))
    
    # Add bound fills if bounds exist
    if bounds is not None:
        min_freq = bounds['min_freq'].iloc[0]
        max_freq = bounds['max_freq'].iloc[0]
        
        # Add bound lines
        fig.add_hline(y=min_freq, line_dash="dash", line_color="red", name="Lower Bound")
        fig.add_hline(y=max_freq, line_dash="dash", line_color="red", name="Upper Bound")
        
        # Add fills
        y_range = [data['freq_filtered'].min(), data['freq_filtered'].max()]
        
        # Fill between bounds (green)
        fig.add_hrect(y0=min_freq, y1=max_freq,
                     fillcolor="lightgreen", opacity=0.2,
                     layer="below", name="Normal Range")
        
        # Fill below min (red)
        fig.add_hrect(y0=y_range[0], y1=min_freq,
                     fillcolor="red", opacity=0.2,
                     layer="below", name="Below Range")
        
        # Fill above max (red)
        fig.add_hrect(y0=max_freq, y1=y_range[1],
                     fillcolor="red", opacity=0.2,
                     layer="below", name="Above Range")
    
    # Set y-axis limits from bounds if available
    y_min = None
    y_max = None
    if bounds is not None and not bounds.empty:
        if 'y_min' in bounds.columns and 'y_max' in bounds.columns:
            y_min = bounds['y_min'].iloc[0]
            y_max = bounds['y_max'].iloc[0]

    fig.update_layout(
        title=f"Device {device_num} Frequency Data",
        xaxis_title="Time",
        yaxis_title="Frequency (RPM)",
        height=400,
        showlegend=True,
        yaxis=dict(
            range=[y_min, y_max] if y_min is not None and y_max is not None else None
        )
    )
    
    return fig

# Main app layout
st.sidebar.header("Controls")

# Auto-refresh interval
refresh_interval = st.sidebar.slider(
    "Refresh Interval (seconds)",
    min_value=1,
    max_value=60,
    value=1
)

# Delay threshold
delay_threshold = st.sidebar.slider(
    "Delay Threshold (seconds)",
    min_value=1,
    max_value=300,
    value=60
)

# Get available devices
if use_s3:
    s3_client = get_s3_client()
    if s3_client:
        try:
            response = s3_client.list_objects_v2(
                Bucket=s3_bucket,
                Prefix=f"{s3_prefix.rstrip('/')}/recent_data_device_"
            )
            device_files = [obj['Key'] for obj in response.get('Contents', [])]
            device_numbers = [int(f.split('_')[-1].split('.')[0]) for f in device_files]
        except Exception as e:
            st.error(f"Error listing S3 objects: {str(e)}")
            device_numbers = []
    else:
        device_numbers = []
else:
    device_files = glob(os.path.join(recent_data_dir, 'recent_data_device_*.csv'))
    device_numbers = [int(f.split('_')[-1].split('.')[0]) for f in device_files]

audio_elements = []
if not device_numbers:
    st.warning("No device data found in the recent data directory.")
else:
    # Create columns for metrics
    cols = st.columns(len(device_numbers))
    
    # Create plots for each device
    for i, device_num in enumerate(device_numbers):
        try:
            data, bounds = load_data(device_num)
            
            if data is not None and not data.empty:
                with cols[i]:
                    try:
                        # Display current frequency and delay
                        current_freq = data['freq_filtered'].iloc[-1]
                        last_time = data['time'].iloc[-1]
                        timezone = bounds['timezone'].iloc[0]
                        last_time = pd.to_datetime(last_time).tz_localize(timezone)
                        delay = (
                                pd.Timestamp.now(tz = timezone) - last_time 
                                ).total_seconds()
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric(
                                f"Device {device_num} Current Frequency",
                                f"{current_freq:.1f} RPM"
                            )
                        with col2:
                            st.metric(
                                "Delay",
                                f"{delay:.1f} seconds"
                            )
                        
                        # Check if frequency is within bounds
                        if bounds is not None and not bounds.empty:
                            min_freq = bounds['min_freq'].iloc[0]
                            max_freq = bounds['max_freq'].iloc[0]
                            freq_out_of_bounds = current_freq < min_freq or current_freq > max_freq
                            delay_too_large = delay > delay_threshold
                            
                            if freq_out_of_bounds or delay_too_large:
                                if freq_out_of_bounds:
                                    st.error("⚠️ Frequency out of bounds!")
                                if delay_too_large:
                                    st.error(f"⚠️ Delay exceeds threshold ({delay:.1f}s > {delay_threshold}s)!")
                                if len(audio_elements) == 0: # Only play audio once 
                                    this_audio = st.audio(os.path.join(artifacts_dir, 'warning.wav'),
                                         autoplay=True, loop=True)
                                    audio_elements.append(this_audio)
                                elif len(audio_elements) > 0: 
                                    pass
                            else:
                                st.success("✅ Frequency within bounds")
                        
                        # Create and display plot
                        fig = create_plot(data, bounds, device_num)
                        st.plotly_chart(fig, use_container_width=True)
                    except Exception as e:
                        st.error(f"Error processing data for Device {device_num}: {str(e)}")
            else:
                with cols[i]:
                    st.warning(f"No valid data available for Device {device_num}")
        except Exception as e:
            with cols[i]:
                st.error(f"Error processing Device {device_num}: {str(e)}")
    
    # Auto-refresh
    time.sleep(refresh_interval)
    # st.experimental_rerun()
    st.rerun()
