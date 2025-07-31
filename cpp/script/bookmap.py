import pandas as pd
import numpy as np
import plotly.graph_objects as go
import os
from datetime import datetime, time

class CleanBookmapVisualizer:
    def __init__(self, min_order_size: int = 20):
        self.min_order_size = min_order_size
    
    def load_and_clean_data(self, filepath: str) -> pd.DataFrame:
        """Load and clean L2 data with proper time filtering"""
        # Load data with GBK encoding
        df = pd.read_csv(filepath, encoding='gbk')
        
        # Standardize column names
        expected_cols = [
            '市场代码', '证券代码', '时间', '最新价', '成交笔数', '成交额', '成交量', '方向',
            '买一价', '买二价', '买三价', '买四价', '买五价',
            '卖一价', '卖二价', '卖三价', '卖四价', '卖五价',
            '买一量', '买二量', '买三量', '买四量', '买五量',
            '卖一量', '卖二量', '卖三量', '卖四量', '卖五量'
        ]
        
        if len(df.columns) == len(expected_cols):
            df.columns = expected_cols
        
        # Convert time and numeric columns
        df['时间'] = pd.to_datetime(df['时间'])
        
        # Filter to market hours only (9:25-15:00)
        market_start = time(9, 25)
        market_end = time(15, 0)
        df = df[
            (df['时间'].dt.time >= market_start) & 
            (df['时间'].dt.time <= market_end)
        ].copy()
        
        # Convert numeric columns
        numeric_cols = ['最新价', '成交笔数', '成交额', '成交量'] + \
                      ['买一价', '买二价', '买三价', '买四价', '买五价'] + \
                      ['卖一价', '卖二价', '卖三价', '卖四价', '卖五价'] + \
                      ['买一量', '买二量', '买三量', '买四量', '买五量'] + \
                      ['卖一量', '卖二量', '卖三量', '卖四量', '卖五量']
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df.reset_index(drop=True)
    
    def calculate_coordinates(self, df: pd.DataFrame):
        """Pre-calculate all coordinates for unified plotting"""
        n_points = len(df)
        
        # Time axis: use data point indices (0 to n_points-1)
        time_coords = np.arange(n_points)
        
        # Daily price range (actual min/max of prices)
        daily_price_min = df['最新价'].min()
        daily_price_max = df['最新价'].max()
        daily_price_range = daily_price_max - daily_price_min
        
        # Chart dimensions with blank spaces
        # Main chart uses original data points width
        main_chart_width = n_points
        # Extend horizontal axis by 40% for volume profile (20% buy + 20% sell)
        volume_profile_width = n_points * 0.4  # 40% additional width for volume profile
        total_chart_width = main_chart_width + volume_profile_width
        volume_profile_x_start = main_chart_width
        volume_profile_center = main_chart_width + (volume_profile_width / 2)  # Center line for volume alignment
        
        # Y-axis layout with blank spaces
        # 20% blank upwards, 30% blank downwards (10% separation + 20% volume bars)
        blank_upwards = daily_price_range * 0.2
        separation_space = daily_price_range * 0.1
        volume_bar_space = daily_price_range * 0.2
        
        # Calculate extended price range
        extended_price_max = daily_price_max + blank_upwards
        extended_price_min = daily_price_min - separation_space - volume_bar_space
        extended_price_range = extended_price_max - extended_price_min
        
        # Volume bar positioning
        volume_bar_y_base = daily_price_min - separation_space - volume_bar_space
        volume_bar_y_top = daily_price_min - separation_space
        volume_bar_height = volume_bar_space
        
        coords = {
            'n_points': n_points,
            'time_coords': time_coords,
            'daily_price_min': daily_price_min,
            'daily_price_max': daily_price_max,
            'daily_price_range': daily_price_range,
            'extended_price_min': extended_price_min,
            'extended_price_max': extended_price_max,
            'extended_price_range': extended_price_range,
            'total_chart_width': total_chart_width,
            'main_chart_width': main_chart_width,
            'volume_profile_width': volume_profile_width,
            'volume_profile_x_start': volume_profile_x_start,
            'volume_profile_center': volume_profile_center,
            'volume_bar_height': volume_bar_height,
            'volume_bar_y_base': volume_bar_y_base,
            'volume_bar_y_top': volume_bar_y_top
        }
        
        return coords
    
    def create_support_resistance_heatmap(self, df: pd.DataFrame, coords: dict):
        """Create support/resistance heatmap shapes"""
        chinese_nums = ['一', '二', '三', '四', '五']
        shapes = []
        
        # Calculate daily max order size for normalization
        max_order_size = 0
        for num in chinese_nums:
            for side in ['买', '卖']:
                volume_col = f'{side}{num}量'
                if volume_col in df.columns:
                    col_max = df[volume_col].max()
                    if not pd.isna(col_max):
                        max_order_size = max(max_order_size, col_max)
        max_order_size = max(max_order_size, 100)
        
        # Create limited number of heatmap shapes for performance
        # Sample data points and price levels to limit shapes
        max_time_samples = 60*4
        max_price_samples = 100
        
        time_sample_step = max(1, coords['n_points'] // max_time_samples)
        time_samples = range(0, coords['n_points'], time_sample_step)
        
        # Use larger price step for better performance
        price_step = max(0.01, coords['daily_price_range'] / max_price_samples)
        price_bins = np.arange(coords['daily_price_min'], coords['daily_price_max'] + price_step, price_step)
        
        # Only process sampled time points
        for t_idx in time_samples:
            row = df.iloc[t_idx]
            
            # Process bid levels
            for num in chinese_nums:
                price_col = f'买{num}价'
                volume_col = f'买{num}量'
                if price_col in df.columns and volume_col in df.columns:
                    price = row[price_col]
                    volume = row[volume_col]
                    if not pd.isna(price) and not pd.isna(volume) and volume >= self.min_order_size:
                        # Find closest price bin
                        price_idx = int((price - coords['daily_price_min']) / price_step)
                        if 0 <= price_idx < len(price_bins):
                            opacity = min(0.8, volume / max_order_size)
                            shapes.append({
                                'type': 'rect',
                                'x0': t_idx - time_sample_step/2,
                                'x1': t_idx + time_sample_step/2,
                                'y0': price - price_step/2,
                                'y1': price + price_step/2,
                                'fillcolor': f'rgba(0,255,0,{opacity})',
                                'line': dict(width=0),
                                'layer': 'below'
                            })
            
            # Process ask levels
            for num in chinese_nums:
                price_col = f'卖{num}价'
                volume_col = f'卖{num}量'
                if price_col in df.columns and volume_col in df.columns:
                    price = row[price_col]
                    volume = row[volume_col]
                    if not pd.isna(price) and not pd.isna(volume) and volume >= self.min_order_size:
                        # Find closest price bin
                        price_idx = int((price - coords['daily_price_min']) / price_step)
                        if 0 <= price_idx < len(price_bins):
                            opacity = min(0.8, volume / max_order_size)
                            shapes.append({
                                'type': 'rect',
                                'x0': t_idx - time_sample_step/2,
                                'x1': t_idx + time_sample_step/2,
                                'y0': price - price_step/2,
                                'y1': price + price_step/2,
                                'fillcolor': f'rgba(255,0,0,{opacity})',
                                'line': dict(width=0),
                                'layer': 'below'
                            })
        
        return shapes
    
    def create_vertical_volume_bars(self, df: pd.DataFrame, coords: dict):
        """Create vertical volume bar shapes - consistent with trade bubble granularity"""
        shapes = []
        
        # Use same tick-by-tick calculation as trade bubbles for consistency
        volume_diff = df['成交量'].diff()
        price_diff = df['最新价'].diff()
        
        # Filter for meaningful volume changes (same threshold as bubbles)
        significant_volume_mask = (volume_diff > 50) & ~volume_diff.isna()
        
        if not significant_volume_mask.any():
            return shapes
        
        # Get significant volume data
        significant_indices = coords['time_coords'][significant_volume_mask]
        significant_volumes = volume_diff[significant_volume_mask]
        significant_price_changes = price_diff[significant_volume_mask]
        
        # Aggregate nearby trades to reduce visual clutter
        aggregation_window = max(1, coords['n_points'] // 200)  # Aggregate within small windows
        aggregated_data = []
        
        i = 0
        while i < len(significant_indices):
            window_start = significant_indices[i]
            window_volume = 0
            window_price_change = 0
            window_end = window_start
            
            # Aggregate all trades within the window
            while i < len(significant_indices) and significant_indices[i] <= window_start + aggregation_window:
                window_volume += significant_volumes.iloc[i]
                window_price_change += significant_price_changes.iloc[i]
                window_end = significant_indices[i]
                i += 1
            
            window_center = (window_start + window_end) / 2
            aggregated_data.append((window_center, window_volume, window_price_change))
        
        if not aggregated_data:
            return shapes
        
        # Extract aggregated data
        x_positions = [data[0] for data in aggregated_data]
        volumes = [data[1] for data in aggregated_data]
        price_changes = [data[2] for data in aggregated_data]
        
        # Normalize heights
        max_volume = max(volumes)
        bar_width = aggregation_window * 0.8
        
        for x_pos, volume, price_change in aggregated_data:
            height = (volume / max_volume) * coords['volume_bar_height']
            color = 'green' if price_change >= 0 else 'red'
            
            shapes.append({
                'type': 'rect',
                'x0': x_pos - bar_width/2,
                'x1': x_pos + bar_width/2,
                'y0': coords['volume_bar_y_base'],
                'y1': coords['volume_bar_y_base'] + height,
                'fillcolor': color,
                'opacity': 0.7,
                'line': dict(width=0)
            })
        
        return shapes
    
    def create_horizontal_volume_profile(self, df: pd.DataFrame, coords: dict):
        """Create horizontal volume profile shapes - using exact execution prices"""
        shapes = []
        
        # Collect volumes at exact execution prices (no binning)
        price_volumes = {}  # execution_price -> {'buy': volume, 'sell': volume}
        
        for i in range(1, coords['n_points']):
            volume_delta = df.iloc[i]['成交量'] - df.iloc[i-1]['成交量']
            price_change = df.iloc[i]['最新价'] - df.iloc[i-1]['最新价']
            
            if pd.isna(volume_delta) or volume_delta <= 0:
                continue
            
            # Use same execution price logic as trade bubbles
            if price_change > 0:
                # Active BUY = executed at ASK price
                if '卖一价' in df.columns and not pd.isna(df['卖一价'].iloc[i]):
                    execution_price = df['卖一价'].iloc[i]
                else:
                    execution_price = df['最新价'].iloc[i]
            else:
                # Active SELL = executed at BID price  
                if '买一价' in df.columns and not pd.isna(df['买一价'].iloc[i]):
                    execution_price = df['买一价'].iloc[i]
                else:
                    execution_price = df['最新价'].iloc[i]
            
            # Round to 0.01 precision for grouping
            rounded_price = round(execution_price, 2)
            
            # Initialize if not seen before
            if rounded_price not in price_volumes:
                price_volumes[rounded_price] = {'buy': 0, 'sell': 0}
            
            # Add volume to appropriate category
            if price_change >= 0:
                price_volumes[rounded_price]['buy'] += volume_delta
            else:
                price_volumes[rounded_price]['sell'] += volume_delta
        
        # Create separate volume profile bars for active buy and active sell
        if not price_volumes:
            return shapes
            
        # Find max volume for scaling
        max_buy_volume = max((vol['buy'] for vol in price_volumes.values()), default=0)
        max_sell_volume = max((vol['sell'] for vol in price_volumes.values()), default=0)
        max_volume = max(max_buy_volume, max_sell_volume)
        
        if max_volume == 0:
            return shapes
        
        # Each half gets 20% of the main chart width (half of the 40% volume profile area)
        max_bar_width = coords['volume_profile_width'] / 2 * 0.9  # 90% of half-width for bars
        price_step = 0.01  # Height of each volume bar
        
        for execution_price, volumes in price_volumes.items():
            buy_vol = volumes['buy']
            sell_vol = volumes['sell']
            
            # Active BUY volume bar (green, extends right from center)
            if buy_vol > 0:
                bar_width = (buy_vol / max_volume) * max_bar_width
                shapes.append({
                    'type': 'rect',
                    'x0': coords['volume_profile_center'],
                    'x1': coords['volume_profile_center'] + bar_width,
                    'y0': execution_price - price_step/2,
                    'y1': execution_price + price_step/2,
                    'fillcolor': 'green',
                    'opacity': 0.7,
                    'line': dict(width=0)
                })
            
            # Active SELL volume bar (red, extends left from center)
            if sell_vol > 0:
                bar_width = (sell_vol / max_volume) * max_bar_width
                shapes.append({
                    'type': 'rect',
                    'x0': coords['volume_profile_center'] - bar_width,
                    'x1': coords['volume_profile_center'],
                    'y0': execution_price - price_step/2,
                    'y1': execution_price + price_step/2,
                    'fillcolor': 'red',
                    'opacity': 0.7,
                    'line': dict(width=0)
                })
        
        return shapes
    
    def create_bookmap(self, df: pd.DataFrame, symbol: str) -> go.Figure:
        """Create clean bookmap with pre-calculated coordinates"""
        # Calculate all coordinates
        coords = self.calculate_coordinates(df)
        
        # Create figure
        fig = go.Figure()
        
        # Add support/resistance heatmap shapes
        heatmap_shapes = self.create_support_resistance_heatmap(df, coords)
        
        # Add vertical volume bar shapes
        volume_bar_shapes = self.create_vertical_volume_bars(df, coords)
        
        # Add horizontal volume profile shapes
        volume_profile_shapes = self.create_horizontal_volume_profile(df, coords)
        
        # Combine all shapes
        all_shapes = heatmap_shapes + volume_bar_shapes + volume_profile_shapes
        
        # Add best bid/ask lines
        if '买一价' in df.columns:
            fig.add_trace(go.Scatter(
                x=coords['time_coords'],
                y=df['买一价'],
                mode='lines',
                line=dict(color='lime', width=1),
                name='Best Bid',
                showlegend=False
            ))
        
        if '卖一价' in df.columns:
            fig.add_trace(go.Scatter(
                x=coords['time_coords'],
                y=df['卖一价'],
                mode='lines',
                line=dict(color='red', width=1),
                name='Best Ask',
                showlegend=False
            ))
        
        # Add VWAP line
        vwap_line = self.calculate_vwap(df, coords)
        if vwap_line:
            fig.add_trace(vwap_line)
        
        # Add CVD plot
        cvd_line, cvd_zero_line = self.calculate_cvd(df, coords)
        if cvd_line:
            fig.add_trace(cvd_line)
        if cvd_zero_line:
            fig.add_trace(cvd_zero_line)
        
        # Add trade bubbles
        trade_bubbles = self.create_trade_bubbles(df, coords)
        if trade_bubbles:
            fig.add_trace(trade_bubbles)
        
        # Update layout
        fig.update_layout(
            title=dict(
                text=f'Level 2 Market Depth - {symbol}',
                font=dict(color='white', size=16)
            ),
            paper_bgcolor='#1e1e1e',
            plot_bgcolor='#2d2d2d',
            font=dict(color='white'),
            height=800,
            showlegend=True,
            legend=dict(
                x=0.02,
                y=0.98,
                bgcolor='rgba(0,0,0,0.5)',
                font=dict(color='white')
            ),
            shapes=all_shapes,
            xaxis=dict(
                title=dict(text='Time (Data Points)', font=dict(color='white')),
                range=[0, coords['total_chart_width']],
                gridcolor='#404040',
                tickfont=dict(color='white')
            ),
            yaxis=dict(
                title=dict(text='Price', font=dict(color='white')),
                range=[coords['extended_price_min'], coords['extended_price_max']],
                gridcolor='#404040',
                tickfont=dict(color='white')
            )
        )
        
        return fig
    
    def create_trade_bubbles(self, df: pd.DataFrame, coords: dict):
        """Create trade bubble scatter positioned at actual execution prices (bid/ask)"""
        # Calculate volume changes
        volume_diff = df['成交量'].diff()
        price_diff = df['最新价'].diff()
        
        # Filter significant trades
        significant_mask = (volume_diff > 50) & ~volume_diff.isna()
        
        if not significant_mask.any():
            return None
        
        # Get trade data
        trade_indices = coords['time_coords'][significant_mask]
        trade_volumes = volume_diff[significant_mask]
        trade_price_changes = price_diff[significant_mask]
        
        # Position bubbles at actual execution prices
        # Active BUY orders execute at ASK price (卖一价) - taking liquidity from sellers
        # Active SELL orders execute at BID price (买一价) - taking liquidity from buyers
        execution_prices = []
        trade_types = []
        
        for i, idx in enumerate(trade_indices):
            price_change = trade_price_changes.iloc[i]
            
            if price_change > 0:
                # Positive price change = Active BUY = executed at ASK price
                if '卖一价' in df.columns and not pd.isna(df['卖一价'].iloc[idx]):
                    execution_price = df['卖一价'].iloc[idx]
                    trade_type = 'Active BUY'
                else:
                    execution_price = df['最新价'].iloc[idx]
                    trade_type = 'BUY (est.)'
            else:
                # Negative price change = Active SELL = executed at BID price  
                if '买一价' in df.columns and not pd.isna(df['买一价'].iloc[idx]):
                    execution_price = df['买一价'].iloc[idx]
                    trade_type = 'Active SELL'
                else:
                    execution_price = df['最新价'].iloc[idx]
                    trade_type = 'SELL (est.)'
            
            execution_prices.append(execution_price)
            trade_types.append(trade_type)
        
        # Size and color bubbles
        max_volume = trade_volumes.max()
        sizes = (trade_volumes / max_volume) * 20 + 5  # Size 5-25
        colors = ['green' if pc > 0 else 'red' for pc in trade_price_changes]
        
        return go.Scatter(
            x=trade_indices,
            y=execution_prices,
            mode='markers',
            marker=dict(
                size=sizes,
                color=colors,
                opacity=0.8,
                line=dict(width=1, color='white')
            ),
            name='Trades',
            showlegend=False,
            hovertemplate='%{customdata[1]}<br>Execution Price: %{y}<br>Volume: %{text}<br>Price Change: %{customdata[0]}<extra></extra>',
            text=trade_volumes,
            customdata=list(zip(trade_price_changes, trade_types))
        )
    
    def calculate_vwap(self, df: pd.DataFrame, coords: dict):
        """Calculate and create VWAP (Volume Weighted Average Price) line"""
        # Calculate volume deltas (actual traded volumes)
        volume_diff = df['成交量'].diff()
        
        # Initialize arrays for VWAP calculation
        cumulative_pv = 0  # cumulative price * volume
        cumulative_volume = 0  # cumulative volume
        vwap_values = []
        valid_indices = []
        
        for i in range(len(df)):
            current_price = df['最新价'].iloc[i]
            
            if i == 0:
                # First data point - no volume change yet
                vwap_values.append(current_price)
                valid_indices.append(i)
                continue
            
            volume_delta = volume_diff.iloc[i]
            
            # Only include meaningful volume changes
            if pd.notna(volume_delta) and volume_delta > 0:
                cumulative_pv += current_price * volume_delta
                cumulative_volume += volume_delta
            
            # Calculate VWAP
            if cumulative_volume > 0:
                vwap = cumulative_pv / cumulative_volume
                vwap_values.append(vwap)
                valid_indices.append(i)
            else:
                # If no volume yet, use current price
                vwap_values.append(current_price)
                valid_indices.append(i)
        
        if not vwap_values:
            return None
        
        # Create VWAP line trace
        return go.Scatter(
            x=[coords['time_coords'][i] for i in valid_indices],
            y=vwap_values,
            mode='lines',
            line=dict(
                color='yellow',
                width=2,
                dash='dash'
            ),
            name='VWAP',
            showlegend=True,
            hovertemplate='VWAP: %{y:.3f}<extra></extra>'
        )
    
    def calculate_cvd(self, df: pd.DataFrame, coords: dict):
        """Calculate and create CVD (Cumulative Volume Delta) line"""
        # Calculate volume deltas and price changes
        volume_diff = df['成交量'].diff()
        price_diff = df['最新价'].diff()
        
        # Initialize CVD calculation
        cumulative_delta = 0
        cvd_values = []
        valid_indices = []
        
        for i in range(len(df)):
            if i == 0:
                # First data point
                cvd_values.append(0)
                valid_indices.append(i)
                continue
                
            volume_delta = volume_diff.iloc[i]
            price_change = price_diff.iloc[i]
            
            # Only include meaningful volume changes
            if pd.notna(volume_delta) and pd.notna(price_change) and volume_delta > 0:
                if price_change > 0:
                    # Price up = buying pressure (positive CVD)
                    cumulative_delta += volume_delta
                elif price_change < 0:
                    # Price down = selling pressure (negative CVD)
                    cumulative_delta -= volume_delta
                # price_change == 0: neutral, no CVD change
            
            cvd_values.append(cumulative_delta)
            valid_indices.append(i)
        
        if not cvd_values or all(v == 0 for v in cvd_values):
            return None, None
        
        # Scale CVD to fit in the volume bar area (below main chart)
        cvd_range = max(cvd_values) - min(cvd_values)
        if cvd_range == 0:
            return None, None
            
        # Map CVD to volume bar area coordinates
        cvd_scaled = []
        for cvd in cvd_values:
            # Normalize CVD to 0-1 range
            normalized = (cvd - min(cvd_values)) / cvd_range
            # Scale to volume bar area (using 80% of the height)
            scaled_y = coords['volume_bar_y_base'] + (normalized * coords['volume_bar_height'] * 0.8)
            cvd_scaled.append(scaled_y)
        
        # Calculate zero line position
        zero_position = min(cvd_values)
        zero_normalized = (0 - zero_position) / cvd_range if 0 >= min(cvd_values) and 0 <= max(cvd_values) else None
        
        # Create CVD line trace
        cvd_line = go.Scatter(
            x=[coords['time_coords'][i] for i in valid_indices],
            y=cvd_scaled,
            mode='lines',
            line=dict(
                color='cyan',
                width=2
            ),
            name='CVD',
            showlegend=True,
            hovertemplate='CVD: %{customdata:.0f}<br>Scaled Y: %{y:.2f}<extra></extra>',
            customdata=cvd_values
        )
        
        # Create zero line if CVD crosses zero
        cvd_zero_line = None
        if zero_normalized is not None:
            zero_y = coords['volume_bar_y_base'] + (zero_normalized * coords['volume_bar_height'] * 0.8)
            cvd_zero_line = go.Scatter(
                x=[0, coords['main_chart_width']],
                y=[zero_y, zero_y],
                mode='lines',
                line=dict(
                    color='white',
                    width=1,
                    dash='dot'
                ),
                name='CVD Zero',
                showlegend=False,
                hovertemplate='CVD Zero Line<extra></extra>'
            )
        
        return cvd_line, cvd_zero_line
    
    def process_file(self, filepath: str):
        """Process a single file"""
        # Extract symbol from filename
        filename = os.path.basename(filepath)
        symbol = filename.split('_')[0] if '_' in filename else filename.replace('.csv', '')
        
        print(f"Processing {filename}...")
        
        # Load and clean data
        df = self.load_and_clean_data(filepath)
        
        if len(df) == 0:
            print(f"No valid data found in {filename}")
            return
        
        print(f"Loaded {len(df)} records for {symbol}")
        print(f"Time range: {df['时间'].min()} to {df['时间'].max()}")
        
        # Create visualization
        fig = self.create_bookmap(df, symbol)
        
        # Save and show
        output_file = f"{filepath}/bookmap_{symbol}_{df['时间'].iloc[0].strftime('%Y%m%d')}.html"
        fig.write_html(output_file)
        print(f"Saved to {output_file}")
        
        fig.show()
    
    def process_all_files(self, data_dir: str = "sample L2 snapshot"):
        """Process all CSV files"""
        if not os.path.exists(data_dir):
            print(f"Directory {data_dir} not found!")
            return
        
        csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
        
        if not csv_files:
            print(f"No CSV files found in {data_dir}")
            return
        
        print(f"Found {len(csv_files)} CSV files")
        
        for csv_file in csv_files:
            try:
                filepath = os.path.join(data_dir, csv_file)
                self.process_file(filepath)
                print()  # Empty line between files
            except Exception as e:
                print(f"Error processing {csv_file}: {str(e)}")

def main():
    print("Clean Bookmap Visualizer")
    print("=" * 50)
    
    visualizer = CleanBookmapVisualizer(min_order_size=20)
    visualizer.process_all_files()
    
    print("Visualization complete!")

if __name__ == "__main__":
    main()
