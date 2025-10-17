#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 15 13:54:34 2025

@author: aloksharma
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Auction Simulation Pipeline

This script reruns historical auction data with updated partner bidding strategies
and finite, depleting budgets to simulate a realistic marketplace outcome.
"""
import pandas as pd
import json
import ast
import argparse
import logging
import os
from typing import Dict, Any, List, Tuple

# --- Basic Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def _calculate_new_bid(bids_data: dict, partner_mcpa_data: dict) -> dict:
    """Helper function to recalculate bids based on new CPA values."""
    if not isinstance(bids_data, dict):
        return bids_data
    
    for key, value in bids_data.items():
        if isinstance(value, dict):
            bid_value = value.get('bid_value')
            cpa_target = value.get('cpa_target')
            final_value = None
            try:
                partner_id = int(key)
                if partner_id in partner_mcpa_data:
                    new_mcpa = partner_mcpa_data.get(partner_id)
                    if all(isinstance(v, (int, float)) for v in [bid_value, cpa_target, new_mcpa]) and cpa_target != 0:
                        final_value = (bid_value / cpa_target) * new_mcpa
            except (ValueError, TypeError):
                pass # Ignore partners that can't be processed
            value['final_value'] = final_value
    return bids_data

def run_simulation(raw_df: pd.DataFrame, partner_df: pd.DataFrame, timestamp_column: str) -> Tuple[pd.DataFrame, Dict[int, float]]:
    """
    Core function to run the stateful auction simulation.

    Args:
        raw_df (pd.DataFrame): DataFrame with raw auction data.
        partner_df (pd.DataFrame): DataFrame with partner info (Budget, Simulated CPA).
        timestamp_column (str): The name of the timestamp column to sort by.

    Returns:
        Tuple[pd.DataFrame, Dict[int, float]]: A tuple containing the final DataFrame with simulation
                                               results and a dictionary of final partner budgets.
    """
    # --- 1. Preparation ---
    if timestamp_column not in raw_df.columns:
        logging.error(f"Timestamp column '{timestamp_column}' not found in the data.")
        raise ValueError(f"Timestamp column '{timestamp_column}' not found.")

    logging.info(f"Sorting {len(raw_df)} auctions by '{timestamp_column}'...")
    df = raw_df.sort_values(by=timestamp_column).reset_index(drop=True)

    partner_mcpa_data = partner_df.set_index('PARTNER_ID')['Simulated CPA'].to_dict()
    partner_remaining_budget = partner_df.set_index('PARTNER_ID')['Budget'].to_dict()

    # --- 2. Pre-calculate New Bids (Stateless) ---
    logging.info("Pre-calculating all potential new bids...")
    df.dropna(subset=['MODEL_OUTPUT'], inplace=True)
    model_outputs = df['MODEL_OUTPUT'].apply(lambda x: json.loads(x).get('bids') if pd.notna(x) else None)
    df['processed_bids'] = model_outputs.apply(lambda x: _calculate_new_bid(x, partner_mcpa_data))

    # --- 3. Stateful Auction Rerun (Performance Optimized) ---
    logging.info(f"Rerunning {len(df)} auctions chronologically...")
    final_rerun_summaries = []
    
    # Use to_dict('records') for much faster iteration than iterrows()
    auction_iterator = df[['AUCTION_SUMMARY', 'BID_VALUE', 'BROKER_ID', 'processed_bids']].to_dict('records')

    for row in auction_iterator:
        summary_str = row['AUCTION_SUMMARY']
        processed_bids = row['processed_bids']
        
        try:
            # --- A. Get Original Bids ---
            original_bidders_raw = ast.literal_eval(summary_str) if isinstance(summary_str, str) else json.loads(summary_str)
            if not isinstance(original_bidders_raw, list): original_bidders_raw = []
            
            # Add the original winner to the list
            original_bidders_raw.append({"BID": row['BID_VALUE'], "BROKER_ID": row['BROKER_ID']})
            
            original_bids, new_bids = [], []

            for bidder in original_bidders_raw:
                if not (isinstance(bidder, dict) and 'BROKER_ID' in bidder and 'BID' in bidder): continue

                broker_id = bidder.get('BROKER_ID')
                original_bid_amount = bidder.get('BID')
                original_bids.append({"partner_id": broker_id, "amount": original_bid_amount})

                # --- B. Get New Bids ---
                new_bid_amount = original_bid_amount
                broker_id_str = str(broker_id)
                if isinstance(processed_bids, dict) and broker_id_str in processed_bids:
                    calculated_value = processed_bids[broker_id_str].get('final_value')
                    if isinstance(calculated_value, (int, float)): new_bid_amount = calculated_value
                
                new_bids.append({"partner_id": broker_id, "amount": new_bid_amount})

            # Sort both lists by bid amount, highest first
            original_bids.sort(key=lambda x: x.get('amount', 0) or 0, reverse=True)
            new_bids.sort(key=lambda x: x.get('amount', 0) or 0, reverse=True)

            # --- C. Find Winner & Deplete Budget ---
            for potential_winner in new_bids:
                bidder_id = potential_winner.get('partner_id')
                bid_amount = potential_winner.get('amount')
                
                if bidder_id in partner_remaining_budget and isinstance(bid_amount, (int, float)) and bid_amount <= partner_remaining_budget[bidder_id]:
                    partner_remaining_budget[bidder_id] -= bid_amount
                    break # Winner found, budget depleted. Stop checking.

            # --- D. Build and Append Final JSON ---
            output_json = {"original_bids": original_bids, "new_bids": new_bids}
            final_rerun_summaries.append(json.dumps(output_json))

        except Exception:
            logging.warning(f"Could not process auction summary: {summary_str}. Appending original data.")
            final_rerun_summaries.append(summary_str)

    df['CUMULATIVE_RERUN_SUMMARY'] = final_rerun_summaries
    
    # Clean up temporary columns before returning
    final_df = df.drop(columns=['MODEL_OUTPUT', 'processed_bids'])
    
    return final_df, partner_remaining_budget


def main():
    """Main function to parse arguments and orchestrate the pipeline."""
    parser = argparse.ArgumentParser(description="Run a realistic ad auction simulation with depleting budgets.")
    parser.add_argument('--raw_data', type=str, required=True, help='Path to the raw auction data CSV file (e.g., 30dys.csv).')
    parser.add_argument('--partner_info', type=str, required=True, help='Path to the partner information CSV file (partnerfile.csv).')
    parser.add_argument('--output_analysis', type=str, default='final_analysis_rerun.csv', help='Path to save the final analysis CSV file.')
    parser.add_argument('--output_budgets', type=str, default='final_budgets_rerun.csv', help='Path to save the final budgets CSV file.')
    parser.add_argument('--timestamp_col', type=str, default='RECORD_DATE_TIME_UTC', help='Name of the timestamp column in the raw data.')
    
    args = parser.parse_args()

    try:
        logging.info(f"Loading raw data from: {args.raw_data}")
        raw_df = pd.read_csv(args.raw_data)
        
        logging.info(f"Loading partner info from: {args.partner_info}")
        partner_df = pd.read_csv(args.partner_info)
        
    except FileNotFoundError as e:
        logging.error(f"Error loading files: {e}. Please check your file paths.")
        return

    # Run the core simulation
    final_df, final_budgets = run_simulation(raw_df, partner_df, args.timestamp_col)

    # Save the results
    logging.info(f"Saving final analysis to: {args.output_analysis}")
    final_df.to_csv(args.output_analysis, index=False)

    budget_df = pd.DataFrame(list(final_budgets.items()), columns=['PARTNER_ID', 'Remaining_Budget'])
    logging.info(f"Saving final budgets to: {args.output_budgets}")
    budget_df.to_csv(args.output_budgets, index=False)

    logging.info("✅ Pipeline complete.")


if __name__ == '__main__':
    main()
