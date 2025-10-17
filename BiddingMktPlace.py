#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 29 22:02:47 2025

@author: aloksharma USEEE MEEE!!!
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 29 20:25:39 2025

@author: aloksharma
"""

import pandas as pd
import json
import ast

def run_cumulative_budget_pipeline(
    raw_data_path='/Users/aloksharma/Downloads/30dys.csv',
    partner_info_path='/Users/aloksharma/Documents/mktplcPrj/v3/partnerfile.csv',
    final_output_path='final_analysis_with_cumulative_rerun.csv',
    timestamp_column='CREATED_AT'
):
    """
    Runs a complete auction pipeline with cumulative, stateful partner budgets.
    
    1.  Calculates new bids based on partner CPA.
    2.  Sorts all auctions chronologically.
    3.  Loops through each auction, finding the highest bidder who has enough remaining budget.
    4.  Depletes the winner's budget by the bid amount.
    5.  Saves the final analysis with the realistically determined winner.
    """
    print("--- Starting Pipeline with Cumulative Budgets ---")
    
    try:
        # --- PART 1: Initial Data Loading and Preparation ---
        print("Step 1: Reading and preparing data...")
        df = pd.read_csv(raw_data_path)
        partner_df = pd.read_csv(partner_info_path)

        if timestamp_column not in df.columns:
            print(f"❌ Error: Timestamp column '{timestamp_column}' not found in the data.")
            return

        # Sort auctions to process them in order
        df = df.sort_values(by=timestamp_column).reset_index(drop=True)
        
        # --- Create lookup dictionaries and initialize remaining budgets ---
        partner_mcpa_data = partner_df.set_index('PARTNER_ID')['Simulated CPA'].to_dict()
        # This dictionary will track the depleting budget for each partner
        partner_remaining_budget = partner_df.set_index('PARTNER_ID')['Budget'].to_dict()

        # --- PART 2: Pre-calculate New Bids (Stateless Operation) ---
        print("Step 2: Calculating all potential new bids...")
        
        def calculate_new_bid(bids_data):
            if not isinstance(bids_data, dict): return bids_data
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
                    except (ValueError, TypeError): pass
                    value['final_value'] = final_value
            return bids_data
        
        df.dropna(subset=['MODEL_OUTPUT'], inplace=True)
        model_outputs = df['MODEL_OUTPUT'].apply(lambda x: json.loads(x).get('bids') if pd.notna(x) else None)
        df['processed_bids'] = model_outputs.apply(calculate_new_bid)

        # --- PART 3: Stateful Auction Rerun with Cumulative Budget ---
        print(f"Step 3: Rerunning {len(df)} auctions chronologically...")
        
        final_rerun_summaries = []
        for index, row in df.iterrows():
            summary_str = row['AUCTION_SUMMARY']
            processed_bids = row['processed_bids']
            original_winner_id = row['BROKER_ID']
            
            try:
                summary_list = ast.literal_eval(summary_str) if isinstance(summary_str, str) else json.loads(summary_str)
            except Exception:
                final_rerun_summaries.append(summary_str)
                continue

            # Update the summary with the new potential bids
            for item in summary_list:
                broker_id_str = str(item.get('BROKER_ID'))
                if isinstance(processed_bids, dict) and broker_id_str in processed_bids:
                    item['BID'] = processed_bids[broker_id_str].get('final_value')
            
            summary_list.append({"BID": row['BID_VALUE'], "BROKER_ID": row['BROKER_ID']})
            
            valid_bidders = [b for b in summary_list if isinstance(b, dict) and isinstance(b.get('BID'), (int, float))]
            sorted_bidders = sorted(valid_bidders, key=lambda x: x['BID'], reverse=True)
            
            final_winner_id = None
            winning_bid = None
            
            for bidder in sorted_bidders:
                bidder_id = bidder.get('BROKER_ID')
                bid_amount = bidder.get('BID')
                
                if bidder_id in partner_remaining_budget and bid_amount <= partner_remaining_budget[bidder_id]:
                    final_winner_id = bidder_id
                    winning_bid = bid_amount
                    partner_remaining_budget[bidder_id] -= winning_bid
                    break
            
            for item in summary_list:
                broker_id = item.get('BROKER_ID')
                if final_winner_id is None:
                    item['WIN_TYPE'] = 'UNSOLD_NO_BUDGET'
                else:
                    is_og = (broker_id == original_winner_id)
                    is_new = (broker_id == final_winner_id)
                    if is_og and is_new: item['WIN_TYPE'] = 'OG_and_NEW_WINNER'
                    elif is_og: item['WIN_TYPE'] = 'OG_WINNER'
                    elif is_new: item['WIN_TYPE'] = 'NEW_WINNER'
            
            final_rerun_summaries.append(json.dumps(summary_list))

        df['CUMULATIVE_RERUN_SUMMARY'] = final_rerun_summaries
        
        # --- Final cleanup and save ---
        print("Step 4: Saving final analysis...")
        final_columns = [col for col in df.columns if col not in ['MODEL_OUTPUT', 'processed_bids']]
        df_final = df[final_columns]
        df_final.to_csv(final_output_path, index=False)
        
        print(f"\n✅ Pipeline complete. Final analysis saved to '{final_output_path}'")
        
        return df_final, partner_remaining_budget

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    final_df, final_budgets = run_cumulative_budget_pipeline(timestamp_column='RECORD_DATE_TIME_UTC')
    
    if final_df is not None:
        print("\n--- First 5 rows of the final analysis ---")
        print(final_df[['AUCTION_SUMMARY', 'CUMULATIVE_RERUN_SUMMARY']].head())

    if 'final_budgets' in locals():
        print("\n--- Processing Final Partner Budgets ---")
        budget_df = pd.DataFrame(list(final_budgets.items()), columns=['PARTNER_ID', 'Remaining_Budget'])
        
        # Sort the data for display
        sorted_budgets = budget_df.sort_values(by='Remaining_Budget', ascending=False)
        
        print("Top 10 partners with the most budget remaining:")
        print(sorted_budgets.head(10))
        
        # <-- ADDED: Save the final budgets to a CSV file.
        budget_filename = 'final_budgets.csv'
        budget_df.to_csv(budget_filename, index=False)
        print(f"\n✅ Successfully saved final budgets to '{budget_filename}'")
