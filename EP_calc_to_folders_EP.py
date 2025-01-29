import dask.dataframe as dd
from dask.distributed import Client
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import pyarrow.compute as pc
import gc
from decimal import Decimal  # Add this import statement
from concurrent.futures import ThreadPoolExecutor
import shutil
import decimal


#initialise@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ 

speriod=int(input("Enter the simulation period: "))
samples=int(input("Enter the number of samples: "))
# Define the folder containing the Parquet files
folder_path = r'D:\RISHIN\13_ILC_TASK1\input\PARQUET_FILES'
output_folder_path = input("Enter the output folder path: ")

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

parquet_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.parquet')]





# Check if there are any Parquet files in the folder
if parquet_files:
    # Read the first Parquet file in chunks
    parquet_file = pq.ParquetFile(parquet_files[0])
    for batch in parquet_file.iter_batches(batch_size=1000):
        # Convert the first batch to a PyArrow Table
        table = pa.Table.from_batches([batch])
        
        # Convert the PyArrow Table to a Pandas DataFrame
        df = table.to_pandas()
        
        # Extract the first value of LocationName and split it by '_'
        location_name = df['LocationName'].iloc[0]
        country = location_name.split('_')[0]
        
        
        # Define the main folder path
        main_folder_path = os.path.join(output_folder_path, f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_Losses')
        
        # Define subfolders
        subfolders = ['EP', 'PLT', 'STATS']
        nested_folders = ['Lob', 'Portfolio']
        innermost_folders = ['GR', 'GU']
        
        # Create the main folder and subfolders
        for subfolder in subfolders:
            subfolder_path = os.path.join(main_folder_path, subfolder)
            os.makedirs(subfolder_path, exist_ok=True)
            
            for nested_folder in nested_folders:
                nested_folder_path = os.path.join(subfolder_path, nested_folder)
                os.makedirs(nested_folder_path, exist_ok=True)
                
                for innermost_folder in innermost_folders:
                    innermost_folder_path = os.path.join(nested_folder_path, innermost_folder)
                    os.makedirs(innermost_folder_path, exist_ok=True)
        
        print(f"Folders created successfully at {main_folder_path}")
        break  # Process only the first batch
else:
    print("No Parquet files found in the specified folder.")



# For EP LOB GU 




# Initialize an empty list to store the results
final_grouped_tables = []
# Process each Parquet file individually
for file in parquet_files:
    # Read the Parquet file into a PyArrow Table
    table = pq.read_table(file)
    
    # Perform the aggregation: sum the Loss column grouped by EventId, PeriodId, and LobName
    grouped_table = table.group_by(['EventId', 'PeriodId', 'LobName']).aggregate([('Loss', 'sum')])
    
    # Rename the aggregated column to Sum_Loss
    grouped_table = grouped_table.rename_columns(['EventId', 'PeriodId', 'LobName', 'Sum_Loss'])
    
    # Append the grouped Table to the final_grouped_tables list
    final_grouped_tables.append(grouped_table)

# Concatenate all grouped tables
final_table = pa.concat_tables(final_grouped_tables)

# Perform final grouping and sorting
final_grouped_table = final_table.group_by(['EventId', 'PeriodId', 'LobName']).aggregate([('Sum_Loss', 'sum')])
sorted_final_table = final_grouped_table.sort_by([('Sum_Loss_sum', 'descending')])
# The Table is now ready for the next instructions
dataframe_1 = sorted_final_table
dataframe_1= dataframe_1.to_pandas()
if not dataframe_1[dataframe_1['LobName'] == 'AGR'].empty:
    daf_AGR = dataframe_1[dataframe_1['LobName'] == 'AGR']

if not dataframe_1[dataframe_1['LobName'] == 'AUTO'].empty:
    daf_AUTO = dataframe_1[dataframe_1['LobName'] == 'AUTO']

if not dataframe_1[dataframe_1['LobName'] == 'COM'].empty:
    daf_COM = dataframe_1[dataframe_1['LobName'] == 'COM']

if not dataframe_1[dataframe_1['LobName'] == 'IND'].empty:
    daf_IND = dataframe_1[dataframe_1['LobName'] == 'IND']

if not dataframe_1[dataframe_1['LobName'] == 'SPER'].empty:
    daf_SPER = dataframe_1[dataframe_1['LobName'] == 'SPER']

if not dataframe_1[dataframe_1['LobName'] == 'FRST'].empty:
    daf_FRST = dataframe_1[dataframe_1['LobName'] == 'FRST']

if not dataframe_1[dataframe_1['LobName'] == 'GLH'].empty:
    daf_GLH = dataframe_1[dataframe_1['LobName'] == 'GLH']





def process_and_save_parquet(dataframe_1, parquet_file_path, speriod, samples):
    dataframe_2 = dataframe_1.groupby(['PeriodId', 'LobName'], as_index=False).agg({'Sum_Loss_sum': 'max'})
    dataframe_2.rename(columns={'Sum_Loss_sum': 'Max_Loss'}, inplace=True)
    dataframe_2 = dataframe_2.sort_values(by='Max_Loss', ascending=False)

    dataframe_3 = dataframe_1.groupby(['PeriodId', 'LobName'], as_index=False).agg({'Sum_Loss_sum': 'sum'})
    dataframe_3.rename(columns={'Sum_Loss_sum': 'S_Sum_Loss'}, inplace=True)
    dataframe_3 = dataframe_3.sort_values(by='S_Sum_Loss', ascending=False)

    dataframe_2['rate'] = (1 / (speriod * samples))
    dataframe_2['cumrate'] = dataframe_2['rate'].cumsum()
    dataframe_2['RPs'] = (1 / dataframe_2['cumrate'])
    dataframe_2['TCE_OEP_1'] = ((dataframe_2['Max_Loss'] - dataframe_2['Max_Loss'].shift(-1)) * 
                              (dataframe_2['cumrate'] + dataframe_2['cumrate'].shift(-1)) * 0.5)
    dataframe_2['TCE_OEP_2'] = (dataframe_2['TCE_OEP_1'].shift().cumsum() * dataframe_2['RPs'])
    dataframe_2['TCE_OEP_Final'] = (dataframe_2['TCE_OEP_2'] + dataframe_2['Max_Loss'])

    dataframe_3['rate'] = (1 / (speriod * samples))
    dataframe_3['cumrate'] = dataframe_3['rate'].cumsum()
    dataframe_3['RPs'] = (1 / dataframe_3['cumrate'])
    dataframe_3['TCE_AEP_1'] = ((dataframe_3['S_Sum_Loss'] - dataframe_3['S_Sum_Loss'].shift(-1)) * 
                              (dataframe_3['cumrate'] + dataframe_3['cumrate'].shift(-1)) * 0.5)
    dataframe_3['TCE_AEP_2'] = (dataframe_3['TCE_AEP_1'].shift().cumsum() * dataframe_3['RPs'])
    dataframe_3['TCE_AEP_Final'] = (dataframe_3['TCE_AEP_2'] + dataframe_3['S_Sum_Loss'])

    rps_values = [float(x) for x in [10000, 5000, 1000, 500, 250, 200, 100, 50, 25, 10, 5, 2]]
    fdataframe_2 = pd.DataFrame()
    fdataframe_3 = pd.DataFrame()
    decimal_places = 8

    for value in rps_values:
        rounded_value = round(value, decimal_places)
        fdataframe_2 = pd.concat([fdataframe_2, dataframe_2[np.round(dataframe_2['RPs'], decimal_places) == rounded_value]])
        fdataframe_3 = pd.concat([fdataframe_3, dataframe_3[np.round(dataframe_3['RPs'], decimal_places) == rounded_value]])

    fdataframe_3.rename(columns={'S_Sum_Loss': 'AEP', 'TCE_AEP_Final': 'TCE-AEP'}, inplace=True)
    fdataframe_2.rename(columns={'Max_Loss': 'OEP', 'TCE_OEP_Final': 'TCE-OEP'}, inplace=True)

    lobname_to_lobid = {
        'AGR': 1,
        'AUTO': 2,
        'COM': 3,
        'IND': 4,
        'SPER': 5,
        'FRST': 6,
        'GLH': 7
    }

    fdataframe_2['LobId'] = fdataframe_2['LobName'].map(lobname_to_lobid)
    fdataframe_3['LobId'] = fdataframe_3['LobName'].map(lobname_to_lobid)

    # Cast LobId to Decimal with precision 38 and scale 0
    fdataframe_2['LobId'] = fdataframe_2['LobId'].apply(lambda x: decimal.Decimal(x).scaleb(-0))
    fdataframe_3['LobId'] = fdataframe_3['LobId'].apply(lambda x: decimal.Decimal(x).scaleb(-0))

    columns_to_keep_3 = ['RPs', 'LobId', 'LobName']
    columns_to_melt_3 = ['AEP', 'TCE-AEP']
    melted_df_3 = fdataframe_3.melt(id_vars=columns_to_keep_3, value_vars=columns_to_melt_3, 
                                    var_name='EPType', value_name='Loss')
    melted_df_3.rename(columns={'RPs': 'ReturnPeriod'}, inplace=True)
    final_df_3 = melted_df_3[['EPType', 'Loss', 'ReturnPeriod', 'LobId', 'LobName']]

    columns_to_keep_2 = ['RPs', 'LobId', 'LobName']
    columns_to_melt_2 = ['OEP', 'TCE-OEP']
    melted_df_2 = fdataframe_2.melt(id_vars=columns_to_keep_2, value_vars=columns_to_melt_2, 
                                    var_name='EPType', value_name='Loss')
    melted_df_2.rename(columns={'RPs': 'ReturnPeriod'}, inplace=True)
    final_df_2 = melted_df_2[['EPType', 'Loss', 'ReturnPeriod', 'LobId', 'LobName']]

    final_df_EP_LOB_GU = pd.concat([final_df_2, final_df_3], ignore_index=True)
    new_ep_type_order = ["OEP", "AEP", "TCE-OEP", "TCE-AEP"]
    final_df_EP_LOB_GU['EPType'] = pd.Categorical(final_df_EP_LOB_GU['EPType'], categories=new_ep_type_order, ordered=True)
    final_df_EP_LOB_GU = final_df_EP_LOB_GU.sort_values(by=['EPType', 'ReturnPeriod'], ascending=[True, False]).reset_index(drop=True)

    # Define the schema to match the required Parquet file schema
    schema = pa.schema([
        pa.field('EPType', pa.string(), nullable=True),
        pa.field('Loss', pa.float64(), nullable=True),
        pa.field('ReturnPeriod', pa.float64(), nullable=True),
        pa.field('LobId', pa.decimal128(38, 0), nullable=True),
        pa.field('LobName', pa.string(), nullable=True)
    ])

    # Convert DataFrame to Arrow Table with the specified schema
    table = pa.Table.from_pandas(final_df_EP_LOB_GU, schema=schema)

    # Save to Parquet
    pq.write_table(table, parquet_file_path)

    print(f"Parquet file saved successfully at {parquet_file_path}")


# In[124]:


pq_file_path_1=os.path.join(main_folder_path, 'EP', 'Lob', 'GU', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_EP_Lob_GU_0.parquet')

pq_file_path_2=os.path.join(main_folder_path, 'EP', 'Lob', 'GU', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_EP_Lob_GU_1.parquet')

pq_file_path_3=os.path.join(main_folder_path, 'EP', 'Lob', 'GU', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_EP_Lob_GU_2.parquet')

pq_file_path_4=os.path.join(main_folder_path, 'EP', 'Lob', 'GU', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_EP_Lob_GU_3.parquet')

pq_file_path_5=os.path.join(main_folder_path, 'EP', 'Lob', 'GU', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_EP_Lob_GU_4.parquet')

pq_file_path_6=os.path.join(main_folder_path, 'EP', 'Lob', 'GU', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_EP_Lob_GU_5.parquet')

pq_file_path_7=os.path.join(main_folder_path, 'EP', 'Lob', 'GU', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_EP_Lob_GU_6.parquet')




# In[127]:


try:
    process_and_save_parquet(daf_AGR, pq_file_path_1, speriod, samples)
except NameError:
    pass

try:
    process_and_save_parquet(daf_AUTO, pq_file_path_2, speriod, samples)
except NameError:
    pass

try:
    process_and_save_parquet(daf_COM, pq_file_path_3, speriod, samples)
except NameError:
    pass

try:
    process_and_save_parquet(daf_IND, pq_file_path_4, speriod, samples)
except NameError:
    pass

try:
    process_and_save_parquet(daf_SPER, pq_file_path_5, speriod, samples)
except NameError:
    pass

try:
    process_and_save_parquet(daf_FRST, pq_file_path_6, speriod, samples)
except NameError:
    pass

try:
    process_and_save_parquet(daf_GLH, pq_file_path_7, speriod, samples)
except NameError:
    pass




#now for EP portfoilio GU



# Initialize an empty list to store the results
final_grouped_tables = []

# Process each Parquet file individually
for file in parquet_files:
    # Read the Parquet file into a PyArrow Table
    table = pq.read_table(file)
    
    # Perform the aggregation: sum the Loss column grouped by EventId, PeriodId, and LobName
    grouped_table = table.group_by(['EventId', 'PeriodId']).aggregate([('Loss', 'sum')])
    
    # Rename the aggregated column to Sum_Loss
    grouped_table = grouped_table.rename_columns(['EventId', 'PeriodId', 'Sum_Loss'])
    
    # Append the grouped Table to the final_grouped_tables list
    final_grouped_tables.append(grouped_table)

# Concatenate all grouped tables
final_table = pa.concat_tables(final_grouped_tables)

# Perform final grouping and sorting
final_grouped_table = final_table.group_by(['EventId', 'PeriodId']).aggregate([('Sum_Loss', 'sum')])
sorted_final_table = final_grouped_table.sort_by([('Sum_Loss_sum', 'descending')])
# The Table is now ready for the next instructions
dataframe_1 = sorted_final_table
dataframe_1= dataframe_1.to_pandas()
#dataframe_1 = dataframe_1[dataframe_1['LobName'] == 'AUTO']

# Initialize dataframe_2 by selecting PeriodId and max(Sum_Loss) grouped by PeriodId
dataframe_2 = dataframe_1.groupby(['PeriodId'], as_index=False).agg({'Sum_Loss_sum': 'max'})

# Rename the aggregated column to Max_Loss
dataframe_2.rename(columns={'Sum_Loss_sum': 'Max_Loss'}, inplace=True)

# Sort dataframe_2 by Max_Loss in descending order
dataframe_2 = dataframe_2.sort_values(by='Max_Loss', ascending=False)

# Initialize dataframe_2 by selecting PeriodId and Sum(Sum_Loss) grouped by PeriodId
dataframe_3 = dataframe_1.groupby(['PeriodId'], as_index=False).agg({'Sum_Loss_sum': 'sum'})

# Rename the aggregated column to Sum_Loss
dataframe_3.rename(columns={'Sum_Loss_sum': 'S_Sum_Loss'}, inplace=True)

# Sort dataframe_3 by S_sum_Loss in descending order
dataframe_3 = dataframe_3.sort_values(by='S_Sum_Loss', ascending=False)

#dataframe_2['Max_Loss'] = dataframe_2['Max_Loss'].round(5)

dataframe_2['rate'] = (1 / (speriod * samples))

# Calculate the cumulative rate column and round to 6 decimal places
dataframe_2['cumrate'] = dataframe_2['rate'].cumsum()

# Calculate the RPs column and round to 6 decimal places
dataframe_2['RPs'] = (1 / dataframe_2['cumrate'])




# Calculate the TCE_OEP_1 column and round to 6 decimal places
dataframe_2['TCE_OEP_1'] = ((dataframe_2['Max_Loss'] - dataframe_2['Max_Loss'].shift(-1)) * 
                          (dataframe_2['cumrate'] + dataframe_2['cumrate'].shift(-1)) * 0.5)

# Calculate the TCE_OEP_2 column and round to 6 decimal places
dataframe_2['TCE_OEP_2'] = (dataframe_2['TCE_OEP_1'].shift().cumsum() * dataframe_2['RPs'])


# Calculate the TCE_OEP_Final column and round to 6 decimal places
dataframe_2['TCE_OEP_Final'] = (dataframe_2['TCE_OEP_2'] + dataframe_2['Max_Loss'])

#dataframe_3['S_Sum_Loss'] = dataframe_3['S_Sum_Loss'].round(5)

# Calculate the rate column and round to 6 decimal places
dataframe_3['rate'] = (1 / (speriod * samples))

# Calculate the cumulative rate column and round to 6 decimal places
dataframe_3['cumrate'] = dataframe_3['rate'].cumsum()

# Calculate the RPs column and round to 6 decimal places
dataframe_3['RPs'] = (1 / dataframe_3['cumrate'])


# Calculate the TCE_AEP_1 column and round to 6 decimal places
dataframe_3['TCE_AEP_1'] = ((dataframe_3['S_Sum_Loss'] - dataframe_3['S_Sum_Loss'].shift(-1)) * 
                          (dataframe_3['cumrate'] + dataframe_3['cumrate'].shift(-1)) * 0.5)

# Calculate the cumulative sum up to the previous row and multiply by the current row's RPs, then round to 6 decimal places
dataframe_3['TCE_AEP_2'] = (dataframe_3['TCE_AEP_1'].shift().cumsum() * dataframe_3['RPs'])

# Calculate the TCE_AEP_Final column and round to 6 decimal places
dataframe_3['TCE_AEP_Final'] = (dataframe_3['TCE_AEP_2'] + dataframe_3['S_Sum_Loss'])

# Define the list of RPs values to filter and convert them to float
rps_values = [float(x) for x in [10000, 5000, 1000, 500, 250, 200, 100, 50, 25, 10, 5, 2]]

# Initialize an empty DataFrame to store the filtered results
fdataframe_2 = pd.DataFrame()
fdataframe_3 = pd.DataFrame()

# Define the number of decimal places to round to
decimal_places = 8

# Loop through each value in rps_values and filter the DataFrames
for value in rps_values:
    rounded_value = round(value, decimal_places)
    fdataframe_2 = pd.concat([fdataframe_2, dataframe_2[np.round(dataframe_2['RPs'], decimal_places) == rounded_value]])
    fdataframe_3 = pd.concat([fdataframe_3, dataframe_3[np.round(dataframe_3['RPs'], decimal_places) == rounded_value]])


fdataframe_3.rename(columns={'S_Sum_Loss': 'AEP','TCE_AEP_Final': 'TCE-AEP'}, inplace=True)
fdataframe_2.rename(columns={ 'Max_Loss': 'OEP','TCE_OEP_Final': 'TCE-OEP'}, inplace=True)

# Define the columns to be used in the new DataFrame for fdataframe_3
columns_to_keep_3 = ['RPs']
columns_to_melt_3 = [ 'AEP','TCE-AEP']

# Melt fdataframe_3 to reshape it
melted_df_3 = fdataframe_3.melt(id_vars=columns_to_keep_3, value_vars=columns_to_melt_3, 
                                var_name='EPType', value_name='Loss')

# Rename columns to match the desired output
melted_df_3.rename(columns={'RPs': 'ReturnPeriod'}, inplace=True)

# Reorder columns
final_df_3 = melted_df_3[['EPType', 'Loss', 'ReturnPeriod']]

# Define the columns to be used in the new DataFrame for fdataframe_2
columns_to_keep_2 = ['RPs']
columns_to_melt_2 = [ 'OEP','TCE-OEP']

# Melt fdataframe_2 to reshape it
melted_df_2 = fdataframe_2.melt(id_vars=columns_to_keep_2, value_vars=columns_to_melt_2, 
                                var_name='EPType', value_name='Loss')

# Rename columns to match the desired output
melted_df_2.rename(columns={'RPs': 'ReturnPeriod'}, inplace=True)

# Reorder columns
final_df_2 = melted_df_2[['EPType', 'Loss', 'ReturnPeriod']]

# Concatenate the two DataFrames
final_df_EP_Portfolio_GU = pd.concat([ final_df_2,final_df_3], ignore_index=True)


# Define the new order for EPType
new_ep_type_order = ["OEP", "AEP", "TCE-OEP", "TCE-AEP"]

# Update the EPType column to the new order
final_df_EP_Portfolio_GU['EPType'] = pd.Categorical(final_df_EP_Portfolio_GU['EPType'], categories=new_ep_type_order, ordered=True)

# Sort the DataFrame by EPType and then by ReturnPeriod in descending order within each EPType
final_df_EP_Portfolio_GU = final_df_EP_Portfolio_GU.sort_values(by=['EPType', 'ReturnPeriod'], ascending=[True, False]).reset_index(drop=True)


main_folder_path = os.path.join(output_folder_path, f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_Losses')

# Define the file path for the Parquet file
parquet_file_path = os.path.join(main_folder_path, 'EP', 'Portfolio', 'GU', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_EP_Portfolio_GU_0.parquet')

# Save final_df as a Parquet file
final_df_EP_Portfolio_GU.to_parquet(parquet_file_path, index=False)

print(f"Parquet file saved successfully at {parquet_file_path}")



# In[ ]:


#now for stats LOB GU 


# In[63]:



main_folder_path = os.path.join(output_folder_path, f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_Losses')

# Define the file path for the Parquet file
parquet_file_path = os.path.join(main_folder_path, 'STATS', 'Lob', 'GU', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_STATS_Lob_GU_0.parquet')
aggregated_tables_lob_stats = []

# Define the mapping of LobName to LobId
lobname_to_lobid = {
    'AGR': 1,
    'AUTO': 2,
    'COM': 3,
    'IND': 4,
    'SPER': 5,
    'FRST': 6,
    'GLH': 7
}

# Process each Parquet file individually
for file in parquet_files:
    # Check if the file exists
    if os.path.exists(file):
        # Read the Parquet file into a PyArrow Table
        table = pq.read_table(file)
        
        # Perform the aggregation: sum the Loss column grouped by LobName
        grouped = table.group_by('LobName').aggregate([('Loss', 'sum')])
        
        # Calculate AAL
        loss_sum = grouped.column('Loss_sum').to_numpy()
        aal = loss_sum / speriod / samples
        aal_array = pa.array(aal)
        grouped = grouped.append_column('AAL', aal_array)
        
        # Select only the necessary columns
        grouped = grouped.select(['LobName', 'AAL'])
        
        # Append the grouped Table to the list
        aggregated_tables_lob_stats.append(grouped)
    else:
        print(f"File not found: {file}")

# Check if any tables were aggregated
if not aggregated_tables_lob_stats:
    print("No tables were aggregated. Please check the input files.")
else:
    # Concatenate all the grouped Tables
    final_table = pa.concat_tables(aggregated_tables_lob_stats)

    # Group the final Table again to ensure all groups are combined
    final_grouped = final_table.group_by('LobName').aggregate([('AAL', 'sum')])

    # Sort the final grouped Table by 'AAL' in descending order
    final_grouped = final_grouped.sort_by([('AAL_sum', 'descending')])

    # Convert the final grouped Table to a Pandas DataFrame
    final_df = final_grouped.to_pandas()

    # Map LobName to LobId
    final_df['LobId'] = final_df['LobName'].map(lobname_to_lobid).apply(lambda x: Decimal(x))

    final_df_STATS_Lob = final_df.rename(columns={'AAL_sum': 'AAL'})

    # Define the columns with NaN values for 'Std' and 'CV'
    final_df_STATS_Lob['Std'] = np.nan
    final_df_STATS_Lob['CV'] = np.nan

    # Reorder the columns to match the specified format
    final_df_STATS_Lob = final_df_STATS_Lob[['AAL', 'Std', 'CV', 'LobId', 'LobName']]

    # Define the desired schema
    desired_schema = pa.schema([
        pa.field('AAL', pa.float64()),
        pa.field('Std', pa.float64()),
        pa.field('CV', pa.float64()),
        pa.field('LobId', pa.decimal128(38)),
        pa.field('LobName', pa.string())
    ])

    # Convert the DataFrame back to a PyArrow Table with the desired schema
    final_table_STATS_Lob = pa.Table.from_pandas(final_df_STATS_Lob, schema=desired_schema)
    pq.write_table(final_table_STATS_Lob, parquet_file_path)
    print(f"Parquet file saved successfully at {parquet_file_path}")


# In[ ]:


#now for STATS Portfolio GU


# In[9]:


aggregated_tables = []

# Process each Parquet file individually
for file in parquet_files:
    # Read the Parquet file into a PyArrow Table
    table = pq.read_table(file)
    
    # Perform the aggregation: sum the Loss column grouped by LobName
    grouped = table.group_by('LobName').aggregate([('Loss', 'sum')])
    
    # Calculate AAL
    loss_sum = grouped.column('Loss_sum').to_numpy()
    aal = loss_sum / speriod / samples
    aal_array = pa.array(aal)
    grouped = grouped.append_column('AAL', aal_array)
    
    # Select only the necessary columns
    grouped = grouped.select(['LobName', 'AAL'])
    
    # Append the grouped Table to the list
    aggregated_tables.append(grouped)

# Concatenate all the grouped Tables
final_table = pa.concat_tables(aggregated_tables)

# Convert the final table to a Pandas DataFrame
final_df = final_table.to_pandas()

# Sum all the AAL values without grouping by LobName
total_aal = final_df['AAL'].sum()

# Create a DataFrame with the specified columns
final_df_STATS_Portfolio = pd.DataFrame({
    'AAL': [total_aal],
    'Std': [np.nan],
    'CV': [np.nan],
})




main_folder_path = os.path.join(output_folder_path, f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_Losses')

# Define the file path for the Parquet file
parquet_file_path = os.path.join(main_folder_path, 'STATS', 'Portfolio', 'GU', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_STATS_Portfolio_GU_0.parquet')
final_df_STATS_Portfolio.to_parquet(parquet_file_path, index=False)
print(f"Parquet file saved successfully at {parquet_file_path}")




#PLT GU Lob


# In[111]:


# List all Parquet files in the folder
parquet_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.parquet')]

# Define the schema
schema = pa.schema([
    pa.field('PeriodId', pa.decimal128(38, 0), nullable=True),
    pa.field('EventId', pa.decimal128(38, 0), nullable=True),
    pa.field('EventDate', pa.timestamp('ms', tz='UTC'), nullable=True),
    pa.field('LossDate', pa.timestamp('ms', tz='UTC'), nullable=True),
    pa.field('Loss', pa.float64(), nullable=True),
    pa.field('Region', pa.string(), nullable=True),
    pa.field('Peril', pa.string(), nullable=True),
    pa.field('Weight', pa.float64(), nullable=True),
    pa.field('LobId', pa.decimal128(38, 0), nullable=True),
    pa.field('LobName', pa.string(), nullable=True)
])

# Directory to store intermediate results
intermediate_dir = os.path.join(folder_path, 'intermediate_results')
os.makedirs(intermediate_dir, exist_ok=True)

group_by_columns = ['PeriodId', 'EventId', 'EventDate', 'LossDate', 'Region', 'Peril', 'Weight', 'LobId', 'LobName']

# Process each Parquet file in chunks and write intermediate results to disk
for i, file in enumerate(parquet_files):
    file_path = os.path.join(folder_path, file)
    parquet_file = pq.ParquetFile(file_path)
    for j, batch in enumerate(parquet_file.iter_batches()):
        table = pa.Table.from_batches([batch])
        
        # Cast columns to the desired types
        table = table.set_column(table.schema.get_field_index('PeriodId'), 'PeriodId', pa.compute.cast(table['PeriodId'], pa.decimal128(38, 0)))
        table = table.set_column(table.schema.get_field_index('EventId'), 'EventId', pa.compute.cast(table['EventId'], pa.decimal128(38, 0)))
        table = table.set_column(table.schema.get_field_index('EventDate'), 'EventDate', pa.compute.cast(table['EventDate'], pa.timestamp('ms', tz='UTC')))
        table = table.set_column(table.schema.get_field_index('LossDate'), 'LossDate', pa.compute.cast(table['LossDate'], pa.timestamp('ms', tz='UTC')))
        table = table.set_column(table.schema.get_field_index('Loss'), 'Loss', pa.compute.cast(table['Loss'], pa.float64()))
        table = table.set_column(table.schema.get_field_index('Region'), 'Region', pa.compute.cast(table['Region'], pa.string()))
        table = table.set_column(table.schema.get_field_index('Peril'), 'Peril', pa.compute.cast(table['Peril'], pa.string()))
        table = table.set_column(table.schema.get_field_index('Weight'), 'Weight', pa.compute.cast(table['Weight'], pa.float64()))
        table = table.set_column(table.schema.get_field_index('LobId'), 'LobId', pa.compute.cast(table['LobId'], pa.decimal128(38, 0)))
        table = table.set_column(table.schema.get_field_index('LobName'), 'LobName', pa.compute.cast(table['LobName'], pa.string()))
        
        grouped_table = table.group_by(group_by_columns).aggregate([('Loss', 'sum')])
        intermediate_file = os.path.join(intermediate_dir, f"intermediate_{i}_{j}.parquet")
        pq.write_table(grouped_table, intermediate_file)

# Read intermediate results and combine them
intermediate_files = [os.path.join(intermediate_dir, f) for f in os.listdir(intermediate_dir) if f.endswith('.parquet')]
intermediate_tables = [pq.read_table(file) for file in intermediate_files]
combined_grouped_table = pa.concat_tables(intermediate_tables)

# Perform the final group by and aggregation
final_grouped_table = combined_grouped_table.group_by(group_by_columns).aggregate([('Loss_sum', 'sum')])
final_grouped_table = final_grouped_table.sort_by([('Loss_sum_sum', 'descending')])

# Rename the aggregated column
final_grouped_table = final_grouped_table.rename_columns(group_by_columns + ['Loss'])

# Save the final table to a Parquet file
main_folder_path = os.path.join(output_folder_path, f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_Losses')
os.makedirs(main_folder_path, exist_ok=True)
parquet_file_path = os.path.join(main_folder_path, 'PLT', 'Lob', 'GU', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_PLT_Lob_GU_0.parquet')

# Reorder the columns in the desired order
ordered_columns = ['PeriodId', 'EventId', 'EventDate', 'LossDate', 'Loss', 'Region', 'Peril', 'Weight', 'LobId', 'LobName']
final_grouped_table = final_grouped_table.select(ordered_columns)

# Save the final table to a Parquet file
pq.write_table(final_grouped_table, parquet_file_path)
print(f"Parquet file saved successfully at {parquet_file_path}")

# Delete intermediate files
for file in intermediate_files:
    try:
        os.remove(file)
    except FileNotFoundError:
        print(f"File not found: {file}")

# Remove the intermediate directory
try:
    os.rmdir(intermediate_dir)
except FileNotFoundError:
    print(f"Directory not found: {intermediate_dir}")
except OSError:
    print(f"Directory not empty or other error: {intermediate_dir}")




#PLT Portfolio GU




# Flush memory at the beginning
main_folder_path = os.path.join(output_folder_path, f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_Losses')
gc.collect()

# Directory to store intermediate results
intermediate_dir = os.path.join(main_folder_path, 'PLT', 'Portfolio', 'GU', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_PLT_Portfolio_G.parquet')
os.makedirs(intermediate_dir, exist_ok=True)

group_by_columns = ["PeriodId", "EventId", "EventDate", "LossDate", "Region", "Peril", "Weight"]

# Process each Parquet file in chunks and write intermediate results to disk
for i, file in enumerate(parquet_files):
    parquet_file = pq.ParquetFile(file)
    for j, batch in enumerate(parquet_file.iter_batches()):
        table = pa.Table.from_batches([batch])
        grouped_table = table.group_by(group_by_columns).aggregate([('Loss', 'sum')])
        intermediate_file = os.path.join(intermediate_dir, f"intermediate_{i}_{j}.parquet")
        pq.write_table(grouped_table, intermediate_file)

# Read intermediate results and combine them
intermediate_files = [os.path.join(intermediate_dir, f) for f in os.listdir(intermediate_dir) if f.endswith('.parquet')]
intermediate_tables = [pq.read_table(file) for file in intermediate_files]
combined_grouped_table = pa.concat_tables(intermediate_tables)

# Perform the final group by and aggregation
final_grouped_table = combined_grouped_table.group_by(group_by_columns).aggregate([('Loss_sum', 'sum')])

# Rename the aggregated column
final_grouped_table = final_grouped_table.rename_columns(group_by_columns + ['Loss'])

# Convert PeriodId and EventId to strings
final_grouped_table = final_grouped_table.set_column(
    final_grouped_table.schema.get_field_index('PeriodId'),
    'PeriodId',
    final_grouped_table.column('PeriodId').cast(pa.string())
)
final_grouped_table = final_grouped_table.set_column(
    final_grouped_table.schema.get_field_index('EventId'),
    'EventId',
    final_grouped_table.column('EventId').cast(pa.string())
)

# Define the schema
schema = pa.schema([
    pa.field('PeriodId', pa.decimal128(38, 0), nullable=True, metadata={'field_id': '-1'}),
    pa.field('EventId', pa.decimal128(38, 0), nullable=True, metadata={'field_id': '-1'}),
    pa.field('EventDate', pa.timestamp('ms', tz='UTC'), nullable=True, metadata={'field_id': '-1'}),
    pa.field('LossDate', pa.timestamp('ms', tz='UTC'), nullable=True, metadata={'field_id': '-1'}),
    pa.field('Loss', pa.float64(), nullable=True, metadata={'field_id': '-1'}),
    pa.field('Region', pa.string(), nullable=True, metadata={'field_id': '-1'}),
    pa.field('Peril', pa.string(), nullable=True, metadata={'field_id': '-1'}),
    pa.field('Weight', pa.float64(), nullable=True, metadata={'field_id': '-1'})
])

# Convert the table to the specified schema
final_grouped_table = pa.Table.from_arrays(
    [final_grouped_table.column(name).cast(schema.field(name).type) for name in schema.names],
    schema=schema
)

# Write the table to a Parquet file with the specified schema
parquet_file_path = os.path.join(main_folder_path, 'PLT', 'Portfolio', 'GU', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_PLT_Portfolio_GU_0.parquet')
final_grouped_table = final_grouped_table.sort_by([('Loss', 'descending')])
pq.write_table(final_grouped_table, parquet_file_path)
print(f"Parquet file saved successfully at {parquet_file_path}")

# Delete intermediate files
for file in intermediate_files:
    try:
        os.remove(file)
    except FileNotFoundError:
        print(f"File not found: {file}")

# Remove the intermediate directory
try:
    os.rmdir(intermediate_dir)
except FileNotFoundError:
    print(f"Directory not found: {intermediate_dir}")
except OSError:
    print(f"Directory not empty or other error: {intermediate_dir}")

