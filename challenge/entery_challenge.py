import numpy as np
import pandas as pd

#helper func
def to_min(timestamp: str):
    temp = timestamp.split(sep=":")
    minutes = int(temp[0]) * 60 + int(temp[1])
    return minutes

#helper func
def to_timestamp(min: str):
    timestamp = str(int(min//60)).zfill(2) +':' + str(min%60).zfill(2)
    return timestamp
  
def to_cumulative(stream: list):
  string_result = []
  stream_np = np.array(stream)
  stream_np = np.char.split(stream_np, sep=',')
  streamDf = pd.DataFrame(stream_np.tolist(), columns = ['Timestamp', 'Ticker', 'Quantity','Price'])
  streamDf['Timestamp'] = streamDf['Timestamp'].apply(to_min)
  streamDf['Quantity'] = streamDf['Quantity'].apply(int)
  streamDf['Price'] = streamDf['Price'].apply(float)
  streamDf = streamDf.sort_values(['Timestamp','Ticker']).reset_index(drop=True)
  timeStamps = streamDf.Timestamp.unique()
  streamDf['Notional'] = streamDf['Quantity']*streamDf['Price']
  streamDf['Cum_Quantity'] = streamDf.groupby(['Ticker'])['Quantity'].cumsum(axis = 0)
  streamDf['Cum_Notional'] = streamDf.groupby(['Ticker'])['Notional'].cumsum(axis = 0)
  print(streamDf)
  for j in timeStamps:
        temp = streamDf.loc[streamDf['Timestamp'] == j].to_numpy()
        string_out = to_timestamp(j)
        for k in range(temp.shape[0]):
            string_out += ','
            string_out += temp[k][1]
            string_out += ','
            string_out += str(temp[k][5])
            string_out += ','
            string_out += str(round(temp[k][6], 1))

        string_result.append(string_out)

  return string_result

def to_cumulative_delayed(stream: list, quantity_block: int):
  string_result = []
  stream_np = np.array(stream)
  stream_np = np.char.split(stream_np, sep=',')
  streamDf = pd.DataFrame(stream_np.tolist(), columns = ['Timestamp', 'Ticker', 'Quantity','Price'])
  streamDf['Timestamp'] = streamDf['Timestamp'].apply(to_min)
  streamDf['Quantity'] = streamDf['Quantity'].apply(int)
  streamDf['Price'] = streamDf['Price'].apply(float)
  streamDf = streamDf.sort_values(['Timestamp','Ticker']).reset_index(drop=True)
  timeStamps = streamDf.Timestamp.unique()
  streamDf['Notional'] = streamDf['Quantity']*streamDf['Price']
  streamDf['Cum_Quantity'] = streamDf.groupby(['Ticker'])['Quantity'].cumsum(axis = 0)
  streamDf['Cum_Notional'] = streamDf.groupby(['Ticker'])['Notional'].cumsum(axis = 0)
  ticketsNextQuota = {i: quantity_block for i in streamDf.Ticker.unique()}
  for j in timeStamps:
        temp = streamDf.loc[streamDf['Timestamp'] == j].to_numpy()
        string_out = to_timestamp(j)
        for k in range(temp.shape[0]):
            if temp[k][5] < ticketsNextQuota[temp[k][1]]:
                continue
            ticketsNextQuota[temp[k][1]] = temp[k][5] - temp[k][5]%quantity_block + quantity_block
            string_out += ','
            string_out += temp[k][1]
            string_out += ','
            string_out += str(temp[k][5] - temp[k][5]%quantity_block)
            string_out += ','
            string_out += str(round(temp[k][6] - temp[k][5]%quantity_block * temp[k][3], 1))

        if ',' in string_out:
            string_result.append(string_out)

  return string_result