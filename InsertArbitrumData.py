from clickhouse_driver import Client
from clickhouse_driver import connect
from web3 import Web3, HTTPProvider
from hexbytes import HexBytes
import pandas as pd
import sqlite3

pw = 'tvM4udMRZ37sf8Kh'
w3 = Web3(HTTPProvider('https://damp-empty-asphalt.arbitrum-mainnet.quiknode.pro/911cd609990518256679b8aa557b938a1ac1f6da/'))
chClient = Client('twitter-test.luabase.altinity.cloud', user='admin', password=pw, port=9440, secure='y', verify=False, database='cody_arbitrum')
#conn = connect(host='twitter-test.luabase.altinity.cloud', user='admin', password=pw, port=9440, database='cody_arbitrum')
#cursor = conn.cursor()

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d
connection = sqlite3.connect('luaArbTest.db')
connection.row_factory = dict_factory
cur = connection.cursor()

endingBlock = 36129700
currentBlock = 36129600

blocksToInsert = []
transactionsToInsert = []
#loop through blocks and acquire all block and transaction data
while currentBlock < endingBlock:
    blockDict = w3.eth.get_block(currentBlock, True)
    blockToInsert = (
        blockDict.number,
        blockDict.hash.hex(),
        blockDict.parentHash.hex(),
        int(blockDict.nonce.hex(), 0),
        blockDict.sha3Uncles.hex(),
        blockDict.logsBloom.hex(),
        blockDict.transactionsRoot.hex(),
        blockDict.stateRoot.hex(),
        blockDict.receiptsRoot.hex(),
        blockDict.miner,
        blockDict.difficulty,
        blockDict.totalDifficulty,
        blockDict.size,
        blockDict.extraData.hex(),
        blockDict.gasLimit,
        blockDict.gasUsed,
        blockDict.timestamp,
        len(blockDict.transactions),
        blockDict.get("baseFeePerGas", 0),
        int(blockDict.l1BlockNumber, 0),
    )
    blocksToInsert.append(blockToInsert)
    for transaction in blockDict.transactions:
        transactionToInsert = (
            transaction.hash.hex(),
            transaction.nonce,
            transaction.blockHash.hex(),
            transaction.blockNumber,
            transaction.transactionIndex,
            transaction["from"],
            transaction.to,
            transaction.value,
            transaction.gas,
            transaction.gasPrice,
            transaction.input,
            blockDict.timestamp,
            0,
            0,
            transaction.type
        )
        transactionsToInsert.append(transactionToInsert)
    currentBlock = currentBlock + 1

#insert block and transaction data into their respective data tables
cur.executemany('INSERT INTO blocks_raw (number, hash, parent_hash, nonce, sha3_uncles, logs_bloom, transactions_root, state_root, receipts_root, miner, difficulty, total_difficulty, size, extra_data, gas_limit, gas_used, timestamp, transaction_count, base_fee_per_gas, l1blocknumber) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', blocksToInsert)
cur.executemany('INSERT INTO transactions_raw (hash, nonce, block_hash, block_number, transaction_index, from_address, to_address, value, gas, gas_price, input, block_timestamp, max_fee_per_gas, max_priority_fee_per_gas, transaction_type) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', transactionsToInsert)
connection.commit()
connection.close()

'''
bdf = pd.DataFrame(testblocks, columns=['number', 'hash', 'parent_hash', 'nonce', 'sha3_uncles', 'logs_bloom', 'transactions_root', 'state_root', 'receipts_root', 'miner', 'difficulty', 'total_difficulty', 'size', 'extra_data', 'gas_limit', 'gas_used', 'timestamp', 'transaction_count', 'base_fee_per_gas', 'l1blocknumber'])
#tdf = pd.DataFrame(transactionsToInsert, columns=['hash', 'nonce', 'block_hash', 'block_number', 'transaction_index', 'from_address', 'to_address', 'value', 'gas', 'gas_price', 'input', 'block_timestamp', 'max_fee_per_gas', 'max_priority_fee_per_gas', 'transaction_type'])
print(bdf)
#number, hash, parent_hash, nonce, sha3_uncles, logs_bloom, transactions_root, state_root, receipts_root, miner, difficulty, total_difficulty, size, extra_data, gas_limit, gas_used, timestamp, transaction_count, base_fee_per_gas, l1blocknumber
chClient.insert_dataframe('INSERT INTO cody_arbitrum.blocks_raw (*) VALUES', bdf)
#chClient.insert_dataframe('INSERT INTO cody_arbitrum.transactions_raw (hash, nonce, block_hash, block_number, transaction_index, from_address, to_address, value, gas, gas_price, input, block_timestamp, max_fee_per_gas, max_priority_fee_per_gas, transaction_type) VALUES', tdf)
'''