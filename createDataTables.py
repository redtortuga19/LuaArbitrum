from clickhouse_driver import Client

pw = 'tvM4udMRZ37sf8Kh'
chClient = Client('twitter-test.luabase.altinity.cloud', user='admin', password=pw, port=9440, secure='y', verify=False, database='cody_arbitrum')

#Create blocks_raw table
sql = '''
CREATE TABLE IF NOT EXISTS cody_arbitrum.blocks_raw
(
    number Int64,
    hash String,
    parent_hash String,
    nonce String,
    sha3_uncles String,
    logs_bloom String,
    transactions_root String,
    state_root String,
    receipts_root String,
    miner String,
    difficulty Int64,
    total_difficulty Int64,
    size Int64,
    extra_data String,
    gas_limit Int64,
    gas_used Int64,
    timestamp Int64,
    transaction_count Int64,
    base_fee_per_gas Int64,
    l1blocknumber Int64
)
ENGINE = MergeTree
PRIMARY KEY (number, hash)
ORDER BY (number, hash)
'''
chClient.execute(sql)

#Create transactions_raw table
sql = '''
CREATE TABLE IF NOT EXISTS cody_arbitrum.transactions_raw
(
    hash String,
    nonce Int64,
    block_hash String,
    block_number Int64,
    transaction_index Int64,
    from_address String,
    to_address String,
    value Int64,
    gas Int64,
    gas_price Int64,
    input String,
    block_timestamp Int64,
    max_fee_per_gas Int64,
    max_priority_fee_per_gas Int64,
    transaction_type Int64
)
ENGINE = MergeTree
PRIMARY KEY (hash)
ORDER BY (hash)
'''
chClient.execute(sql)