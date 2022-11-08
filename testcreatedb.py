import sqlite3

conn = sqlite3.connect('luaArbTest.db')
c = conn.cursor()
c.execute('''
    CREATE TABLE blocks_raw (
        number INTEGER,
        hash TEXT,
        parent_hash TEXT,
        nonce TEXT,
        sha3_uncles TEXT,
        logs_bloom TEXT,
        transactions_root TEXT,
        state_root TEXT,
        receipts_root TEXT,
        miner TEXT,
        difficulty INTEGER,
        total_difficulty INTEGER,
        size INTEGER,
        extra_data TEXT,
        gas_limit INTEGER,
        gas_used INTEGER,
        timestamp INTEGER,
        transaction_count INTEGER,
        base_fee_per_gas INTEGER,
        l1blocknumber INTEGER
    )
''')

c.execute('''
    CREATE TABLE transactions_raw (
        hash TEXT,
        nonce INTEGER,
        block_hash TEXT,
        block_number INTEGER,
        transaction_index INTEGER,
        from_address TEXT,
        to_address TEXT,
        value INTEGER,
        gas INTEGER,
        gas_price INTEGER,
        input TEXT,
        block_timestamp INTEGER,
        max_fee_per_gas INTEGER,
        max_priority_fee_per_gas INTEGER,
        transaction_type INTEGER
    )
''')