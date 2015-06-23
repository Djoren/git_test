

# SQL connection CONSTANT
CNXN_STR = 'DRIVER={SQL Server};SERVER=db1a\Portfolio;UID=ETFuser;PWD=ETFuser_2013;DATABASE=ETFMasterDB;'

# SQL queries CONSTANTS
QRY_BOND = """
    SELECT cusip, initial_date, maturity_date, coupon, issue_size, frequency,
    daycount FROM ETFMasterDb.dbo.fn_ma_BondSpecs() order by cusip
    """
QRY_CALL = """
    SELECT cusip, call_schedule, option_type, delay FROM ETFMasterDb.dbo.fn_ma_BondCallSchedule()
    order by cusip
    """
QRY_PUT = """
    SELECT cusip, put_schedule, option_type, delay FROM ETFMasterDb.dbo.fn_ma_BondPutSchedule()
    order by cusip
    """
QRY_SINK = """
    SELECT cusip, sink_schedule, acceleration, delivery  FROM ETFMasterDb.dbo.fn_ma_BondSinkSchedule()
    order by cusip
    """
QRY_PX = """
    SELECT settle_date, cusip, price FROM ETFMasterDb.dbo.fn_ma_BondPriceData()
    order by settle_date
    """

def getData(cnxn, qry):
    """
    Queries SQL db and creates Dataframe from data
        
    :param cnxn     - (pyodbc connection) Connection to MS SQL database
    :param qry      - (str) Query for importing data from db
    :return Data in DataFrame structure
    """
    # Query database table
    cursor = cnxn.cursor()
    cursor.execute(qry)
    col_names = [h[0] for h in cursor.description] #Column names

    # Transfer data to DataFrame (sql -> list of lists -> df)
    rows = []
    row = cursor.fetchone()
    while row is not None:
        rows.append(list(row))
        row = cursor.fetchone()
    df = pd.DataFrame(rows, columns=col_names)

    cursor.close()
    return df

def genInputFile(df, of_dir, f_name):
    """
    Writes DataFrame to file, where columns are in default order

    :param of_dir  - (str) Output directory path for saving 'input' files
    :param f_name   - (str) 'Input' file name
    :return Data in DataFrame structure
    """
    df.to_csv(of_dir + f_name, '\t', header=False, index=False)

def parseCallData(df):
    """
    Parses columns of Call Dataframe to correct format

    :param df   - (pandas DataFrame) Call data
    :return Final DataFrame
    """
    table = []
    for idx, row in df.iterrows(): #Ignore idx
        row_str = str(row['call_schedule']).replace('CALL_SCHEDULE', '').translate(None, '{=-\r\t\n ')
        row_str = row_str.split('}')
        for part in row_str:
            if part != "" and not part.isspace(): #Check for invalid strings
                date, px = parse("CallDate{}CallPrice{}", part).fixed
                table.append([row['cusip'], date, row['option_type'], px, row['delay']])

    df2 = pd.DataFrame(table, columns=['cusip', 'date', 'option_type', 'px', 'delay'])
    return df2

def parsePutData(df):
    """
    Parses columns of Put Dataframe to correct format

    :param df   - (pandas DataFrame) Put data
    :return Final DataFrame
    """
    table = []
    for idx, row in df.iterrows(): #Ignore idx
        row_str = str(row['put_schedule']).replace('PUT_SCHEDULE', '').translate(None, '{=-\r\t\n ')
        row_str = row_str.split('}')
        for part in row_str:
            if part != "" and not part.isspace(): #Check for invalid strings
                date, px = parse("PutDate{}PutPrice{}", part).fixed
                table.append([row['cusip'], date, row['option_type'], px, row['delay']])

    df2 = pd.DataFrame(table, columns=['cusip', 'date', 'option_type', 'px', 'delay'])
    return df2

def parseSinkData(df):
    """
    Parses columns of Sink Dataframe to correct format

    :param df   - (pandas DataFrame) Sink data
    :return Final DataFrame
    """
    table = []
    for idx, row in df.iterrows(): #Ignore idx
        row_str = str(row['sink_schedule']).replace('SINK_SCHEDULE', '').translate(None, '{=-\r\t\n ')
        row_str = row_str.split('}')
        for part in row_str:
            if part != "" and not part.isspace(): #Check for invalid strings
                date, px, amount = parse("SinkDate{}SinkPrice{}SinkAmount{}", part).fixed
                table.append([row['cusip'], date, row['acceleration'], px, amount, row['delivery']])

    df2 = pd.DataFrame(table, columns=['cusip', 'date', 'acceleration', 'px', 'amount', 'delivery'])
    return df2

def createFiles(of_dir):
    """
    Sets up connection with SQL database and calls on parser and input file generators

    :param of_dir   - (str) output dir for input files
    """
    # Setup Database Connection
    cnxn = p.connect(CNXN_STR)

    # Generate bond file
    df_bond = getData(cnxn, QRY_BOND)
    genInputFile(df_bond, of_dir, 'bond_.txt')

    # Generate call file
    df_call = getData(cnxn, QRY_CALL)
    df_call = parseCallData(df_call)
    genInputFile(df_call, of_dir, 'call_.txt')

    # Generate put file
    df_put = getData(cnxn, QRY_PUT)
    df_put = parsePutData(df_put)
    genInputFile(df_put, of_dir, 'put_.txt')

    # Generate sink file
    df_sink = getData(cnxn, QRY_SINK)
    df_sink = parseSinkData(df_sink)
    genInputFile(df_sink, of_dir, 'sink_.txt')

    # Generate price file
    df_px = getData(cnxn, QRY_PX)
    genInputFile(df_px, of_dir, 'price_.txt')

    # Close connections
    cnxn.commit()
    cnxn.close()

#Courtesy:
    #1) https://pypi.python.org/pypi/parse
    #2) http://pandas.pydata.org/pandas-docs/dev/generated/pandas.DataFrame.html
    #3) https://code.google.com/p/pyodbc/wiki/Cursor
    #4) https://code.google.com/p/pyodbc/wiki/GettingStarted


#http://code.activestate.com/recipes/137270-use-generators-for-fetching-large-db-record-sets/