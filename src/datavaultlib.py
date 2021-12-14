
import yaml
def getdbcred():
    '''loads credentials and server data from conf.yaml file'''
    with open('./conf.yaml', 'r') as file:
        conf = yaml.safe_load(file)
        host= conf['db']['host']
        port=conf['db']['port']
        user=conf['db']['user']
        password=conf['db']['passwd']
    return host,port,user,password

def clean_all():
    '''Drops all Project related Tables and Schemas'''
    connection, cursor = db_con()
    queries=["DROP SCHEMA IF EXISTS bi_staging","DROP SCHEMA IF EXISTS bi_cdwh"]
    for query in queries:
        cursor.execute(query)
    
    connection.commit()
    connection.close()
    
import mysql.connector
def db_con():
    '''
    establishes database connection defined in conf.yaml
    '''
    try:
        host,port,user,password = getdbcred()
    except Exception as e: print(e)

    # Connect to server
    connection = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password)
    cursor = connection.cursor(buffered=True)
    return connection, cursor


def createtbl(schema,table,columns,droptbl):
    connection, cursor = db_con()
    if droptbl:
        cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
    query=f'''CREATE TABLE IF NOT EXISTS {schema}.{table} ({",".join(columns)})'''
    try: cursor.execute(query)
    except Exception as e: print(e)

def add2db(schema, tblname, columns, records):
    ''' add2db(schema, tblname, columns, paramnr, records)
        schema: Name of Database 
        tblname: Name of table to append to
        columns: ame of table columns
        records: List of Tuples, each containing data for one row'''
    connection, cursor = db_con()
    errorcnt=0
    addedRecords=0
    rownr=0
    paramnr=len(columns)
    query="INSERT INTO {}.{} {} VALUES (%s{})".format(schema, tblname,str(tuple(columns)).replace("'",""),",%s"*(paramnr-1))
    "INSERT INTO customers (name, address) VALUES (%s, %s)"
    for row in records:
        
        try: 
            cursor.execute(query,row)
            addedRecords+=1
        except Exception as e: 
            print(e, rownr, row)
            errorcnt+=1
            if errorcnt >5:
                break
        rownr+=1
    connection.commit()
    connection.close()
    print("{} records were added to {}".format(addedRecords,tblname))    

#Methode to generate HUBs
def hub_gen(hublst=None,cdwhschema="bi_cdwh"):
    '''
    cdwhschema: Name of DWH schema name
    hublst: List of Tuples, each containing HUB_Name, BusinessKey and Datatype
    '''
    connection, cursor = db_con()
    for bk in hublst:
        tblname,bk, datatype = bk
        hshcol=tblname+"_HSH"
        query=f'''CREATE TABLE IF NOT EXISTS {cdwhschema}.{tblname} ({hshcol} CHAR(32),
                                                                    {bk} {datatype},
                                                                    LOAD_DTS TIMESTAMP,
                                                                    SRC varchar(64),
                                                                    PRIMARY KEY ({hshcol})
                                                                )ENGINE=INNODB;'''
        try: 
            cursor.execute(query)
            print(f"HUB was created: {tblname}")
        except Exception as e: print(f"Generating {tblname} failed\n{e}\n{query}")
    connection.commit()
    connection.close()
        
def link_gen(cdwhschema="bi_cdwh",tblname=None,linklst=None,):
    '''
    cdwhschema: Name of DWH schema name
    tblname: Name of Table
    linklst: List of Tuples, each containing LINK_Name and List of Linked HUB's
       
    '''
    connection, cursor = db_con()
    for link in linklst:
        tblname, hubs=link
        query=f'''CREATE TABLE IF NOT EXISTS {cdwhschema}.{tblname}(
                                            {tblname}_HSH CHAR(32),
                                            {','.join([hub+"_HSH CHAR(32)" for hub in hubs])},
                                            LOAD_DTS TIMESTAMP,
                                            SRC varchar(64),
                                            PRIMARY KEY ({tblname}_HSH),
                                            {','.join([f"FOREIGN KEY({hub}_HSH) REFERENCES {cdwhschema}.{hub}({hub}_HSH)" for hub in hubs])}
                                            )ENGINE=INNODB;'''
        try: 
            cursor.execute(query)
            print(f"LINK was created: {tblname}")
        except Exception as e: print(f"Creating {tblname} failed!\n{e}\n{query}")
    connection.commit()
    connection.close()
        
def sat_gen(cdwhschema="bi_cdwh",satlst=[]):
    '''
    cdwhschema: Name of DWH schema name
    satlst: List Tuples, each containing LINK_Name and List of Linked HUB's
    '''
    connection, cursor = db_con()
    for sat in satlst:
        tblname, hub,attributes = sat
        attstring=""
        for att in attributes:
            attstring+=' '.join(att)+','
        hsh_diflst=[]
        for att in attributes:
            hsh_diflst.append(att[0])
        query=f'''CREATE TABLE IF NOT EXISTS {cdwhschema}.{tblname} (LOAD_DTS TIMESTAMP, 
                                        {hub}_HSH CHAR(32),
                                        {attstring}
                                        SRC varchar(64),
                                        HSH_DIF CHAR(32) AS (MD5(concat({','.join(hsh_diflst)}))) PERSISTENT,
                                        FOREIGN KEY ({hub}_HSH) REFERENCES {cdwhschema}.{hub}({hub}_HSH),
                                        PRIMARY KEY (LOAD_DTS,{hub}_HSH))ENGINE=INNODB;                            
              '''
        try: 
            cursor.execute(query)
            print(f"SAT was created: {tblname}")
        except Exception as e: print(f"Creation of {tblname} failed!\n{e}\n{query}")
    connection.commit()
    connection.close()

#Methode to write to HUB_Tables
def hub_insert (cdwhschema="bi_cdwh",stagingschema="bi_staging",cdwhtbl=None,stagingtbl=None,bk=None,columns=None,debug=False):
    '''
    cdwhschema: Name of DWH schema name to insert data to
    stagingschema: Name of Staging schema to select data from
    cdwhtbl: Name of Table to insert to
    stagingtbl: name of staging table to select data from
    bk: Name of column containing business key
    columns: List containing all columns
    debug: default False. If True prints SQL query to console
    '''
    connection, cursor = db_con()
    str_lst_stripe=lambda x: str(x)[2:-2].replace("'","")    
    columnsstr=str_lst_stripe(columns)
    query=f"INSERT INTO {cdwhschema}.{cdwhtbl} SELECT MD5({bk}),{','.join(columns)} FROM {stagingschema}.{stagingtbl} GROUP BY {bk}"
    try:
        cursor.execute(query)
        print(f"Insert into {cdwhtbl} was successfull")
    except Exception as e: print(f"Insert into HUB_{bk} failed:\n",e)

    if debug: print(query)
    connection.commit()
    connection.close()
#Methode to write to LINK_Tables
def link_insert(cdwhschema="bi_cdwh",stagingschema="bi_staging",cdwhtbl=None,stagingtbl=None,fklst=None,debug=False):
    '''
    cdwhschema: Name of DWH schema name to insert data to
    stagingschema: Name of Staging schema to select data from
    cdwhtbl: Name of Table to insert to
    stagingtbl: name of staging table to select data from
    fklist: List containing all columns business keys of HUB's to link
    debug: default False. If True prints SQL query to console
    '''
    connection, cursor = db_con()
    query=f'''INSERT INTO {cdwhschema}.{cdwhtbl} SELECT 
                MD5(Concat({''.join([f"MD5({x}), "for x in fklst])[:-2]})), {''.join([f"MD5({x}), "for x in fklst])[:-1]} LOAD_DTS,SRC
                FROM {stagingschema}.{stagingtbl}
                GROUP BY {','.join(fklst)};'''
    try: 
        cursor.execute(query)
        print(f"Insert to {cdwhtbl} was successfull")
    except Exception as e: print("Something went wrong", e)
    if debug: print(query)
    connection.commit()
    connection.close()
   
    
    
def sat_insert (cdwhschema="bi_cdwh",stagingschema="bi_staging",cdwhtbl=None,stagingtbl=None,bk=None,columns=None,hub=None,debug=False):
    '''
    cdwhschema: Name of DWH schema name to insert data to
    stagingschema: Name of Staging schema to select data from
    cdwhtbl: Name of Table to insert to
    stagingtbl: name of staging table to select data from
    bk: business key of HUB referenced by SAT 
    columns: List of all columns of SAT
    debug: default False. If True prints SQL query to console
    '''
    connection, cursor = db_con()
    query=f'''INSERT INTO {cdwhschema}.{cdwhtbl} (LOAD_DTS,{hub+"_HSH"},{','.join(columns)},SRC)
            SELECT LOAD_DTS,MD5({bk}),{','.join(columns)},SRC
            FROM {stagingschema}.{stagingtbl} GROUP BY {bk};'''
    try:
        cursor.execute(query)
        print(f"Insert into {cdwhtbl} was successfull")
    except Exception as e: print("Record might already be existing, otherwise follow error message:\n",e,query)
    if debug: print(query)
    connection.commit()
    connection.close()
    
