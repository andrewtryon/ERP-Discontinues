from dotenv import load_dotenv
load_dotenv()
import os
from subprocess import Popen
import datetime
import time
from datetime import datetime, date, timedelta
import os
import logging
import logzero
import pandas as pd
import pyodbc
import json
import requests
import subprocess

def flatten_json(nested_json, exclude=['']):
    """Flatten json object with nested keys into a single level.
        Args:
            nested_json: A nested json object.
            exclude: Keys to exclude from output.
        Returns:
            The flattened json object if successful, None otherwise.
    """
    out = {}

    def flatten(x, name='', exclude=exclude):
        if type(x) is dict:
            for a in x:
                if a not in exclude: flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out    

def makeWrikeTask (title = "New Pricing Task", description = "No Description Provided", status = "Active", assignees = "KUAAY4PZ", folderid = "IEAAJKV3I4JBAOZD"):
    url = "https://www.wrike.com/api/v4/folders/" + folderid + "/tasks"
    querystring = {
        'title':title,
        'description':description,
        'status':status,
        'responsibles':assignees
        } 
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
        }        
    response = requests.request("POST", url, headers=headers, params=querystring)
    print(response)
    return response

def attachWrikeTask (attachmentpath, taskid):
    url = "https://www.wrike.com/api/v4/tasks/" + taskid + "/attachments"
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
    }

    files = {
        'X-File-Name': (attachmentpath, open(attachmentpath, 'rb')),
    }

    response = requests.post(url, headers=headers, files=files)
    return response     


if __name__ == '__main__':

    Week_ago = datetime.today()- timedelta(days=7)
    print(Week_ago)    
    time.sleep(3) 

    #ProductLine ItemCode UDF_REPLACEMENT_ITEM
    #Audit Sage for new Inactives + Add to Rolling List
    #Look for Doug's Return Data + Load to Sage + Remove those from Rolling List
    #Create Wrike Task based on rolling list

    current_run_time = datetime.today().strftime("%Y-%m-%d") # - timedelta(hours=12)
    print(current_run_time)

    #Path to where stuff should go
    workingdir = '\\\\FOT00WEB\\Alt Team\\Kris\\GitHubRepos\\discos\\'
    cmlinedir = 'Y:\\Kris\\GitHubRepos\\discos\\'

    #can't remember why i have this
    pd.options.display.max_colwidth = 9999
    #or this
    logzero.loglevel(logging.WARN)    

    #Need to first Process Returned Replacements
    compiledSubmittedDFs = pd.DataFrame(data=None)  
    directory = os.fsencode(workingdir + 'ReviewRequired\\submitted\\')
        
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        if filename.endswith(".xlsx"): 
            print(str(file))
            submittedFileDF = pd.read_excel(workingdir + 'ReviewRequired\\submitted\\' + filename, dtype={'ItemCode': str})
            print(submittedFileDF.shape[0])
            print(list(submittedFileDF))
            if ('ItemCode' in list(submittedFileDF)) & ('UDF_REPLACEMENT_ITEM' in list(submittedFileDF)) & ('UDF_DISCONTINUED_STATUS' in list(submittedFileDF)):
                
                print("File validated")
                #Drop Cols we don't need
                submittedFileDF.drop(submittedFileDF.columns.difference(['ItemCode','UDF_REPLACEMENT_ITEM','UDF_DISCONTINUED_STATUS']), 1, inplace=True)
                #Appened to compiled df 
                compiledSubmittedDFs = compiledSubmittedDFs.append(submittedFileDF, sort=False)
                #Move File to Processed and attach date
                os.rename(workingdir + 'ReviewRequired\\submitted\\' + filename, workingdir + 'ReviewRequired\\processed\\' + (current_run_time + '_' + filename.replace(current_run_time + '_','')))
            else:
                print("ItemCode and/or UDF_REPLACEMENT_ITEM not found")
                #tag with failed validation
                os.rename(workingdir + 'ReviewRequired\\submitted\\' + filename, workingdir + 'ReviewRequired\\submitted\\' + ('FailedValidation_' + filename.replace('FailedValidation_' ,'')))
            continue
        else:
            print("Not formatted in xlsx")
            continue       

    compiledSubmittedDFs = compiledSubmittedDFs[~compiledSubmittedDFs.index.duplicated(keep='last')]
    #compiledSubmittedDFs.drop_duplicates(inplace =True, subset='ItemCode') 
    
    #If we have submitted data
    if compiledSubmittedDFs.shape[0] > 0:
        #prepare df
        compiledSubmittedDFs.set_index('ItemCode', inplace=True)
        #Fill empty disco status
        compiledSubmittedDFs.loc[compiledSubmittedDFs['UDF_DISCONTINUED_STATUS'].isnull(), 'UDF_DISCONTINUED_STATUS'] = '20 - Obsolete: Legacy'
        #Overwrite disco status with '30 - Obsolete: Replacement Provided' for anythng with a replacement...but is not set to '40 - Item Code Error: Deletion' or '40 - Item Code Error: Deletion'
        compiledSubmittedDFs.loc[(~compiledSubmittedDFs['UDF_REPLACEMENT_ITEM'].isnull()) & ((compiledSubmittedDFs['UDF_DISCONTINUED_STATUS'] != '40 - Item Code Error: Deletion') | (compiledSubmittedDFs['UDF_DISCONTINUED_STATUS'] != '40 - Item Code Error: Deletion')), 'UDF_DISCONTINUED_STATUS'] = '30 - Obsolete: Replacement Provided'

        
        #remove from rolling pickledf
        rollingPicklesDf = pd.read_pickle(workingdir + 'DONOTTOUCH\\datapickles.p')
        print(rollingPicklesDf)
        rollingPicklesDf.drop(compiledSubmittedDFs.index, inplace= True)
        print(rollingPicklesDf)
        rollingPicklesDf.to_pickle(workingdir + 'DONOTTOUCH\\datapickles.p') 

        #Auto VI Replacements        
        #sage data batch file
        print('syncing: ' + str(compiledSubmittedDFs.shape[0]))
        compiledSubmittedDFs.to_csv(workingdir + 'DONOTTOUCH\\AutoBatching\\Auto_Replacements_VIWI7B.csv', columns=['UDF_REPLACEMENT_ITEM','UDF_DISCONTINUED_STATUS'], header=False, sep='|', index=True) 
        print('to csv')
        time.sleep(15) 
        path = cmlinedir + 'DONOTTOUCH\\AutoBatching'
        print(path)
        p = subprocess.Popen('Auto_Replacements_VIWI7B.bat', cwd= path, shell = True)
        stdout, stderr = p.communicate()   
        p.wait()
        print('to sage done')
    else:
        print("nothing submiittted")

    #Sage Connection Stuff  
    sage_conn_str = os.environ.get(r"sage_conn_str").replace("UID=;","UID=" + os.environ.get(r"sage_login") + ";").replace("PWD=;","PWD=" + os.environ.get(r"sage_pw") + ";")      

    #Establish sage connection
    sage_cnxn = pyodbc.connect(sage_conn_str, autocommit=True)
    #SQL Sage data into dataframe
    SageSQLquery = """
        SELECT CI_Item.ItemCode, CI_Item.InactiveItem, CI_Item.ProductType, CI_Item.UDF_REPLACEMENT_ITEM, CI_Item.UDF_DISCONTINUED_STATUS, CI_Item.UDF_VENDOR_PRICE_DATE, 
            CI_Item.UDF_PRODUCT_NAME_150, CI_Item.DateCreated, CI_Item.DefaultWarehouseCode, CI_Item.PrimaryVendorNo, CI_Item.ProductLine, UDF_SPECIALORDER, LastSoldDate, LastReceiptDate,
            CI_Item.UDF_CATEGORY1, CI_Item.UDF_CATEGORY2, CI_Item.UDF_CATEGORY3, CI_Item.UDF_CATEGORY4, CI_Item.UDF_CATEGORY5, CI_Item.UDF_CATEGORY_ID,
            IM_ItemWarehouse.QuantityOnHand, IM_ItemWarehouse.QuantityOnPurchaseOrder, IM_ItemWarehouse.QuantityOnSalesOrder, IM_ItemWarehouse.QuantityOnBackOrder,
            CI_Item.UDF_VENDOR_STOCK_LEVEL, CI_Item.UDF_VENDOR_STOCK_LEVEL_DATE
        FROM CI_Item CI_Item, IM_ItemWarehouse IM_ItemWarehouse
        WHERE ((CI_Item.ProductType = 'D') AND (CI_Item.InactiveItem <> 'Y')) AND 
            CI_Item.ItemCode = IM_ItemWarehouse.ItemCode"""
                                   
    #Execute SQL
    print('Retrieving Sage data1')
    SageDiscoDF = pd.read_sql(SageSQLquery,sage_cnxn)#.set_index('ItemCode', drop=True).reset_index()

    SageDiscoDF['UDF_VENDOR_PRICE_DATE'] = pd.to_datetime(SageDiscoDF['UDF_VENDOR_PRICE_DATE'])
    SageDiscoDF['UDF_VENDOR_STOCK_LEVEL_DATE'] = pd.to_datetime(SageDiscoDF['UDF_VENDOR_STOCK_LEVEL_DATE'])
    SageDiscoDF['UDF_VENDOR_STOCK_LEVEL'].fillna('0', inplace=True)    
    discoDF = SageDiscoDF.copy()

    print(discoDF['UDF_VENDOR_STOCK_LEVEL'])
    discoDF.loc[(discoDF['UDF_VENDOR_STOCK_LEVEL_DATE'] < Week_ago) & (~discoDF['UDF_VENDOR_STOCK_LEVEL_DATE'].isna()) , 'UDF_VENDOR_STOCK_LEVEL'] = 0
    
    #Sum up all the warehouses ....results in disco df of what we need to get rid off
    discoDF = discoDF.groupby('ItemCode')[['QuantityOnHand','QuantityOnPurchaseOrder','QuantityOnSalesOrder','QuantityOnBackOrder','UDF_VENDOR_STOCK_LEVEL']].sum()
    print(discoDF)
    
    discoDF['WareHouseAction'] = discoDF['QuantityOnHand'] + discoDF['QuantityOnPurchaseOrder'] + discoDF['QuantityOnSalesOrder'] + discoDF['QuantityOnBackOrder'] + discoDF['UDF_VENDOR_STOCK_LEVEL']
    discoDF = discoDF.query('WareHouseAction == 0')

    #Make a discodf with single column 'WareHouseAction' .,... not sure I need to do this anymore
    SageDiscoDF = SageDiscoDF.set_index('ItemCode', drop=True)
    common_cols=list(set.intersection(set(SageDiscoDF), set(discoDF)))
    SageDiscoDF = SageDiscoDF.drop(columns=common_cols)#.drop_duplicates().reset_index()
    discoDF = discoDF.reset_index().merge(SageDiscoDF.reset_index(), how='left', left_on='ItemCode', right_on='ItemCode').set_index('ItemCode', drop=True)
    discoDF = discoDF.reset_index().drop_duplicates(subset='ItemCode').set_index('ItemCode', drop=True)    

    #let 'while supplies last stay active 4 weeks past disco date...which should be the vendor date
    discoDF['Fourweeksago'] = datetime.today() - timedelta(weeks=4)
    discoDF = discoDF.drop(discoDF[(discoDF.UDF_DISCONTINUED_STATUS == '35 - Obsolete: While Supplies Last') & (discoDF.UDF_VENDOR_PRICE_DATE > discoDF.Fourweeksago)].index)
    #discoDF = discoDF.drop(discoDF[(discoDF.UDF_DISCONTINUED_STATUS == '75 - Temporarily Unavailable:') & (discoDF.UDF_VENDOR_PRICE_DATE > discoDF.Fourweeksago)].index)
    print('gonna make these inactive and add to rolling list')
    print(discoDF)
    
    #combine with rollingPickles df
    rollingPicklesDf = pd.read_pickle(workingdir + 'DONOTTOUCH\\datapickles.p')
    rollingPicklesDf = rollingPicklesDf.append(discoDF, sort=False)
    #discoDF.drop_duplicates(inplace= True, subset='ItemCode')
    rollingPicklesDf = rollingPicklesDf[~rollingPicklesDf.index.duplicated(keep='last')]
    rollingPicklesDf.to_pickle(workingdir + 'DONOTTOUCH\\datapickles.p') 

    #Ksenyia's ProductURLs
    try:
        from akeneo_api_client.client import Client
    except ModuleNotFoundError as e:
        import sys
        sys.path.append("..")
        from akeneo_api_client.client import Client

    print("connnected")

    AKENEO_CLIENT_ID = os.environ.get("AKENEO_CLIENT_ID")
    AKENEO_SECRET = os.environ.get("AKENEO_SECRET")
    AKENEO_USERNAME = os.environ.get("AKENEO_USERNAME")
    AKENEO_PASSWORD = os.environ.get("AKENEO_PASSWORD")
    AKENEO_BASE_URL = os.environ.get("AKENEO_BASE_URL")   

    akeneo = Client(AKENEO_BASE_URL, AKENEO_CLIENT_ID,
                    AKENEO_SECRET, AKENEO_USERNAME, AKENEO_PASSWORD)

    pandaObject = pd.DataFrame(data=None, columns=['identifier','ProductUrl'])
    pandaObject.set_index('identifier',inplace=True)

    searchparams = """
    {
        "limit": 100,
        "locales": "en_US",
        "attributes": "ProductUrl",
        "search": {
            "ProductUrl":[{
                "operator": "NOT EMPTY"
            }]
        }
    }
    """ 

    result = akeneo.products.fetch_list(json.loads(searchparams))

    go_on = True
    count = 0
    #for i in range(1,8):
    while go_on:
        count += 1
        try:
            print(str(count) + ": normalizing")
            page = result.get_page_items()
            #print(page)
            pagedf = pd.DataFrame([flatten_json(x,['scope','locale','currency','unit']) for x in page])
            pagedf.columns = pagedf.columns.str.replace('values_','')
            pagedf.columns = pagedf.columns.str.replace('_0','')
            pagedf.columns = pagedf.columns.str.replace('_data','')
            pagedf.columns = pagedf.columns.str.replace('_amount','')
            pandaObject = pandaObject.append(pagedf, sort=False)
        except:
            #print(item)
            go_on = False
            break
        go_on = result.fetch_next_page()

    print(pandaObject)
    pandaObject.set_index('identifier', inplace= True)
    rollingPicklesDf['ProductUrl'] = ''
    rollingPicklesDf.update(pandaObject)
    #I think that's it...right?

    #dump rolled file for review
    rollingPicklesDf.drop(columns=['QuantityOnHand', 'QuantityOnPurchaseOrder', 'QuantityOnSalesOrder', 'QuantityOnBackOrder','WareHouseAction','Fourweeksago'], inplace=True)
    rollingPicklesDf.to_excel(workingdir + 'ReviewRequired\\Discontinued-ReplacementReview.xlsx', header=True, index=True)    

    #Only VI stuff if we have stuff to inactivate
    if discoDF.shape[0] > 0:

        #VI inactive Y and review required = N
        print('syncing: ' + str(compiledSubmittedDFs.shape[0]))
        discoDF.to_csv(workingdir + 'DONOTTOUCH\\AutoBatching\\Auto_Inactives_VIWI74.csv', columns=['UDF_REPLACEMENT_ITEM'], header=False, sep=',', index=True) 
        print('to csv')
        time.sleep(15) 
        path = cmlinedir + 'DONOTTOUCH\\AutoBatching'
        print(path)
        p = subprocess.Popen('Auto_Inactives_VIWI74.bat', cwd= path, shell = True)
        stdout, stderr = p.communicate()   
        print('to sage done')

    #Make Wrike Task for Doug on Fridays or when # of items exceeds 250
    if (rollingPicklesDf.shape[0] > 250) | (date.today().weekday() == 4):
    #if True:        
        if rollingPicklesDf.shape[0] > 250:
            assignees = '[KUAAZAC4,KUACOUUA,KUAAY4PZ,KUAAZJ3D]'#Doug Andrew Kris Ksenyia 
        else:
            assignees = '[KUAAZAC4,KUAAZJ3D]'#Doug Ksenyia 
        folderid = 'IEAAJKV3I4FCUOG2' #Discontinuations web  folder ... i think
        description = "These items have recently been inactivated. Please review and enter any missing replacement items.\nPlease only enter a single item code in the UDF_REPLACEMENT_ITEM column, and drop a file into the submitted folder here:\n" + workingdir + "ReviewRequired\\submitted\\" 
        title = current_run_time  + " Disco Process: Replacement Item Review"
        title = "TEST " + current_run_time  + " Disco Process: Replacement Item Review"
        response = makeWrikeTask(title = title, description = description, assignees = assignees, folderid = folderid)
        response_dict = json.loads(response.text)
        print(response_dict)
        taskid = response_dict['data'][0]['id']
        print('Attaching file to ', taskid)
        attachWrikeTask(attachmentpath = workingdir + 'ReviewRequired\\Discontinued-ReplacementReview.xlsx', taskid = taskid)
        print('File attached!') #probably should have a handle for the...can't attach too big
    else:
        print("No wrike task needed")    