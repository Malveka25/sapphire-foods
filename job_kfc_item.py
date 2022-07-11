import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from datetime import datetime, timedelta
import boto3
import pg8000
import pandas as pd


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sparkContext = SparkContext()
glueContext = GlueContext(sparkContext)
sparkSession = glueContext.spark_session
glueJob = Job(glueContext)
glueJob.init(args['JOB_NAME'], args)
client = boto3.client('s3')


##### Creds need to be removed from code
host = '192.168.10.75'
port = '1433'
database = 'POSSQLDB'
user = 'Ganit'
password = 'G@nit@1234'
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

bucket_name = 'sapphire-raw-data'
folder_name = 'item_master'
table_name = 'item'
schema_name = 'dev_schema'
dest_database = 'sapphire'

# dest creds
schema_name = "dev_schema"
source_schema = "kfc"
dest_database = "sapphire"
dest_table_name = "etl_meta_data"
dest_host = "prod-sapphire-analytics.co0tpxwuxsbi.ap-south-1.redshift.amazonaws.com"
dest_port = "5439"
dest_user = "ganit_admin_user"
dest_password = "cn2!*HC6^2!%S*"
aws_role = "arn:aws:iam::875560361730:role/AmazonRedshiftAllCommands"

#variables
date_today = datetime.now()
date_today_string= str(date_today.strftime('%Y%m%d_%H%M'))
first_day =(datetime.now().date() - timedelta(days=7)).strftime("%Y-%m-%d")


# Job Details
job_name = str(args['JOB_NAME'])
load_type = "Full Load"
service = 'glue'
stat = 'Inprogress'
mssql_count = 0 
rshift_count = 0

#Output_Path
outpath_path = "s3://{0}/{1}/{2}/{3}".format(bucket_name, source_schema, table_name, date_today_string)
outpath_key = source_schema+'/'+table_name+'/'+date_today_string+'/'


# for object in client.list_objects(Bucket=bucket_name,Prefix=outpath_key)['Contents']:
#     print('Deleting', object['Key'])
#     client.delete_object(Bucket=bucket_name, Key=object['Key'])
try: 
    # Redshift connection
    conn = pg8000.connect(
        database=dest_database,
        host=dest_host,
        port=dest_port,
        user=dest_user,
        password=dest_password,
    )
    
    cursor= conn.cursor()
    
    q1 = """insert into {0}.{1} values ('{2}','{3}', '{4}', '{5}' ,'{12}', '{6}', {7}, {8}, '{9}', '{10}','{11}', '')""".format(schema_name,dest_table_name,date_today_string,job_name, database, 'Sapphire Foods India Pvt_ Ltd_$Item' , table_name, mssql_count, rshift_count, service, stat,load_type,source_schema)
    print(q1)
    print("**********************************************************************************************************************************************************************************")
    cursor.execute(q1)
    conn.commit()
    
    ##### Query to fetch all records from Tender Type Master Table - Full Load
    query_kfc1 = """(SELECT [timestamp]
          ,[No_]
          ,[No_ 2]
          ,[Description]
          ,[Search Description]
          ,[Description 2]
          ,[Base Unit of Measure]
          ,[Price Unit Conversion]
          ,[Type]
          ,[Inventory Posting Group]
          ,[Shelf No_]
          ,[Item Disc_ Group]
          ,[Allow Invoice Disc_]
          ,[Statistics Group]
          ,[Commission Group]
          ,[Unit Price]
          ,[Price_Profit Calculation]
          ,[Profit _]
          ,[Costing Method]
          ,[Unit Cost]
          ,[Standard Cost]
          ,[Last Direct Cost]
          ,[Indirect Cost _]
          ,[Cost is Adjusted]
          ,[Allow Online Adjustment]
          ,[Vendor No_]
          ,[Vendor Item No_]
          ,[Lead Time Calculation]
          ,[Reorder Point]
          ,[Maximum Inventory]
          ,[Reorder Quantity]
          ,[Alternative Item No_]
          ,[Unit List Price]
          ,[Duty Due _]
          ,[Duty Code]
          ,[Gross Weight]
          ,[Net Weight]
          ,[Units per Parcel]
          ,[Unit Volume]
          ,[Durability]
          ,[Freight Type]
          ,[Tariff No_]
          ,[Duty Unit Conversion]
          ,[Country_Region Purchased Code]
          ,[Budget Quantity]
          ,[Budgeted Amount]
          ,[Budget Profit]
          ,[Blocked]
          ,[Block Reason]
          ,FORMAT ([Last DateTime Modified], 'yyyy-MM-dd hh:mm:ss')[Last DateTime Modified]
          ,convert(date,[Last Date Modified])[Last Date Modified]
          ,FORMAT ([Last Time Modified], 'yyyy-MM-dd hh:mm:ss')[Last Time Modified]
          ,[Price Includes VAT]
          ,[VAT Bus_ Posting Gr_ (Price)]
          ,[Gen_ Prod_ Posting Group]
          ,[Picture]
          ,[Country_Region of Origin Code]
          ,[Automatic Ext_ Texts]
          ,[No_ Series]
          ,[Tax Group Code]
          ,[VAT Prod_ Posting Group]
          ,[Reserve]
          ,[Global Dimension 1 Code]
          ,[Global Dimension 2 Code]
          ,[Stockout Warning]
          ,[Prevent Negative Inventory]
          ,[Application Wksh_ User ID]
          ,[Assembly Policy]
          ,[GTIN]
          ,[Default Deferral Template Code]
          ,[Low-Level Code]
          ,[Lot Size]
          ,[Serial Nos_]
          ,convert(date,[Last Unit Cost Calc_ Date])[Last Unit Cost Calc_ Date]
          ,[Rolled-up Material Cost]
          ,[Rolled-up Capacity Cost]
          ,[Scrap _]
          ,[Inventory Value Zero]
          ,[Discrete Order Quantity]
          ,[Minimum Order Quantity]
          ,[Maximum Order Quantity]
          ,[Safety Stock Quantity]
          ,[Order Multiple]
          ,[Safety Lead Time]
          ,[Flushing Method]
          ,[Replenishment System]
          ,[Rounding Precision]
          ,[Sales Unit of Measure]
          ,[Purch_ Unit of Measure]
          ,[Time Bucket]
          ,[Reordering Policy]
          ,[Include Inventory]
          ,[Manufacturing Policy]
          ,[Rescheduling Period]
          ,[Lot Accumulation Period]
          ,[Dampener Period]
          ,[Dampener Quantity]
          ,[Overflow Level]
          ,[Manufacturer Code]
          ,[Item Category Code]
          ,[Created From Nonstock Item]
          ,[Product Group Code]
          ,[Service Item Group]
          ,[Item Tracking Code]
          ,[Lot Nos_]
          ,[Expiration Calculation]
          ,[Warehouse Class Code]
          ,[Special Equipment Code]
          ,[Put-away Template Code]
          ,[Put-away Unit of Measure Code]
          ,[Phys Invt Counting Period Code]
          ,convert(date,[Last Counting Period Update])[Last Counting Period Update]
          ,[Use Cross-Docking]
          ,convert(date,[Next Counting Start Date])[Next Counting Start Date]
          ,convert(date,[Next Counting End Date])[Next Counting End Date]
          ,[Id]
          ,[Unit of Measure Id]
          ,[Tax Group Id]
          ,[Sales Blocked]
          ,[Purchasing Blocked]
          ,[Item Category Id]
          ,[IsDeal]
          ,[Veg]
          ,[Item Price Change Allowed]
          ,[Exclude From KDS]
          ,[Not Inlcude In Net Sales]
          ,[Emplyoee Meal Item]
          ,convert(date,[Date Created])[Date Created]
          ,[Created by User]
          ,[Division Code]
          ,[Retail Product Code]
          ,[Last Modified by User]
          ,[Item Error Check Code]
          ,[Item Error Check Status]
          ,[Suggested Qty_ on POS]
          ,[Item Capacity Value]
          ,[Qty not in Decimal]
          ,[Warranty Card]
          ,[Def_ Ordered by]
          ,[Def_ Ordering Method]
          ,[Original Vendor No_]
          ,[Original Vendor Item No_]
          ,[BOM Method]
          ,[BOM Receipt Print]
          ,[Recipe Version Code]
          ,[Recipe Item Type]
          ,[BOM Cost Price Distribution]
          ,[BOM Type]
          ,[BOM Receiving Explode]
          ,[External Item No_]
          ,[Extern_ Size+Crust]
          ,[Variant Framework Code]
          ,[Season Code]
          ,[Lifecycle Length]
          ,convert(date,[Lifecycle Starting Date])[Lifecycle Starting Date]
          ,convert(date,[Lifecycle Ending Date])[Lifecycle Ending Date]
          ,[Error Check Internal Usage]
          ,[Attrib 1 Code]
          ,[Attrib 2 Code]
          ,[Attrib 3 Code]
          ,[Attrib 4 Code]
          ,[Attrib 5 Code]
          ,[ABC Sales]
          ,[ABC Profit]
          ,[Wastage _]
          ,[Excluded from Portion Weight]
          ,[Unaff_ by Multipl_ Factor]
          ,[Exclude from Menu Requisition]
          ,[Recipe No_ of Portions]
          ,[Max_ Modifiers No Price]
          ,[Max_ Ingr_ Removed No Price]
          ,[Max_ Ingr_ + Modifiers]
          ,[Production Time (Min_)]
          ,[Recipe Main Ingredient]
          ,[Recipe Style]
          ,[Recipe Category]
          ,[Available as Dish]
          ,[UOM Pop-up on POS]
          ,[Replenishment Calculation Type]
          ,[Manual Estimated Daily Sale]
          ,[Store Stock Cover Reqd (Days)]
          ,[Wareh Stock Cover Reqd (Days)]
          ,[Replenishment Sales Profile]
          ,[Replenishment Grade Code]
          ,[Exclude from Replenishment]
          ,[Transfer Multiple]
          ,[Store Forward Sales Profile]
          ,[Wareh_ Forward Sales Profile]
          ,[Purch_ Order Delivery]
          ,[Replenish as Item No_]
          ,[Profit Goal _]
          ,[Replen_ Data Profile]
          ,[Like for Like Replen_ Method]
          ,[Like for Like Process Method]
          ,[Replenish as Item No - Method]
          ,[Replen_ Transfer Rule Code]
          ,[Select Lowest Price Vendor]
          ,[Effective Inv_ Sales Order]
          ,[Effective Inv_ Purchase Ord_]
          ,[Effective Inv_ Transfer Inb_]
          ,[Effective Inv_ Transfer Outb_]
          ,[LS Forecast Method]
          ,[LS Forecast Calc_ Horizon]
          ,[Fuel Item]
          ,[Routing No_]
          ,[Production BOM No_]
          ,[Single-Level Material Cost]
          ,[Single-Level Capacity Cost]
          ,[Single-Level Subcontrd_ Cost]
          ,[Single-Level Cap_ Ovhd Cost]
          ,[Single-Level Mfg_ Ovhd Cost]
          ,[Overhead Rate]
          ,[Rolled-up Subcontracted Cost]
          ,[Rolled-up Mfg_ Ovhd Cost]
          ,[Rolled-up Cap_ Overhead Cost]
          ,[Order Tracking Policy]
          ,[Critical]
          ,[Item Family Code]
          ,[Unit Price Including VAT]
          ,[POS Cost Calculation]
          ,[No Stock Posting]
          ,[Zero Price Valid]
          ,[Qty_ Becomes Negative]
          ,[No Discount Allowed]
          ,[Keying in Price]
          ,[Scale Item]
          ,[Keying in Quantity]
          ,[Skip Compression When Scanned]
          ,[Skip Compression When Printed]
          ,[Barcode Mask]
          ,[Use EAN Standard Barc_]
          ,[Qty_ per Base Comp_ Unit]
          ,[Base Comp_ Unit Code]
          ,[Comparison Unit Code]
          ,[Comp_ Price Incl_ VAT]
          ,[Explode BOM in Statem_ Posting]
          ,[Dispense Printer Group]
          ,[Print Variants Shelf Labels]
          ,[Common Item No_]
          ,[Tare Weight]
          ,[Lifecycle Curve Code]
          ,[Dimension Pattern Code]
          ,[Upd_ Cost and Weight w_Posting]
          ,[Store Coverage Days Profile]
          ,[Wareh Coverage Days Profile]
          ,[Dummy]
      FROM [POSSQLDB].[dbo].[Sapphire Foods India Pvt_ Ltd_$Item]
    ) kfcquery"""
    
    
    kfcDF1 = sparkSession.read \
        .format("jdbc") \
        .option("url", "jdbc:sqlserver://{0}:{1};databaseName={2}".format(host,port,database)) \
        .option("dbtable", query_kfc1) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .load()
    
    
    
    # kfc_count = """select count(*) from [POSSQLDB].[dbo].[Sapphire Foods India Pvt_ Ltd_$Item]"""
    # kfc_count_df = sparkSession.read \
    #     .format("jdbc") \
    #     .option("url", "jdbc:sqlserver://{0}:{1};databaseName={2}".format(host,port,database)) \
    #     .option("dbtable", kfc_count) \
    #     .option("user", user) \
    #     .option("password", password) \
    #     .option("driver", driver) \
    #     .load()
        
    mssql_count = kfcDF1.count()
    
    kfcDF1.write.option("header","true").option("sep","|").csv(outpath_path)
    
    print("Completed writing to s3")
    
    #listing objects in s3 bucket to get the csv file
    
    file_list = []
    print(outpath_path)
    for files in client.list_objects(Bucket=bucket_name,Prefix=outpath_key)['Contents']:
        file_list.append(files["Key"])
    
    objects = list(filter(lambda x: x[-3:].lower()=="csv", file_list)) 
    
    master_file_path = (objects[0])                 
    master_file = master_file_path.split('/')[-1]
    
    file_path = outpath_path + '/' + master_file
    
    #delete old records from redshift
    cur = conn.cursor()
    
    cur.execute(f"""
    BEGIN;
    truncate table {schema_name}.{table_name};
    commit;
    """)
    
    
    #copy new records to redshift
    cur.execute(f"""
    BEGIN;
    copy {schema_name}.{table_name} from '{file_path}'
    iam_role 'arn:aws:iam::875560361730:role/AmazonRedshiftAllCommands'
    DELIMITER AS '|' IGNOREHEADER 1
    NULL AS 'NULL'
    EMPTYASNULL
    REMOVEQUOTES;
    commit;
    """)

    # update variables if stat successs

    stat = 'Success'

    cursor = conn.cursor()
    

    q2 = """SELECT count(*) FROM {0}.{1}""".format(source_schema,table_name)
    print(q2)
    print("**********************************************************************************************************************************************************************************")
    cursor.execute(q2)
    conn.commit()
    rc = cursor.fetchall()
    rcdf = pd.DataFrame(rc)
    rshift_count = rcdf.iloc[0,0]
    print("query 2")
    print("**********************************************************************************************************************************************************************************")

    q3 = """Update {0}.{1} set status = '{2}', mssql_row_count = {3}, redshift_row_count = {4} where glue_job_name = '{5}' and date = '{6}'
                    """.format(schema_name,dest_table_name,str(stat),mssql_count,rshift_count,job_name,date_today_string)
    print(q3)
    print("**********************************************************************************************************************************************************************************")

    cursor.execute(q3)
    conn.commit()

    
    print("query 3")
    print("**********************************************************************************************************************************************************************************")


except Exception as e:
    
    # update variables if stat failure
    schema_name = "dev_schema"
    dest_table_name = "etl_meta_data"
    job_name = str(args['JOB_NAME'])
    stat = 'Failed'
    msg = str(repr(e)).replace('"','')
    msg = msg.replace("'",'')
    print(msg)
    cursor = conn.cursor()
    print("in except")
    print("**********************************************************************************************************************************************************************************")
    
    try:
        q4 = """ Begin;
                  Update {0}.{1} set  status = '{2}', comment = '{3}' where glue_job_name = '{4}' and date = '{5}';
                  commit;
                  """.format(schema_name,dest_table_name,stat,msg,job_name,date_today_string)
                  
        print(q4)
        print("**********************************************************************************************************************************************************************************")
        
        
        cursor.execute(q4)
        conn.commit()
        
    except Exception as e1:
        
        q5 = """ Begin;
                  Update {0}.{1} set  status = '{2}', comment = '{3}' where glue_job_name = '{4}' and date = '{5}';
                  commit;
                  """.format(schema_name,dest_table_name,stat,str(e1),job_name,date.today())
                  
        print(q5)
        print("**********************************************************************************************************************************************************************************")
        
        
        cursor.execute(q5)
        conn.commit()
        print(str(repr(e1)))
  
    conn.commit()
    
    print("query except")
    print("**********************************************************************************************************************************************************************************")
    
conn.commit()



print("{0}.{1} data Job Completed Successfully !!!".format(schema_name, table_name))

glueJob.commit()


