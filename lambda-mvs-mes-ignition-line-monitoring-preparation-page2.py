def get_secret():
    secret_name = os.getenv("DB_SECRET_NAME")
    client = boto3.client("secretsmanager")
    try:
        response = client.get_secret_value(SecretId=secret_name)
        if "SecretString" in response:
            secret = response["SecretString"]
            return json.loads(secret)
        else:
            decoded_binary_secret = response["SecretBinary"]
            return decoded_binary_secret
    except ClientError as e:
        print(f"Unable to retrieve secret: {e}")
        return None


def lambda_handler(event, context):
    plant_id = event['v'].split('.')[0]
    factory = event['v'].split('.')[1]
    line = event['v'].split('.')[2]
    
    # Database Connection
    connection = psycopg2.connect(
        user = RDS_USERNAME,
        password =  RDS_PASSWORD,
        host  = RDS_HOST,
        port = RDS_PORT,
        database = RDS_DATABASE_NAME   
    )
    cursor = connection.cursor()
    try:
        qs = """
            select
            plant_id, factory, factory_date, line, material_description,
            batch, actual_barrel_weight, actual_barrel_meat, barrel_timestamp,
            actual_timestamp, ready_timestamp, barrel_meat
            from core_tumbler
            where
            plant_id = '{0}'
            and factory = '{1}'
            and line = '{2}'
            and status = 'In Stock'
            and barrel_timestamp >= (
                (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Bangkok') - INTERVAL '48 hours'
            ) AT TIME ZONE 'UTC'
            """
        cursor.execute(qs.format(plant_id, factory, line))
        result = cursor.fetchall()
        df = pd.DataFrame(result)
        if len(df) > 0:
            df.columns = [
                'plant_id', 'factory', 'factory_date', 'line',
                'material_description', 'batch', 'actual_barrel_weight',
                'actual_barrel_meat', 'barrel_timestamp', 'actual_timestamp',
                'ready_timestamp', 'barrel_meat'
            ]
            # find total batch that its status = 'In Stock'
            df_summary = pd.pivot_table(
                df,
                index=['plant_id', 'factory', 'line', 'factory_date', 'batch',
                        'ready_timestamp'],
                values=['barrel_timestamp', 'actual_barrel_weight', 'barrel_meat'],
                aggfunc={
                    'barrel_timestamp': 'nunique',
                    'actual_barrel_weight': 'sum',
                    'barrel_meat': 'max'
                }
            ).reset_index()
            number_of_batch = int(df_summary['batch'].count())
            kg_in_stock = float(df_summary['actual_barrel_weight'].sum())
            number_of_barrel = float(df_summary['barrel_timestamp'].sum())
            db.collection('COLLECTION_PREPARATION_ZONE_PAGE2').document(plant_id + '.' + factory + '.' + line + 'NUMBER_OF_BATCH').set({
                'NUMBER_OF_BATCH': number_of_batch
            })
            db.collection('COLLECTION_PREPARATION_ZONE_PAGE2').document(plant_id + '.' + factory + '.' + line + 'KG_IN_STOCK').set({
                'KG_IN_STOCK': kg_in_stock
            })
            db.collection('prod_mes_ignition_preparation_zone_page1').document(plant_id + '.' + factory + '.' + line + 'OVERALL_TOTAL_KG_IN_STOCK').set({
                'OVERALL_TOTAL_KG_IN_STOCK': kg_in_stock
            })
            db.collection('prod_mes_ignition_preparation_zone_page1').document(plant_id + '.' + factory + '.' + line + 'OVERALL_TOTAL_BARREL_IN_STOCK').set({
                'OVERALL_TOTAL_BARREL_IN_STOCK': number_of_barrel
            })
            key_details = ''
            wanted_key = []
            for index, item in df_summary.iterrows():
                # print(item)
                key = str(item[0]) + str(item[1]) + str(item[2]) + str(item[3]) + str(item[4]) + str(item[5])
                wanted_key.append(key)
                key_details += key + '.'
                data = {
                    'FACTORY_DATE': item[3].strftime("%Y-%m-%d"),
                    'BATCH': item[4],
                    'KG': item[6],
                    'READY_TIMESTAMP': item[5],
                    'BARREL_MEAT': item[7]
                }
                db.collection('COLLECTION_PREPARATION_ZONE_PAGE2').document(key).set(data)
            
            # key for batch details
            db.collection('COLLECTION_PREPARATION_ZONE_PAGE2').document(plant_id + '.' + factory + '.' + line + 'BATCH_DETAIL').set({"KEY":key_details})
            
            # delete unwanted batch detail key
            collection_ref = db.collection('COLLECTION_PREPARATION_ZONE_PAGE2')

            # Retrieve all documents in the collection (or use a more specific query if possible)
            docs = collection_ref.stream()

            # Filter documents whose ID begins with "ID"
            filtered_docs = [doc for doc in docs if doc.id.startswith(plant_id + factory + line)]
            for doc in filtered_docs:
                if doc.id not in wanted_key:
                    db.collection('COLLECTION_PREPARATION_ZONE_PAGE2').document(doc.id).delete()
            
            # Available time
            df_std = pd.DataFrame()
            try:
                rds_connection = psycopg2.connect(
                user = RDS_USERNAME,
                password =  RDS_PASSWORD,
                host  = RDS_HOST,
                port = RDS_PORT,
                database = RDS_DATABASE_NAME
                )
                rds_cursor = rds_connection.cursor()

                qs_std ="""
                    select 
                    material_description, belt_scale_feed_rate
                    from core_master_std where line = '{0}'
                    """
                rds_cursor.execute(qs_std.format(line))
                std_result = rds_cursor.fetchall()
                df_std = pd.DataFrame(std_result)
            except Exception as ex:
                print('rds err: ', ex)
            finally:
                if (rds_connection):
                    rds_connection.close()

            df_std.columns = ['material_description', 'std_feed_rate']
            df_merge = pd.merge(df, df_std, how='left', on=['material_description']).reset_index()
            df_merge['std_feed_rate'] = df_merge['std_feed_rate'].replace(['None', 'nan'], np.nan)
            if not pd.isna(df_merge['std_feed_rate']).any():
                print('there is not None in std feed rate')
                print('std feed rate unique: ', df_merge.std_feed_rate.unique())
                df_merge['available_time'] = df_merge['actual_barrel_weight'] / df_merge['std_feed_rate']
                available_time = df_merge['available_time'].sum()
                hr = int(available_time)
                minute = round((available_time - hr) * 60, 0)
                db.collection('COLLECTION_PREPARATION_ZONE_PAGE2').document(plant_id + '.' + factory + '.' + line + 'HOUR').set({
                    'HOUR': hr
                })
                db.collection('COLLECTION_PREPARATION_ZONE_PAGE2').document(plant_id + '.' + factory + '.' + line + 'MIN').set({
                    'MINUTE': minute
                })
            else:
                print('there is None in std feed rate')
                db.collection('COLLECTION_PREPARATION_ZONE_PAGE2').document(plant_id + '.' + factory + '.' + line + 'HOUR').set({
                    'HOUR': 'NA'
                })
                db.collection('COLLECTION_PREPARATION_ZONE_PAGE2').document(plant_id + '.' + factory + '.' + line + 'MIN').set({
                    'MINUTE': 'NA'
                })

        else:
            db.collection('COLLECTION_PREPARATION_ZONE_PAGE2').document(plant_id + '.' + factory + '.' + line + 'NUMBER_OF_BATCH').set({
                'NUMBER_OF_BATCH': 0
            })
            db.collection('COLLECTION_PREPARATION_ZONE_PAGE2').document(plant_id + '.' + factory + '.' + line + 'KG_IN_STOCK').set({
                'KG_IN_STOCK': 0
            })
            db.collection('prod_mes_ignition_preparation_zone_page1').document(plant_id + '.' + factory + '.' + line + 'OVERALL_TOTAL_KG_IN_STOCK').set({
                'OVERALL_TOTAL_KG_IN_STOCK': 0
            })
            db.collection('prod_mes_ignition_preparation_zone_page1').document(plant_id + '.' + factory + '.' + line + 'OVERALL_TOTAL_BARREL_IN_STOCK').set({
                'OVERALL_TOTAL_BARREL_IN_STOCK': 0
            })
            # key for batch details
            db.collection('COLLECTION_PREPARATION_ZONE_PAGE2').document(plant_id + '.' + factory + '.' + line + 'BATCH_DETAIL').set({"KEY":''})
            db.collection('COLLECTION_PREPARATION_ZONE_PAGE2').document(plant_id + '.' + factory + '.' + line + 'HOUR').set({
                'HOUR': 'NA'
            })
            db.collection('COLLECTION_PREPARATION_ZONE_PAGE2').document(plant_id + '.' + factory + '.' + line + 'MIN').set({
                'MINUTE': 'NA'
            })
            # delete unwanted batch detail key
            collection_ref = db.collection('COLLECTION_PREPARATION_ZONE_PAGE2')

            # Retrieve all documents in the collection (or use a more specific query if possible)
            docs = collection_ref.stream()

            # Filter documents whose ID begins with "ID"
            filtered_docs = [doc for doc in docs if doc.id.startswith(plant_id + factory + line)]
            for doc in filtered_docs:
                db.collection('COLLECTION_PREPARATION_ZONE_PAGE2').document(doc.id).delete()
    except Exception as ex:
        print(ex)
    finally:
        if connection:
            connection.close()
