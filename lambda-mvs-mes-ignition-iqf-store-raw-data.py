def hhmmss_from_seconds(total_seconds):
    if pd.isna(total_seconds):
        return pd.NA
    total_seconds = int(round(float(total_seconds)))
    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def per_range(group: pd.DataFrame) -> pd.Series:
    print(group[['temp_range', 'usage_seconds']])
    start_dt = pd.to_datetime(group['start_date'], errors='coerce')
    end_dt   = pd.to_datetime(group['end_date'],   errors='coerce')
    start_timestamp = pd.to_datetime(group['start_timestamp'], errors='coerce')
    end_timestamp = pd.to_datetime(group['end_timestamp'],   errors='coerce')

    min_start = start_dt.min()
    max_end   = end_dt.max()
    min_start_time = start_timestamp.min()
    max_end_time = end_timestamp.max()
    x = group["usage_seconds"].astype(float).sort_values()

    if x.empty:
        return pd.Series({
            "q1": pd.NA, "q3": pd.NA, "iqr": pd.NA,
            "lower_bound": pd.NA, "upper_bound": pd.NA,
            "n_before_bounds": 0, "n_after_bounds": 0,
            "mean_seconds_in_bounds": pd.NA,
            "mean_time_in_bounds": pd.NA,
            "min_start_date": min_start,
            "max_end_date": max_end,
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp
        })

    # Quartiles
    q1 = x.quantile(0.25)
    q3 = x.quantile(0.75)
    iqr = q3 - q1

    # Tukey fences
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr

    # Keep only values inside [lower, upper]
    in_bounds = x[(x >= lower) & (x <= upper)]
    mean_sec = in_bounds.mean() if not in_bounds.empty else pd.NA
    return pd.Series({
        "q1": float(q1),
        "q3": float(q3),
        "iqr": float(iqr),
        "lower_bound": float(lower),
        "upper_bound": float(upper),
        "n_before_bounds": int(x.size),
        "n_after_bounds": int(in_bounds.size),
        "avg_seconds": (float(mean_sec) if mean_sec is not pd.NA else pd.NA),
        "avg_time": hhmmss_from_seconds(mean_sec),
        "min_start_date": min_start,
        "max_end_date": max_end,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp
        
    })

def lambda_handler(event, context):
    payload = pd.DataFrame(event['values'])
    payload['t'] = payload['t'].mul(1e6).apply(pd.Timestamp).astype('datetime64[s]') + pd.Timedelta(hours=7)
    payload.sort_values(by='t', ascending=True, inplace=True)
    payload_timestamp = event['timestamp']
    payload_timestamp = (datetime.fromtimestamp(payload_timestamp/1000) + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S')
    results = []
    
    for index, col in payload[['id', 't', 'q', 'v']].iterrows():
        data ={
            'TAG': col[0],
            'TAG_TIMESTAMP': col[1],
            'IS_CONNECTED': col[2],
            'VALUE': col[3],
            'PAYLOAD_TIMESTAMP': payload_timestamp,
            'LINE':(col[0].split('.')[4]).split('_')[0]
        }
        doc = db.collection('COLLECTION_TAG_LIST_IQF_IQR').document(col[0]).get()
        if not doc.exists:
            print('not existed')               
            db.collection('COLLECTION_TAG_LIST_IQF_IQR').document(col[0]).set(data) # insert data to firestore
            print('status: 200, successfully inserted data to firestore')
        else:
            print('existed')
            db.collection('COLLECTION_TAG_LIST_IQF_IQR').document(col[0]).update(data) # update data to firestore
            print('status: 200, successfully updated data to firestore')
    
    for item in payload[['id', 'v', 'q', 't']].itertuples():
        print('tag_name: ', item[1])
        print('value: ', item[2])
        
        # 1 = id, 2 = v, 3 = quality, 4 = timestamp
        tag, plant_id, factory_code, country, machine_type, machine_name, area, tag_name = item[1].split('.')

        # Product running
        if tag_name == 'Production_running' and item[3] and str(item[2]).upper() == 'TRUE':
            print('Production_running')

            data ={
                'TAG': item[1],
                'TAG_TIMESTAMP': (item[4]).strftime('%Y-%m-%d %H:%M:%S'),
                'IS_CONNECTED': item[3],
                'VALUE': item[2],
                'PAYLOAD_TIMESTAMP': payload_timestamp,
                'LINE':area
            }
            
            doc = db.collection('COLLECTION_TAG_LIST_IQF_IQR').document(item[1]).get()
            if not doc.exists:
                print('not existed')
                db.collection('COLLECTION_TAG_LIST_IQF_IQR').document(item[1]).set(data)
                print('status: 200, successfully inserted data to firestore')
            else:
                print('existed')
                db.collection('COLLECTION_TAG_LIST_IQF_IQR').document(item[1]).update(data)
                print('status: 200, successfully updated data to firestore')

            # insert data to db
            connection = psycopg2.connect(user = RDS_USERNAME,
                        password = RDS_PASSWORD, host = RDS_HOST, port = RDS_PORT, database = RDS_DATABASE_NAME)
            cursor = connection.cursor()
            try:
                query_string = '''INSERT INTO iqf_raw(
                            tag_name, v, q, t
                            )                                                   
                            VALUES(
                            %s, %s, %s, %s
                            )                     
                            '''
                cursor.execute(query_string, (item[1], item[2], item[3], item[4]))
                connection.commit()
                print("Successfully inserted data to pgsql database.")
            except Exception as ex:
                print('error ---> ', ex)
            finally:
                if (connection):
                    connection.close()
                    print("PostgreSQL connection is closed")
        
        production_running = db.collection('COLLECTION_TAG_LIST_IQF_IQR').document(item[1]).get().to_dict()['VALUE']
        
        doc_id = country + plant_id + factory_code + area + 'IQF_RAW_DATA_PROCESSING'
        
        if tag_name == 'REQUEST' and item[3]:
            print('Request')
            myAWSIoTMQTTClient = AWSIoTMQTTClient(f"{CLIENT_ID}-{context.aws_request_id}", useWebsocket=False)
            myAWSIoTMQTTClient.configureEndpoint(IOT_HOST, IOT_PORT)
            myAWSIoTMQTTClient.configureCredentials(CA_PATH, KEY_PATH, CERT_PATH)

            # Resilience (recommended for 1.4.9)
            myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
            myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)
            myAWSIoTMQTTClient.configureDrainingFrequency(2)

            myAWSIoTMQTTClient.configureConnectDisconnectTimeout(60)
            myAWSIoTMQTTClient.configureMQTTOperationTimeout(20)
            ok = myAWSIoTMQTTClient.connect(45)

            if not ok:
                raise RuntimeError("MQTT connect() returned False")
            # query latest 7 calculated value
            connection = psycopg2.connect(user = RDS_USERNAME,
                password = RDS_PASSWORD, host = RDS_HOST, port = RDS_PORT, database = RDS_DATABASE_NAME)
            cursor = connection.cursor()
            connection_w = psycopg2.connect(user = RDS_USERNAME,
                password = RDS_PASSWORD, host = RDS_HOST, port = RDS_PORT, database = RDS_DATABASE_NAME_WEB)
            cursor_w = connection_w.cursor()
            try:
                processing_completed_tumbler_barrrel_qs = '''
                        WITH latest AS (
                            SELECT
                                material_description,
                                line,
                                GREATEST(
                                COALESCE(at_process_timestamp,  TIMESTAMPTZ '-infinity')
                                ) AS latest_ts,
                                status
                            FROM core_tumbler
                            WHERE plant_id = %s
                                AND factory  = %s
                                AND upper(line)     = %s
                            ORDER BY latest_ts DESC NULLS LAST
                            LIMIT 1
                            )
                            SELECT
                            material_description,
                            line,
                            -- format in UTC; drop AT TIME ZONE if you want DB timezone
                            to_char(latest_ts AT TIME ZONE 'UTC', 'HH24') AS "HH",
                            to_char(latest_ts AT TIME ZONE 'UTC', 'MI')   AS "MM",
                            to_char(latest_ts AT TIME ZONE 'UTC', 'SS')   AS "SS",
                            status
                            FROM latest;     
                '''
                cursor.execute(processing_completed_tumbler_barrrel_qs, (plant_id, factory_code, area.upper()))
                row = cursor.fetchone()

                sku = ''
                line = ''
                hh = ''
                mm = ''
                ss = ''
                line_status = ''
                if row:
                    sku, line, hh, mm, ss, line_status = row

                print('(plant_id, factory_code, area.upper(), country, sku): ', (plant_id, factory_code, area.upper(), country, sku))
                
                # query master iqf std
                id = ''
                channel = ''
                plant_id_t = ''
                country_t = ''
                factory = ''
                material_description = ''
                std_cap = ''
                mean_weight_per_piece = ''
                max_time_fry_1 = ''
                max_time_fry_2 = ''
                coating_other_process_time = ''
                max_total_process_time = ''
                iqf_freezer_min_temp = ''
                iqf_freezer_operation_temp = ''
                iqf_freezer_max_temp = ''
                compressor_low_suction_pressure_min = ''
                compressor_low_suction_pressure_operation = ''
                compressor_low_suction_pressure_max = ''
                adjustment_resolution = ''
                std_core_temp = ''
                iqf_capacity = ''
                safety_factor = ''
                check_on = ''

                master_std_qs = """
                    SELECT *
                    FROM core_master_iqf
                    WHERE plant_id = %s
                    AND factory = %s
                    AND upper(line) = %s
                    AND country = %s
                    AND material_description = %s;
                """
                cursor_w.execute(master_std_qs, (plant_id, factory_code, area.upper(), country, sku))
                result = cursor_w.fetchone()   # expect one row
                print('result: ', result)
                if result:
                    (
                        id, channel, country_t, plant_id_t, factory, line,
                        material_description, std_cap, mean_weight_per_piece,
                        max_time_fry_1, max_time_fry_2, coating_other_process_time,
                        max_total_process_time, iqf_freezer_min_temp,
                        iqf_freezer_operation_temp, iqf_freezer_max_temp,
                        compressor_low_suction_pressure_min,
                        compressor_low_suction_pressure_operation,
                        compressor_low_suction_pressure_max, adjustment_resolution,
                        std_core_temp, iqf_capacity, safety_factor, check_on
                    ) = result
                
                # sku
                id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.MES_PRODUCT_NAME'
                command = [
                    {   "id": id,
                        "v": sku
                    }
                    ]
                myAWSIoTMQTTClient.publish(PUB_TOPIC, json.dumps(command),1)

                # line status
                line_status_v = '0' if line_status.upper() == 'COMPLETED' else '1'
                id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.STATUS_LINE'
                command = [
                    {   "id": id,
                        "v": line_status_v
                    }
                    ]
                myAWSIoTMQTTClient.publish(PUB_TOPIC, json.dumps(command),1)

                # hh
                id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.TIME_START_SCAN_HR'
                command = [
                    {   "id": id,
                        "v": hh
                    }
                    ]
                myAWSIoTMQTTClient.publish(PUB_TOPIC, json.dumps(command),1)

                # mm
                id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.TIME_START_SCAN_MIN'
                command = [
                    {   "id": id,
                        "v": mm
                    }
                    ]
                myAWSIoTMQTTClient.publish(PUB_TOPIC, json.dumps(command),1)

                # ss
                id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.TIME_START_SCAN_SEC'
                command = [
                    {   "id": id,
                        "v": ss
                    }
                    ]
                myAWSIoTMQTTClient.publish(PUB_TOPIC, json.dumps(command),1)

                # press down rate
                id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.SC_PRESS_DOWN_RATE'
                command = [
                    {   "id": id,
                        "v": str(adjustment_resolution)
                    }
                    ]
                myAWSIoTMQTTClient.publish(PUB_TOPIC, json.dumps(command),1)

                # press op
                id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.SC_PRESS_OP'
                command = [
                    {   "id": id,
                        "v": str(compressor_low_suction_pressure_operation)
                    }
                    ]
                myAWSIoTMQTTClient.publish(PUB_TOPIC, json.dumps(command),1)

                # press max op
                id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.SC_PRESS_MAXOP'
                command = [
                    {   "id": id,
                        "v": str(compressor_low_suction_pressure_max)
                    }
                    ]
                myAWSIoTMQTTClient.publish(PUB_TOPIC, json.dumps(command),1)

                # press min op
                id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.SC_PRESS_MINOP'
                command = [
                    {   "id": id,
                        "v": str(compressor_low_suction_pressure_min)
                    }
                    ]
                myAWSIoTMQTTClient.publish(PUB_TOPIC, json.dumps(command),1)

                # iqf temp op
                id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.IQF_TEMP_OP'
                command = [
                    {   "id": id,
                        "v": str(iqf_freezer_operation_temp)
                    }
                    ]
                myAWSIoTMQTTClient.publish(PUB_TOPIC, json.dumps(command),1)

                # iqf temp min op
                id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.IQF_TEMP_MINOP'
                command = [
                    {   "id": id,
                        "v": (iqf_freezer_min_temp)
                    }
                    ]
                myAWSIoTMQTTClient.publish(PUB_TOPIC, json.dumps(command),1)

                # iqf temp max op
                id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.IQF_TEMP_MAXOP'
                command = [
                    {   "id": id,
                        "v": str(iqf_freezer_max_temp)
                    }
                    ]
                myAWSIoTMQTTClient.publish(PUB_TOPIC, json.dumps(command),1)

                # mes product name
                id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.MES_PRODUCT_NAME'
                command = [
                    {   "id": id,
                        "v": sku
                    }
                    ]
                myAWSIoTMQTTClient.publish(PUB_TOPIC, json.dumps(command),1)

                # process time op
                id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.PROCESS_TIME_OP'
                command = [
                    {   "id": id,
                        "v": str(coating_other_process_time)
                    }
                    ]
                myAWSIoTMQTTClient.publish(PUB_TOPIC, json.dumps(command),1)

                # load safety ft
                id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.IQF_LOAD_SAFETY_FT'
                command = [
                    {   "id": id,
                        "v": str(safety_factor)
                    }
                    ]
                myAWSIoTMQTTClient.publish(PUB_TOPIC, json.dumps(command),1)

                qs_iqr_history = '''
                        WITH ranked AS (
                        SELECT
                            start_temp, end_temp, usage_seconds, "round",
                            start_date, end_date, start_timestamp, end_timestamp, avg_seconds,
                            ROW_NUMBER() OVER (
                            PARTITION BY start_temp, end_temp
                            ORDER BY "round" DESC, start_timestamp DESC, end_timestamp DESC
                            ) AS rn
                        FROM core_iqr
                        WHERE plant_id = %s
                            AND factory  = %s
                            AND country  = %s
                            AND UPPER(line) = %s
                        )
                        SELECT *
                        FROM ranked
                        WHERE rn = 1
                        ORDER BY "round" DESC, start_date DESC, end_date DESC, start_timestamp DESC;
                        ;
                    '''
                cursor.execute(qs_iqr_history, (plant_id, factory_code, country, area.upper()))
                iqr_history_result = cursor.fetchall()
                current_iqr = pd.DataFrame(iqr_history_result)
                if len(current_iqr) > 0:
                    current_iqr.columns = [
                        'start_temp', 'end_temp', 'usage_seconds', "round",
                        'start_date', 'end_date', 'start_timestamp', 'end_timestamp', 
                        'avg_seconds', 'rn'
                    ]
                    for item in current_iqr[['start_temp', 'end_temp', 'avg_seconds']].itertuples():
                        # Further.4117.00000000000000.Thailand.FEMS.IQF.Line7.TIME_DOWN_RATE_1 ; Temp [-35 ,-40]
                        if item[1] >= -40 and item[2] < -35:
                            id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.TIME_DOWN_RATE_1'
                        # Further.4117.00000000000000.Thailand.FEMS.IQF.Line7.TIME_DOWN_RATE_2 ; Temp [-30 ,-35]
                        elif item[1] >= -35 and item[2] < -30:
                            id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.TIME_DOWN_RATE_2'
                        # Further.4117.00000000000000.Thailand.FEMS.IQF.Line7.TIME_DOWN_RATE_3 ; Temp [-25 ,-30]
                        elif item[1] >= -30 and item[2] < -25:
                            id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.TIME_DOWN_RATE_3'
                        # Further.4117.00000000000000.Thailand.FEMS.IQF.Line7.TIME_DOWN_RATE_4 ; Temp [-20 ,-25]
                        elif item[1] >= -25 and item[2] < -20:
                            id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.TIME_DOWN_RATE_4'
                        # Further.4117.00000000000000.Thailand.FEMS.IQF.Line7.TIME_DOWN_RATE_5 ; Temp [-15 ,-20]
                        elif item[1] >= -20 and item[2] < -15:
                            id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.TIME_DOWN_RATE_5'
                        # Further.4117.00000000000000.Thailand.FEMS.IQF.Line7.TIME_DOWN_RATE_6 ; Temp [-10 ,-15]
                        elif item[1] >= -15 and item[2] < -10:
                            id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.TIME_DOWN_RATE_6'
                        # Further.4117.00000000000000.Thailand.FEMS.IQF.Line7.TIME_DOWN_RATE_7 ; Temp [-5 ,-10]
                        elif item[1] >= -10 and item[2] < -5:
                            id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.TIME_DOWN_RATE_7'
                        # Further.4117.00000000000000.Thailand.FEMS.IQF.Line7.TIME_DOWN_RATE_8 ; Temp [0 ,-5]
                        elif item[1] >= -5 and item[2] < 0:
                            id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.TIME_DOWN_RATE_8'
                        
                        command = [
                        {   "id": id,
                            "v": str(item[3])
                        }
                        ]
                        myAWSIoTMQTTClient.publish(PUB_TOPIC, json.dumps(command),1)

            except Exception as ex:
                print('error ---> ', ex)
            finally:
                if (connection):
                    connection.close()
                if (connection_w):
                    connection_w.close()
        
        if production_running and tag_name == 'Actual_Temp_Freezer' and item[3]:
            # check current job status
            current_job_status_doc = db.collection('COLLECTION_IQF_IQR').document(doc_id).get()
            current_job_status =''
            if current_job_status_doc.exists:
                current_job_status = current_job_status_doc.to_dict()['STATUS']
            else:
                current_job_status = ''
            
            if current_job_status != 'Running' and item[2] <= 0 and item[2] >= -7:
                data = {
                    'COUNTRY': country,
                    'PLANT_ID': plant_id,
                    'FACTORY_CODE': factory_code,
                    'LINE': area.upper(),
                    'STATUS': 'Running',
                    'START_TIME': (item[4]).strftime('%Y-%m-%d %H:%M:%S'),
                    'END_TIME': '',
                    'CALCULATED_IQR_TIME': '',
                    'IQR': ''
                }
                doc = db.collection('COLLECTION_IQF_IQR').document(doc_id).get()
                if doc.exists:
                    db.collection('COLLECTION_IQF_IQR').document(doc_id).update(data)
                else:
                    db.collection('COLLECTION_IQF_IQR').document(doc_id).set(data)

            if current_job_status == 'Running':
                # insert data to db
                connection = psycopg2.connect(user = RDS_USERNAME,
                            password = RDS_PASSWORD, host = RDS_HOST, port = RDS_PORT, database = RDS_DATABASE_NAME)
                cursor = connection.cursor()
                try:
                    query_string = '''INSERT INTO iqf_raw(
                                tag_name, v, q, t
                                )                                                   
                                VALUES(
                                %s, %s, %s, %s
                                )                     
                                '''
                    cursor.execute(query_string, (item[1], item[2], item[3], item[4]))
                    connection.commit()
                    print("Successfully inserted data to pgsql database.")
                except Exception as ex:
                    print('error ---> ', ex)
                finally:
                    if (connection):
                        connection.close()
                        print("PostgreSQL connection is closed")
            
            # if Actual_Temp_Freezer <= -35, update job status to 'end'
            if current_job_status == 'Running' and item[2] <= -35:
                print('Actual_Temp_Freezer <= -35, update job status to end')
                data = {
                    'STATUS': 'End',
                    'END_TIME': (item[4]).strftime('%Y-%m-%d %H:%M:%S')
                }
                db.collection('COLLECTION_IQF_IQR').document(doc_id).update(data)
                

                # query date in range start and end time
                current_job = db.collection('COLLECTION_IQF_IQR').document(doc_id).get()
                if current_job.exists:
                    print('current job exists')
                    start_time = current_job.to_dict()['START_TIME']
                    end_time = current_job.to_dict()['END_TIME']
                    # tag_id = plant_id + '.' + factory_code + '.' + country + '.' + machine_type + '.' + machine_name + '.' + area.capitalize()
                    tag_id = item[1]
                    print('tag_id', tag_id)
                    connection = psycopg2.connect(user = RDS_USERNAME,
                            password = RDS_PASSWORD, host = RDS_HOST, port = RDS_PORT, database = RDS_DATABASE_NAME)
                    cursor = connection.cursor()
                    try:
                        qs_actual_freezer = '''
                                    select * from iqf_raw where tag_name LIKE %s and t >= %s and t <= %s
                                '''
                        cursor.execute(qs_actual_freezer, (tag_id, start_time, end_time))
                        actual_freezer_result = cursor.fetchall()
                        df_freezer = pd.DataFrame(actual_freezer_result)
                    except Exception as ex:
                        print('error ---> ', ex)
                    finally:
                        if (connection):
                            connection.close()
                    print('len of df_freezer: ', len(df_freezer))
                    if len(df_freezer) > 0:
                        df_freezer.columns = ['tag_name', 'v', 'q', 't']
                        df_freezer = df_freezer[df_freezer["q"]].copy()
                        df_freezer["t"] = pd.to_datetime(df_freezer["t"])
                        df_freezer = df_freezer.sort_values("t")
                        df_freezer["v"] = df_freezer["v"].astype('float')

                        # first timestamp each value appears
                        first_seen = df_freezer.groupby("v")["t"].min().to_dict()

                        vmax, vmin = df_freezer["v"].max(), df_freezer["v"].min()
                        upper = int(math.ceil(vmax / 5.0) * 5)
                        lower = int(math.floor(vmin / 5.0) * 5)

                        results = []
                        for U in range(upper, lower, -5):
                            L = U - 5
                            start = first_seen.get(U, pd.NaT)
                            end   = first_seen.get(L, pd.NaT)
                            t_max = pd.NaT
                            t_min = pd.NaT

                            if pd.notna(start) and pd.notna(end) and end >= start:
                                mask_time = (df_freezer["t"] >= start) & (df_freezer["t"] <= end)
                                t_min = df_freezer.loc[mask_time, "t"].min()
                                t_max = df_freezer.loc[mask_time, "t"].max()
                                delta = end - start
                                total_seconds = int(delta.total_seconds())
                                hours, remainder = divmod(total_seconds, 3600)
                                minutes, seconds = divmod(remainder, 60)
                                usage_time = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                                usage_sec = total_seconds
                            else:
                                usage_time = pd.NA
                                usage_sec = pd.NA

                            results.append({
                                "temp_range": f"{U}-{L}",
                                "start": start,
                                "end": end,
                                "start_timestamp": None if pd.isna(t_min) else t_min,
                                "end_timestamp":   None if pd.isna(t_max) else t_max,
                                "usage_time": usage_time,
                                "usage_seconds": usage_sec,
                                "start_date": start_time[:10],
                                "end_date": end_time[:10]
                            })
                        
                        df_current_agg = pd.DataFrame(results)

                        print('len of df_current_agg: ', len(df_current_agg))

                        # calculate iqr
                        # query latest 7 calculated value
                        connection = psycopg2.connect(user = RDS_USERNAME,
                            password = RDS_PASSWORD, host = RDS_HOST, port = RDS_PORT, database = RDS_DATABASE_NAME)
                        cursor = connection.cursor()
                        try:
                            qs_iqr_history = '''
                                    WITH ranked AS (
                                        SELECT
                                            start_temp, end_temp, usage_seconds, "round",
                                            start_date, end_date, start_timestamp, end_timestamp,
                                            ROW_NUMBER() OVER (
                                            PARTITION BY "round", start_temp, end_temp
                                            ORDER BY start_timestamp DESC, end_timestamp DESC
                                            ) AS rn
                                        FROM core_iqr
                                        WHERE plant_id = %s
                                            AND factory  = %s
                                            AND UPPER(country)  = %s
                                            AND UPPER(line) = %s
                                        )
                                        SELECT *
                                        FROM ranked
                                        WHERE rn <= 7
                                        ORDER BY start_temp, end_temp, start_timestamp DESC;
                                '''
                            cursor.execute(qs_iqr_history, (plant_id, factory_code, country.upper(), area.upper()))
                            iqr_history_result = cursor.fetchall()
                        except Exception as ex:
                            print('error ---> ', ex)
                        finally:
                            if (connection):
                                connection.close()
                        
                        df_prev_iqr = pd.DataFrame(iqr_history_result)
                        print('len of df_prev_iqr: ', len(df_prev_iqr))
                        if len(df_prev_iqr) > 0:
                            df_prev_iqr.columns = ['start_temp', 'end_temp', 'usage_seconds', 'round', 'start_date', 'end_date', 'start_timestamp', 'end_timestamp', 'rn']
                            df_prev_iqr['temp_range'] = (
                                df_prev_iqr['start_temp'].astype(int).astype(str)
                                + '-'
                                + df_prev_iqr['end_temp'].astype(int).astype(str)
                            )
                            prev_round = pd.to_numeric(df_prev_iqr.get('rn', pd.Series(dtype='float64')), errors='coerce')
                            current_round = (0 if prev_round.empty or prev_round.notna().sum() == 0 else int(prev_round.max())) + 1
                            df_current_agg['round'] = current_round
                            # merge history and current one
                            df_merge = pd.concat(
                                [
                                    df_current_agg[['temp_range', 'usage_seconds', 'start_date', 'end_date', 'start_timestamp', 'end_timestamp', 'round']],
                                    df_prev_iqr[['temp_range', 'usage_seconds', 'start_date', 'end_date', 'start_timestamp', 'end_timestamp', 'round']]
                                ],
                                ignore_index=True
                            )
                            df_merge = df_merge.copy()
                            df_merge["usage_seconds"] = pd.to_numeric(df_merge["usage_seconds"], errors="coerce")
                            df_cap = df_merge[df_merge["usage_seconds"] < 1800].dropna(subset=["usage_seconds"])
                            
                            # calulate avg time and avg second
                            result = df_cap.groupby("temp_range", dropna=False).apply(per_range).reset_index()
                            result['country'] = country
                            result['plant_id'] = plant_id
                            result['factory'] = factory_code
                            result['line'] = area
                            result['round'] = current_round
                            current_iqr = df_current_agg.merge(
                                result,
                                how="left",
                                on=["temp_range", "round"],
                                suffixes=("", "_df")
                            )
                            pat = r'^\s*(-?\d+(?:\.\d+)?)\s*[-–]\s*(-?\d+(?:\.\d+)?)'

                            current_iqr[['start_temp', 'end_temp']] = (
                                current_iqr['temp_range'].astype(str).str.extract(pat)
                            ).astype(float)
                            current_iqr = current_iqr[[
                                'country', 'plant_id', 'factory', 'line', 'start_temp', 'end_temp',
                                'start_timestamp', 'end_timestamp', 'start_date', 'end_date', 'usage_time',
                                'usage_seconds', 'round', 'avg_time', 'avg_seconds'
                            ]]
                            current_iqr = current_iqr[current_iqr.notna().all(axis=1)]
                            record = tuple(current_iqr.itertuples(index=False, name=None))

                            # query master iqf std
                            id = ''
                            channel = ''
                            country = ''
                            plant_id = ''
                            factory = ''
                            line = ''
                            material_description = ''
                            std_cap = ''
                            mean_weight_per_piece = ''
                            max_time_fry_1 = ''
                            max_time_fry_2 = ''
                            coating_other_process_time = ''
                            max_total_process_time = ''
                            iqf_freezer_min_temp = ''
                            iqf_freezer_operation_temp = ''
                            iqf_freezer_max_temp = ''
                            compressor_low_suction_pressure_min = ''
                            compressor_low_suction_pressure_operation = ''
                            compressor_low_suction_pressure_max = ''
                            adjustment_resolution = ''
                            std_core_temp = ''
                            iqf_capacity = ''
                            safety_factor = ''
                            check_on = ''
                            connection_w = psycopg2.connect(user = RDS_USERNAME,
                            password = RDS_PASSWORD, host = RDS_HOST, port = RDS_PORT, database = RDS_DATABASE_NAME_WEB)
                            cursor_w = connection_w.cursor()
                            try:
                                master_std_qs = """
                                    SELECT *
                                    FROM core_master_iqf
                                    WHERE plant_id = %s
                                    AND factory = %s
                                    AND upper(line) = %s
                                    AND country = %s
                                    AND material_description = %s;
                                """

                                cursor_w.execute(master_std_qs, (plant_id, factory_code, area.upper(), country, sku))
                                result = cursor_w.fetchone()   # expect one row
                                if result:
                                    (
                                        id, channel, country, plant_id, factory, line,
                                        material_description, std_cap, mean_weight_per_piece,
                                        max_time_fry_1, max_time_fry_2, coating_other_process_time,
                                        max_total_process_time, iqf_freezer_min_temp,
                                        iqf_freezer_operation_temp, iqf_freezer_max_temp,
                                        compressor_low_suction_pressure_min,
                                        compressor_low_suction_pressure_operation,
                                        compressor_low_suction_pressure_max, adjustment_resolution,
                                        std_core_temp, iqf_capacity, safety_factor, check_on
                                    ) = result
                            except Exception as ex:
                                print('ex: ', ex)
                            finally:
                                if (connection_w):
                                    connection_w.close()
                            
                            myAWSIoTMQTTClient = AWSIoTMQTTClient(f"{CLIENT_ID}-{context.aws_request_id}", useWebsocket=False)
                            myAWSIoTMQTTClient.configureEndpoint(IOT_HOST, IOT_PORT)
                            myAWSIoTMQTTClient.configureCredentials(CA_PATH, KEY_PATH, CERT_PATH)

                            # Resilience (recommended for 1.4.9)
                            myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
                            myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)
                            myAWSIoTMQTTClient.configureDrainingFrequency(2)

                            myAWSIoTMQTTClient.configureConnectDisconnectTimeout(60)
                            myAWSIoTMQTTClient.configureMQTTOperationTimeout(20)
                            ok = myAWSIoTMQTTClient.connect(45)
                            if not ok:
                                raise RuntimeError("MQTT connect() returned False")
                                    
                            for item in current_iqr[['start_temp', 'end_temp', 'avg_seconds']].itertuples():
                                if pd.notna(item[3]) and str(item[3]).lower() != "nan":
                                    # Further.4117.00000000000000.Thailand.FEMS.IQF.Line7.TIME_DOWN_RATE_1 ; Temp [-35 ,-40]
                                    if item[1] >= -40 and item[2] < -35:
                                        id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.TIME_DOWN_RATE_1'
                                    # Further.4117.00000000000000.Thailand.FEMS.IQF.Line7.TIME_DOWN_RATE_2 ; Temp [-30 ,-35]
                                    elif item[1] >= -35 and item[2] < -30:
                                        id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.TIME_DOWN_RATE_2'
                                    # Further.4117.00000000000000.Thailand.FEMS.IQF.Line7.TIME_DOWN_RATE_3 ; Temp [-25 ,-30]
                                    elif item[1] >= -30 and item[2] < -25:
                                        id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.TIME_DOWN_RATE_3'
                                    # Further.4117.00000000000000.Thailand.FEMS.IQF.Line7.TIME_DOWN_RATE_4 ; Temp [-20 ,-25]
                                    elif item[1] >= -25 and item[2] < -20:
                                        id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.TIME_DOWN_RATE_4'
                                    # Further.4117.00000000000000.Thailand.FEMS.IQF.Line7.TIME_DOWN_RATE_5 ; Temp [-15 ,-20]
                                    elif item[1] >= -20 and item[2] < -15:
                                        id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.TIME_DOWN_RATE_5'
                                    # Further.4117.00000000000000.Thailand.FEMS.IQF.Line7.TIME_DOWN_RATE_6 ; Temp [-10 ,-15]
                                    elif item[1] >= -15 and item[2] < -10:
                                        id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.TIME_DOWN_RATE_6'
                                    # Further.4117.00000000000000.Thailand.FEMS.IQF.Line7.TIME_DOWN_RATE_7 ; Temp [-5 ,-10]
                                    elif item[1] >= -10 and item[2] < -5:
                                        id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.TIME_DOWN_RATE_7'
                                    # Further.4117.00000000000000.Thailand.FEMS.IQF.Line7.TIME_DOWN_RATE_8 ; Temp [0 ,-5]
                                    elif item[1] >= -5 and item[2] < 0:
                                        id = f'{channel}.{plant_id}.{factory_code}.{country.capitalize()}.FEMS.IQF.{area.capitalize()}.TIME_DOWN_RATE_8'
                                    
                                    command = [
                                    {   "id": id,
                                        "v": str(item[3])
                                    }
                                    ]
                                    myAWSIoTMQTTClient.publish(PUB_TOPIC, json.dumps(command),1)
                            
                            # insert to db
                            connection = psycopg2.connect(user = RDS_USERNAME,
                                password = RDS_PASSWORD, host = RDS_HOST, port = RDS_PORT, database = RDS_DATABASE_NAME)
                            cursor = connection.cursor()
                            try:
                                qs_insert = '''
                                            insert into core_iqr(
                                            country, plant_id, factory, line, start_temp, end_temp,
                                            start_timestamp, end_timestamp, start_date, end_date, usage_time,
                                            usage_seconds, round, avg_time, avg_seconds
                                            )

                                            values(
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s
                                            )
                                '''
                                cursor.executemany(qs_insert, record)
                                connection.commit()
                                print('inserted calculated iqr')
                                data = {
                                    'STATUS': 'Completed',
                                    'CALCULATED_IQR_TIME': (datetime.now() + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S'),
                                    'IQR': 'Calculated'
                                }
                                doc = db.collection('COLLECTION_IQF_IQR').document(doc_id).get()
                                if doc.exists:
                                    db.collection('COLLECTION_IQF_IQR').document(doc_id).update(data)
                                else:
                                    db.collection('COLLECTION_IQF_IQR').document(doc_id).set(data)
                            except Exception as ex:
                                print('error ---> ', ex)
                            finally:
                                if (connection):
                                    connection.close()

            
