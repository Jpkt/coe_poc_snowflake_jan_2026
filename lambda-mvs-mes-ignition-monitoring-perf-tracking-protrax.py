def lambda_handler(event, context):
    print(event)
    if len(event) > 0:
        if len(event) > 0:
            # postgresql connection
            connection = psycopg2.connect(user = RDS_USERNAME_1D,
            password = RDS_PASSWORD_1D, host = RDS_HOST_1D, port = RDS_PORT_1D, database = RDS_DATABASE_NAME_1D)
            cursor = connection.cursor()

            # extract plant id, factory, and line
            unix_payload_timestamp = event['timestamp']
            payload_timestamp = event['timestamp']
            payload_timestamp = (datetime.fromtimestamp(payload_timestamp/1000) + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S')
            mqtt_raw_df = pd.DataFrame(event['values'])
            mqtt_raw_df.loc[mqtt_raw_df['q']==False, 'v'] = None
            mqtt_raw_df['t'] = payload_timestamp
            mqtt_raw_df[['project', 'plant', 'factory', 'country', 'machine_type', 'machine_name', 'line', 'tag_name']]= mqtt_raw_df["id"].str.split(".", n = 7, expand = True) # expand col name just to use for loc
            lines = list(set(mqtt_raw_df['line'].tolist()))
            plant = mqtt_raw_df['plant'].tolist()[0]
            machine_name = mqtt_raw_df['machine_name'].tolist()[0]
            machine_type = mqtt_raw_df['machine_type'].tolist()[0]
            factory = mqtt_raw_df['factory'].tolist()[0]
            for line  in lines:
                print('line: ', line)
                suffix = str(plant) + str(factory) + str(machine_name) + str(machine_type) + line.lower()
                print('suffix: ', suffix)
                try:
                    # get is_completed
                    is_completed_check_on = 'is_completed_performance_tracking_'+ suffix
                    qs_is_completed = "select value from misc where key = '{0}'"
                    cursor.execute(qs_is_completed.format(is_completed_check_on))
                    qs_is_completed_result = cursor.fetchall()
                    if len(qs_is_completed_result) == 0:
                        qs_is_completed_result = [('TRUE',)]
                    qs_is_completed_result = qs_is_completed_result[0][0]

                    print('qs_is_completed_result: ', qs_is_completed_result)

                    

                    # active sku
                    active_name = '-'
                    active_code = '-'
                    integer_part = int(re.search(r'\d+', line).group())
                    # active_sku = mqtt_raw_df.loc[mqtt_raw_df['tag_name'] == f'L{integer_part}_CURRENT_PRODUCT_NAME']['v'].tolist()[0].replace(' ', '')[:-1] if mqtt_raw_df.loc[mqtt_raw_df['tag_name'] == f'L{integer_part}_CURRENT_PRODUCT_NAME']['v'].tolist()[0].replace(' ', '')[-1:] == 'A' else mqtt_raw_df.loc[mqtt_raw_df['tag_name'] == f'L{integer_part}_CURRENT_PRODUCT_NAME']['v'].tolist()[0].replace(' ', '')
                    active_sku = mqtt_raw_df.loc[mqtt_raw_df['tag_name'] == 'L1_CURRENT_PRODUCT_NAME']['v'].tolist()[0].replace(' ', '')[:-1] if mqtt_raw_df.loc[mqtt_raw_df['tag_name'] == 'L1_CURRENT_PRODUCT_NAME']['v'].tolist()[0].replace(' ', '')[-1:] == 'A' else mqtt_raw_df.loc[mqtt_raw_df['tag_name'] == 'L1_CURRENT_PRODUCT_NAME']['v'].tolist()[0].replace(' ', '')
                    print('active sku: ', active_sku)

                    if plant == '4045':
                        active_code = mqtt_raw_df.loc[mqtt_raw_df['tag_name'] == 'L1_CURRENT_PRODUCT_CODE']['v'].tolist()[0].replace(' ', '')[:-1] if mqtt_raw_df.loc[mqtt_raw_df['tag_name'] == 'L1_CURRENT_PRODUCT_NAME']['v'].tolist()[0].replace(' ', '')[-1:] == 'A' else mqtt_raw_df.loc[mqtt_raw_df['tag_name'] == 'L1_CURRENT_PRODUCT_CODE']['v'].tolist()[0].replace(' ', '')
                        print('active code: ', active_code)

                    # case 1: is_completed == False
                    if qs_is_completed_result.upper() == 'FALSE': 
                        prefix_doc_id  = f'{plant}.{factory}.{machine_type.upper()}.{machine_name.upper()}.{line.upper()}'
                        print('case 1: is_completed == False')
                        # update active sku to firestore
                        db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'ACTIVE_SKU').set({
                        'ACTIVE_SKU': active_sku
                        })
                        db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'ACTIVE_CODE').set({
                        'ACTIVE_CODE': active_code
                        })
                        
                        # get latest check_on
                        qs_check_on = "select value from misc where key = '{0}'"
                        cursor.execute(qs_check_on.format('current_check_on_line_monitor_performance_tracking' + suffix))
                        check_on = cursor.fetchall()[0][0]
                        print('check_on: ', check_on)

                        # get latest date
                        qs_date = "select value from misc where key = '{0}'"
                        cursor.execute(qs_date.format('factory_date_line_monitor_performance_tracking' + suffix))
                        factory_date = cursor.fetchall()[0][0]

                        # get latest shift
                        qs_shift = "select value from misc where key = '{0}'"
                        cursor.execute(qs_shift.format('shift_line_monitor_performance_tracking' + suffix))
                        shift = cursor.fetchall()[0][0]

                        # update active shift to firestore
                        db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'SHIFT').set({
                        'SHIFT': shift
                        })

                        # get latest sku
                        qs_sku = "select value from misc where key = '{0}'"
                        cursor.execute(qs_sku.format('sku_line_monitor_performance_tracking' + suffix))
                        sku = cursor.fetchall()[0][0]

                        print('factory_date: ', factory_date, ' shift: ', shift, ' sku: ', sku)

                        # po target
                        qs_target = "select id, work_center, material_description, status_process_order, production_quantity, production_unit, shift, process_order_number, cast(material_code as int) as material_code from sap_outbound where schedule_start_date = '{0}' and shift = '{1}' and plant_id = '{2}' and factory = '{3}'".format(factory_date, shift, plant, factory)
                        print('po qs: ', qs_target)
                        cursor.execute(qs_target)
                        target_result = cursor.fetchall()
                        if len(target_result) > 0:
                            df_sap = pd.DataFrame(target_result)
                            df_sap.columns = ['ID', 'WORK_CENTER', 'MATERIAL_DESCRIPTION', 'STATUS_PROCESS_ORDER', 'PRODUCTION_QUANTITY', 'PRODUCTION_UNIT', 'SHIFT', 'PROCESS_ORDER_NUMBER', 'MATERIAL_CODE']
                            df_sap = df_sap.loc[(df_sap['STATUS_PROCESS_ORDER'].str.split(' ').str[0]!= 'TECO')]
                            df_sap['MATERIAL_DESCRIPTION'] = df_sap['MATERIAL_DESCRIPTION'].str.split(':').str[0]
                            df_sap['MATERIAL_CODE'] = df_sap['MATERIAL_CODE'].astype(str)
                            df_sap = pd.pivot_table(
                                        df_sap,
                                            index = ['PROCESS_ORDER_NUMBER', 'WORK_CENTER', 'MATERIAL_DESCRIPTION', 'PRODUCTION_QUANTITY', 'PRODUCTION_UNIT', 'SHIFT', 'MATERIAL_CODE'],
                                            values= 'ID',
                                            aggfunc= 'nunique'
                                        ).reset_index()
                            print('LINE: ', line)
                            
                            # print(df_sap)
                            df_sap = df_sap.loc[(df_sap['WORK_CENTER'] == line.upper())]
                            if len(df_sap) != 0:
                                df_sap['PRODUCTION_QUANTITY'] = df_sap['PRODUCTION_QUANTITY'].astype('float')
                                df_sap.loc[df_sap['PRODUCTION_UNIT'] == 'TON', 'PRODUCTION_QUANTITY'] = df_sap.loc[df_sap['PRODUCTION_UNIT'] == 'TON']['PRODUCTION_QUANTITY'] * 1000
                                df_sap.loc[df_sap['PRODUCTION_UNIT'] == 'TON', 'PRODUCTION_UNIT'] = 'KG'
                                df_sap.loc[df_sap['SHIFT'] == 'D', 'SHIFT_PERIOD'] = '06:00 - 18:00 น.'
                                df_sap.loc[df_sap['SHIFT'] == 'N', 'SHIFT_PERIOD'] = '18:00 - 06:00 น.'
                                # df_sap.loc[df_sap['SHIFT'] == 'D', 'SHIFT'] = 'กะเช้า'
                                # df_sap.loc[df_sap['SHIFT'] == 'N', 'SHIFT'] = 'กะกลางคืน'
                                # df_sap['WORK_CENTER'] = df_sap['WORK_CENTER'].replace({'IQF0':  'LINE'}, regex=True)
                                # df_sap['WORK_CENTER'] = df_sap['WORK_CENTER'].replace({'IQF':  'LINE'}, regex=True)
                                material_description_of_specific_line = df_sap.MATERIAL_DESCRIPTION.unique()
                                doc = db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'WARNING').get()
                                if doc.exists:
                                    db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'WARNING').update({
                                    'SAP_SKU': material_description_of_specific_line.tolist()
                                    })
                                else:
                                    db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'WARNING').set({
                                    'SAP_SKU': material_description_of_specific_line.tolist()
                                    })
                                # print('material_description_of_specific_line: ', material_description_of_specific_line )
                                df_sap_detail = pd.pivot_table(
                                    df_sap,
                                    index = ['MATERIAL_DESCRIPTION', 'WORK_CENTER', 'PRODUCTION_UNIT', 'SHIFT', 'MATERIAL_CODE'],
                                    values = ['PRODUCTION_QUANTITY'],
                                    aggfunc= {
                                                'PRODUCTION_QUANTITY': 'sum'
                                    }
                                ).reset_index()
                                # print(df_sap_detail)
                                po_target = 0
                                po_target = df_sap_detail.loc[(df_sap_detail['MATERIAL_DESCRIPTION'].fillna('').str.upper().str.replace(r'\s+', '', regex=True) == re.sub(r'\s+', '', sku).upper()) & (df_sap_detail['SHIFT'] == shift), 'PRODUCTION_QUANTITY'].iloc[0] if len(df_sap_detail.loc[(df_sap_detail['MATERIAL_DESCRIPTION'].fillna('').str.upper().str.replace(r'\s+', '', regex=True) == re.sub(r'\s+', '', sku).upper()) & (df_sap_detail['SHIFT'] == shift)]) != 0 else None
                                # po_target = float(df_sap_detail.loc[(df_sap_detail['MATERIAL_CODE'] == active_code) & (df_sap_detail['SHIFT'] == shift)]['PRODUCTION_QUANTITY'].iloc[0] if len(df_sap_detail.loc[(df_sap_detail['MATERIAL_CODE'] == active_code) & (df_sap_detail['SHIFT'] == shift)]) != 0 else None)
                                print('factory_date: ', factory_date)
                                print('shift: ', shift)
                                print('line: ', line)
                                print('machine: ', machine_name)
                                print('sku: ', sku)
                                print('po_target', po_target)

                                if po_target:
                                    # update po target
                                    qs_update_po = '''
                                                update line_monitor
                                                set production_target = '{0}',
                                                updated_at = '{1}'
                                                where check_on = '{2}'
                                    '''
                                    cursor.execute(qs_update_po.format(po_target, datetime.now() + timedelta(hours=7), check_on))
                                    connection.commit()

                                    db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'TARGET_KG').set({
                                    'TARGET_KG': po_target
                                    })
                                    doc = db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'WARNING').get()
                                    if doc.exists:
                                        db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'WARNING').update({
                                        'MESSAGE': None
                                        })
                                    else:
                                        db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'WARNING').set({
                                        'MESSAGE': None
                                        })
                                    print('updated po target')

                                else:
                                    doc = db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'WARNING').get()
                                    if doc.exists:
                                        db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'WARNING').update({
                                        'MESSAGE': 'ไม่ตรงแผนผลิต'
                                        })
                                    else:
                                        db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'WARNING').set({
                                        'MESSAGE': 'ไม่ตรงแผนผลิต'
                                        })
                            else:
                                db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'TARGET_KG').set({
                                    'TARGET_KG': 0
                                })
                                db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'WARNING').set({
                                    'MESSAGE': 'ไม่มีแผนผลิต'
                                })
                        else:
                            db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'TARGET_KG').set({
                                'TARGET_KG': 0
                            })
                            db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'WARNING').set({
                                'MESSAGE': 'ไม่มีแผนผลิต'
                            })

                        # update status
                        db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'STATUS').set({
                        'STATUS': mqtt_raw_df.loc[(mqtt_raw_df['tag_name'] == 'STATUS') & (mqtt_raw_df['line'] == line)]['v'].tolist()[0]
                        })
                        
                        # update actual pack
                        actual_pack = float(mqtt_raw_df.loc[(mqtt_raw_df['q'] == True) & (mqtt_raw_df['tag_name'] == 'ACTUAL_PACK') & (mqtt_raw_df['line'] == line)]['v'].tolist()[0])
                        if actual_pack:
                            qs_actual_pack = '''
                                        update line_monitor
                                        set actual_pack = '{0}',
                                        updated_at = '{1}'
                                        where check_on = '{2}'
                                        and coalesce(actual_pack, 0) < '{0}'
                            '''
                            cursor.execute(qs_actual_pack.format(actual_pack, datetime.now() + timedelta(hours=7), check_on))
                            connection.commit()
                            # find current pack on rds
                            qs_current_actual_pack = '''
                                        select actual_pack
                                        from line_monitor
                                        where check_on = '{0}'
                            '''
                            cursor.execute(qs_current_actual_pack.format(check_on))
                            current_pack = cursor.fetchall()[0][0]
                            
                            db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'ACTUAL_PACK').set({
                                'ACTUAL_PACK': float(current_pack)
                            })
                            print('updated actual pack')


                            # calculate actual kg from auto_pack
                            qs_std_auto_pack = "select multihead_auto_pack from core_master_std where line='{0}' and factory = '{1}' and plant_id = '{2}' and material_description = '{3}'" 
                            cursor.execute(qs_std_auto_pack.format(line, factory, plant, sku))
                            std_auto_pack_result = cursor.fetchall()
                            if len(std_auto_pack_result) > 0:
                                actual_kg = float(current_pack) * float(std_auto_pack_result[0][0])/1000
                                print('line: ', line, ' current_pack: ', current_pack, ' std pack: ', std_auto_pack_result[0][0])
                                qs_actual_kg = '''
                                        update line_monitor
                                        set actual_weight = '{0}',
                                        updated_at = '{1}'
                                        where check_on = '{2}'
                                        and coalesce(actual_weight, 0) < '{0}'
                                '''
                                cursor.execute(qs_actual_kg.format(actual_kg, datetime.now() + timedelta(hours=7), check_on))
                                connection.commit()

                                db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'ACTUAL_KG').set({
                                    'ACTUAL_KG': float(actual_kg)
                                })
                            else:
                                db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'ACTUAL_KG').set({
                                    'ACTUAL_KG': None
                                })
                            print('updated actual kg')
                            
                            db.collection('COLLECTION_PERFORMANCE_PROTRAX_P1').document(prefix_doc_id + 'CHECK_ON').set({
                                'CHECK_ON': check_on
                            })

                except Exception as ex:
                    print('error---> ', ex)
                finally:
                    if connection:
                        connection.close()
