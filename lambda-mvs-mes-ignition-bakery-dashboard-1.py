def lambda_handler(event, context):
    print('event: ', event, type(event))
    if event.get("mqtt_topic") == 'bakery/dc/inlinedashboard/prepzone':
        if isinstance(event, dict):
            payload = pd.DataFrame(event['values'])
            payload['t'] = payload['t'].mul(1e6).apply(pd.Timestamp).astype('datetime64[s]') + pd.Timedelta(hours=7)
            payload['factory_date'] = (payload['t'] - pd.Timedelta(hours=1)).dt.date.astype(str)
            
            for index, row in payload[['t', 'id', 'q', 'v']].iterrows():
                # print(row)
                data = {
                    'timestamp': row[0],
                    'id': row[1],
                    'value': row[3],
                    'q': row[2]
                    
                }
                check_on_updated = row[1]
                db.collection('COLLECTION_TAG_LIST_BAKERY_DASHBOARD1').document(check_on_updated).set(data) # insert data to firestore

            for item in payload[['id', 'v', 'q', 't', 'factory_date']].itertuples():
                # print(item)
                factory_date = (item[4] - timedelta(hours=1)).date().isoformat()
                parts = item[1].split(".")
                machine_name = ''
                area = ''
                tag_name = ''
                plant_id = ''
                if len(parts) == 7:
                    tag, plant_id, country, machine_type, machine_name, area, tag_name = parts
                elif len(parts) == 6:
                    tag, plant_id, country, machine_name, area, tag_name = parts

                doc_id_suffix = f'{plant_id}.DASHBOARD1'
                # start mixing
                if machine_name == 'mixer' and area == 'sponge_mixer_4' and tag_name == 'start_time' and item[3]:
                    sku_tag = "MES_Bakery.4105.thailand.mixer.sponge_mixer_4.preparation_room.recipe_name__1_"
                    sku_doc = db.collection("COLLECTION_TAG_LIST_BAKERY_DASHBOARD1").document(sku_tag).get()
                    sku = ''
                    if sku_doc.exists:
                        sku = sku_doc.to_dict().get('value')
                    print('sku: ', sku)

                    batch_no_tag = "Simulator.4105.thailand.mixer.sponge_mixer_4.batch_no"
                    batch_doc = db.collection("COLLECTION_TAG_LIST_BAKERY_DASHBOARD1").document(batch_no_tag).get()
                    batch = ''
                    if batch_doc.exists:
                        batch = batch_doc.to_dict().get('value')
                    print('batch: ', batch)

                    # find total batch
                    # headers = {
                    #     "authorizationToken": API_KEY
                    # }

                    # response = requests.get(f'{API_URL_1}{factory_date}', headers=headers)
                    # result = response.json()

                    data = {
                        'DATE': item[5],
                        # 'ALL_BATCH': len(result['BAKERY_BATCH_DATA']),
                        'ALL_BATCH': '',
                        'BATCH_NO': batch,
                        'SKU': sku,
                        'STAGE': 'SPONGE MIX',
                        'START_SPONGE_MIXING_TIME': item[4].strftime('%Y-%m-%d %H:%M:%S'),
                        'STOP_SPONGE_MIXING_TIME': '',
                        'START_FERMENTING_TIME': '',
                        'STOP_FERMENTING_TIME': '',
                        'FERMENTATION_STATUS': '',
                        'START_DOUGH_MIXING_TIME': '',
                        'STOP_DOUGH_MIXING_TIME': '',
                        'DOUGH_MIXING_STATUS': ''
                    }

                    doc_id = f'{item[5]}.{str(int(batch))}.{doc_id_suffix}'
                    doc = db.collection('COLLECTION_BAKERY_DASHBOARD1').document(doc_id).get()
                    if not doc.exists:
                        print('not existed')
                        db.collection('COLLECTION_BAKERY_DASHBOARD1').document(doc_id).set(data)
                        print('status: 200, successfully inserted data to firestore')
                    
                    # check prev batch
                    prev_doc_id = f'{item[5]}.{str(int(batch)-1)}.{doc_id_suffix}'
                    prev_doc = db.collection('COLLECTION_BAKERY_DASHBOARD1').document(prev_doc_id).get()
                    if prev_doc.exists:
                        start_fermenting_time = prev_doc.to_dict()['START_FERMENTING_TIME']
                        if start_fermenting_time == '':
                            data = {
                                'STAGE': 'FERMENTATION',
                                'FERMENTATION_STATUS': 'IN',
                                'START_FERMENTING_TIME': item[4].strftime('%Y-%m-%d %H:%M:%S')
                            }
                            db.collection('COLLECTION_BAKERY_DASHBOARD1').document(prev_doc_id).update(data)


                    # mixer 7
                    batch_no_tag7 = "Simulator.4105.thailand.mixer.final_mixer_7.batch_no"
                    batch_doc7= db.collection("COLLECTION_TAG_LIST_BAKERY_DASHBOARD1").document(batch_no_tag7).get()
                    batch_7 = batch_doc7.to_dict()['value'] + 11
                    if batch == batch_7:
                        data = {
                            "SIGNAL": False
                        }
                        signal_id = f'{item[5]}.{doc_id_suffix}.SIGNAL.SPONGE'
                        db.collection('COLLECTION_BAKERY_DASHBOARD1').document(signal_id).set(data)

                # start sponge mixing
                # elif machine_name == 'mixer' and area == 'sponge_mixer_4' and tag_name == 'start_time' and item[3]:
                #     batch_no_tag = "Simulator.4105.thailand.mixer.sponge_mixer_4.batch_no"
                #     batch_doc = db.collection("COLLECTION_TAG_LIST_BAKERY_DASHBOARD1").document(batch_no_tag).get()
                #     batch = ''
                #     if batch_doc.exists:
                #         batch = batch_doc.to_dict().get('value')
                #     print('batch: ', batch)
                    
                #     data = {
                #         'START_SPONGE_MIXING_TIME': item[4].strftime('%Y-%m-%d %H:%M:%S'),
                #         'STAGE': 'SPONGE MIX'
                #     }

                #     doc_id = f'{item[5]}.{str(int(batch))}.{doc_id_suffix}'
                #     doc = db.collection('COLLECTION_BAKERY_DASHBOARD1').document(doc_id).get()
                #     if doc.exists:
                #         print('existed')
                #         db.collection('COLLECTION_BAKERY_DASHBOARD1').document(doc_id).update(data)
                #         print('status: 200, successfully inserted data to firestore')
                    
                #     # mixer 7
                #     batch_no_tag7 = "Simulator.4105.thailand.mixer.final_mixer_7.batch_no"
                #     batch_doc7= db.collection("COLLECTION_TAG_LIST_BAKERY_DASHBOARD1").document(batch_no_tag).get()
                #     batch_7 = batch_doc7.to_dict()['value'] + 1
                #     if batch == batch_7:
                #         data = {
                #             "SIGNAL": False
                #         }
                #         signal_id = f'{item[5]}.{doc_id_suffix}.SIGNAL.SPONGE'
                #         db.collection('COLLECTION_BAKERY_DASHBOARD1').document(signal_id).set(data)
                # stop sponge mixing
                elif machine_name == 'mixer' and area == 'sponge_mixer_4' and tag_name == 'stop_time' and item[3]:
                    batch_no_tag = "Simulator.4105.thailand.mixer.sponge_mixer_4.batch_no"
                    batch_doc = db.collection("COLLECTION_TAG_LIST_BAKERY_DASHBOARD1").document(batch_no_tag).get()
                    batch = ''

                    if batch_doc.exists:
                        batch = batch_doc.to_dict().get('value')
                    print('batch: ', batch)

                    data = {
                        'STOP_SPONGE_MIXING_TIME': item[4].strftime("%Y-%m-%d %H:%M:%S")
                    }
                    doc_id = f'{item[5]}.{str(int(batch))}.{doc_id_suffix}'
                    doc = db.collection('COLLECTION_BAKERY_DASHBOARD1').document(doc_id).get()
                    if doc.exists:
                        print('existed')
                        db.collection('COLLECTION_BAKERY_DASHBOARD1').document(doc_id).update(data)
                        print('status: 200, successfully inserted data to firestore')
                    
                    data = {
                        "SIGNAL": True,
                        "BATCH_NO": batch
                    }
                    signal_id = f'{item[5]}.{doc_id_suffix}.SIGNAL.IN'
                    db.collection('COLLECTION_BAKERY_DASHBOARD1').document(signal_id).set(data)
                # start fermentation
                elif machine_name == 'sponge_pusher' and area == 'preparation_room' and tag_name == 'sponge_pusher_on' and item[3] and item[2] == '1':
                    batch_no_tag = "Simulator.4105.thailand.mixer.sponge_mixer_4.batch_no"
                    batch_doc = db.collection("COLLECTION_TAG_LIST_BAKERY_DASHBOARD1").document(batch_no_tag).get()
                    batch = ''
                    if batch_doc.exists:
                        batch = batch_doc.to_dict().get('value')
                    print('batch: ', batch)

                    data = {
                        'START_FERMENTING_TIME': item[4].strftime('%Y-%m-%d %H:%M:%S'),
                        'FERMENTATION_STATUS': 'IN',
                        'STAGE': 'FERMENTATION'
                    }
                    doc_id = f'{item[5]}.{str(int(batch))}.{doc_id_suffix}'
                    doc = db.collection('COLLECTION_BAKERY_DASHBOARD1').document(doc_id).get()
                    if doc.exists:
                        print('existed')
                        db.collection('COLLECTION_BAKERY_DASHBOARD1').document(doc_id).update(data)
                        print('status: 200, successfully inserted data to firestore')
                    
                    signal_id = f'{item[5]}.{doc_id_suffix}.SIGNAL.IN'
                    doc = db.collection('COLLECTION_BAKERY_DASHBOARD1').document(signal_id).get()
                    if doc.exists:
                        if doc.to_dict()['BATCH_NO'] == batch:
                            data = {
                                "SIGNAL": False,
                                "BATCH": batch
                            }
                            db.collection('COLLECTION_BAKERY_DASHBOARD1').document(signal_id).set(data)
                
                # update fermentation status: out, update dough mix start time, dough mix status (on), stage = dough mix
                elif machine_name == 'mixer' and area == 'final_mixer_7' and tag_name == 'start_time' and item[3]:
                    batch_no_tag = "Simulator.4105.thailand.mixer.final_mixer_7.batch_no"
                    batch_doc = db.collection("COLLECTION_TAG_LIST_BAKERY_DASHBOARD1").document(batch_no_tag).get()
                    batch = ''
                    if batch_doc.exists:
                        batch = batch_doc.to_dict().get('value')
                    print('batch: ', batch)

                    data = {
                        'FERMENTATION_STATUS': 'OUT',
                        'STAGE': 'DOUGH MIX',
                        'DOUGH_MIXING_STATUS': 'ON',
                        'START_DOUGH_MIXING_TIME': item[4].strftime('%Y-%m-%d %H:%M:%S')
                    }
                    doc_id = f'{item[5]}.{str(int(batch))}.{doc_id_suffix}'
                    doc = db.collection('COLLECTION_BAKERY_DASHBOARD1').document(doc_id).get()
                    if doc.exists:
                        print('existed')
                        db.collection('COLLECTION_BAKERY_DASHBOARD1').document(doc_id).update(data)
                        print('status: 200, successfully inserted data to firestore')

                    # signal
                    if batch > 11:
                        data = {
                            "SIGNAL": True
                        }
                        signal_id = f'{item[5]}.{doc_id_suffix}.SIGNAL.SPONGE'
                        db.collection('COLLECTION_BAKERY_DASHBOARD1').document(signal_id).set(data)
                
                # update dough mix status: off
                elif machine_name == 'final_mixer_7' and area == 'preparation_room' and tag_name == 'final_mixer_auto_mode' and item[3] and item[2] == '0':
                    batch_no_tag = "Simulator.4105.thailand.mixer.final_mixer_7.batch_no"
                    batch_doc = db.collection("COLLECTION_TAG_LIST_BAKERY_DASHBOARD1").document(batch_no_tag).get()
                    batch = ''
                    if batch_doc.exists:
                        batch = batch_doc.to_dict().get('value')
                    print('batch: ', batch)

                    doc_id = f'{item[5]}.{str(int(batch))}.{doc_id_suffix}'
                    doc = db.collection('COLLECTION_BAKERY_DASHBOARD1').document(doc_id).get()
                    if doc.exists:
                        start_time = doc.to_dict().get('START_DOUGH_MIXING_TIME')
                        start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
                        end_dt = item[4]
                        time_period = (end_dt - start_dt).total_seconds() / 60
                        print('time_period: ', time_period)
                        if time_period > 14:
                            print('> 14 mins')
                            data = {
                                'STAGE': 'DOUGH MIX',
                                'DOUGH_MIXING_STATUS': 'OFF',
                                'STOP_DOUGH_MIXING_TIME': item[4].strftime('%Y-%m-%d %H:%M:%S')
                            }
                            db.collection('COLLECTION_BAKERY_DASHBOARD1').document(doc_id).update(data)
                            print('status: 200, successfully inserted data to firestore')
     
    elif event.get("mqtt_topic") == 'bakery/sponge_dough_temp':
        doc_id_suffix = f'{event["plant_id"]}.DASHBOARD1'
        time_iso_format = str(datetime.fromtimestamp(event['timestamp']/1000, timezone(timedelta(hours=7))))[:19]
        dt = datetime.fromtimestamp(event['timestamp']/1000, timezone(timedelta(hours=7)))
        factory_date = (dt - timedelta(hours=1)).date().isoformat()
        if event['machine_name'] == 'aft_proof_sensor':
            batch = event['batch_no']
            data = {
                'STOP_FERMENTING_TIME': time_iso_format
            }
            doc_id = f'{factory_date}.{str(int(batch))}.{doc_id_suffix}'
            doc = db.collection('COLLECTION_BAKERY_DASHBOARD1').document(doc_id).get()
            if doc.exists:
                db.collection('COLLECTION_BAKERY_DASHBOARD1').document(doc_id).update(data)
                print('status: 200, successfully inserted data to firestore')

    elif event.get("mqtt_topic") == 'bakery/camera':
        doc_id_suffix = f'{event["plant_id"]}.DASHBOARD1.BUN_COUNT'
        dt = datetime.fromtimestamp(event['timestamp'], timezone(timedelta(hours=7)))
        factory_date = (dt - timedelta(hours=1)).date().isoformat()
        if event['machine_name'] == 'BeforeBake_camera':
            if event['bun_count'] > 0:
                doc_id = f'{factory_date}.{doc_id_suffix}'
                doc = db.collection('COLLECTION_BAKERY_DASHBOARD1').document(doc_id).get()
                if doc.exists:
                    print('existed')
                    # find count
                    prev_bun_count = doc.to_dict()["LAST_COUNT"]
                    if prev_bun_count == event["bun_count"]:
                        pass
                    else:
                        tot_bun = (event["bun_count"] - doc.to_dict()['FIRST_COUNT'] + 1)
                        tot_time = (dt - doc.to_dict()['PRODUCTION_TIME_START'].astimezone(timezone(timedelta(hours=7)))).total_seconds()/3600
                        data = {
                            'PRODUCTION_TIME_STOP': dt,
                            'LAST_COUNT': event['bun_count'],
                            'DATE': factory_date,
                            'PERFORMANCE_PER_HOUR': round(tot_bun / tot_time)
                        }
                        db.collection('COLLECTION_BAKERY_DASHBOARD1').document(doc_id).update(data)
                        print('status: 200, successfully inserted data to firestore')
                else:
                    data = {
                        'PRODUCTION_TIME_START': dt,
                        'FIRST_COUNT': event['bun_count'],
                        'LAST_COUNT': event['bun_count'],
                        'DATE': factory_date
                    }
                    db.collection('COLLECTION_BAKERY_DASHBOARD1').document(doc_id).set(data)
                    print('status: 200, successfully inserted data to firestore')
