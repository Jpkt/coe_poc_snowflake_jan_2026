def lambda_handler(event, context):
    plant_id = event['v'].split('.')[0]
    factory = event['v'].split('.')[1]
    line = event['v'].split('.')[2]
    country = 'Thailand' if plant_id != 'B425' else 'Vietnam'

    connection = psycopg2.connect(
        user = RDS_USERNAME,
        password =  RDS_PASSWORD,
        host  = RDS_HOST,
        port = RDS_PORT,
        database = RDS_DATABASE_NAME
    )
    cursor = connection.cursor()
    try:
        # get is_completed
        is_completed_check_on = str(plant_id) + str(factory) + line + 'ViaTumbler' + 'is_completed'
        # qs_is_completed = "select value from misc where key = '{0}'"
        # cursor.execute(qs_is_completed.format(is_completed_check_on))
        # qs_is_completed_result = cursor.fetchall()
        # if len(qs_is_completed_result) == 0:
        #     qs_is_completed_result = [('TRUE',)]
        # qs_is_completed_result = qs_is_completed_result[0][0]

         # status = plant_id + factory_code + line + 'ViaTumbler' + 'status'
        connection_w = psycopg2.connect(
            user = RDS_USERNAME,
            password =  RDS_PASSWORD,
            host  = RDS_HOST,
            port = RDS_PORT,
            database = RDS_DATABASE_NAME_WEB
        )
        cursor_w = connection_w.cursor()
        sql_latest_status = '''
            SELECT status
            FROM core_line_status
            WHERE plant_id = %s
            AND CAST(factory as INT) = %s
            AND UPPER(line) = %s
            ORDER BY timestamp DESC
            LIMIT 1
        '''

        cursor_w.execute(sql_latest_status, (plant_id, str(int(factory)), line.upper()))
        row = cursor_w.fetchone()

        qs_is_completed_result = row[0].upper() if row else 'STOP'
        if connection_w:
            connection_w.close()

        # case 1: is_completed = True
        if qs_is_completed_result.upper() == 'STOP':
            print('case 1: is_completed == True')
            # find sku, actual_barrel_weight, min(at_process_timestamp)
            qs = """
            with shift_start as (
                select 
                    (case 
                        when extract(hour from (current_timestamp at time zone 'asia/bangkok')) >= 6 
                        and extract(hour from (current_timestamp at time zone 'asia/bangkok')) < 18 
                        then date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) + interval '6 hours'
                        when extract(hour from (current_timestamp at time zone 'asia/bangkok')) >= 18 
                        then date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) + interval '18 hours'
                        else date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) - interval '6 hours'
                    end) at time zone 'utc' as start_time
            )
            select
                distinct(ct.material_description), 
                sum(ct.actual_barrel_weight), 
                min(ct.at_process_timestamp) as min_process_timestamp, 
                ct.status
            from core_tumbler ct, shift_start ss
            where
                ct.plant_id = '{0}'
                and ct.factory = '{1}'
                and ct.line = '{2}'
                and (lower(ct.status) = 'processing' or lower(ct.status) = 'completed')
                and ((ct.at_process_timestamp >= ss.start_time and ct.at_completed_timestamp >= ss.start_time) or (ct.at_process_timestamp >= ss.start_time and ct.at_completed_timestamp is null))
            group by ct.material_description, ct.status
            order by min_process_timestamp desc
            limit 1;
            """
            cursor.execute(qs.format(plant_id, factory, line))
            result = cursor.fetchall()
            df = pd.DataFrame(result)
            if len(df) > 0:
                print('df tumbler > 0')
                df.columns = ['sku', 'actual_barrel_weight', 'min_at_processing_time', 'status']
                if len(df.sku.unique()) == 1:
                    print('df.sku.unique(): ', df.sku.unique())
                    start_datetime = df.min_at_processing_time.min()
                    sku = df.sku.unique()[0]
                    qs = """
                    with shift_start as (
                        select 
                            (case 
                                when extract(hour from (current_timestamp at time zone 'asia/bangkok')) >= 6 
                                and extract(hour from (current_timestamp at time zone 'asia/bangkok')) < 18 
                                then date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) + interval '6 hours'
                                when extract(hour from (current_timestamp at time zone 'asia/bangkok')) >= 18 
                                then date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) + interval '18 hours'
                                else date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) - interval '6 hours'
                            end) at time zone 'utc' as start_time
                    )
                    select
                        distinct(ct.material_description), 
                        sum(ct.actual_barrel_weight), 
                        min(ct.at_process_timestamp) as min_process_timestamp, 
                        ct.status
                    from core_tumbler ct, shift_start ss
                    where
                        ct.plant_id = '{0}'
                        and ct.factory = '{1}'
                        and ct.line = '{2}'
                        and (lower(ct.status) = 'processing' or lower(ct.status) = 'completed')
                        and ((ct.at_process_timestamp >= ss.start_time and ct.at_completed_timestamp >= ss.start_time) or (ct.at_process_timestamp >= ss.start_time and ct.at_completed_timestamp is null))
                    group by ct.material_description, ct.status
                    """
                    cursor.execute(qs.format(plant_id, factory, line))
                    result = cursor.fetchall()
                    df = pd.DataFrame(result)
                    df.columns = ['sku', 'actual_barrel_weight', 'min_at_processing_time', 'status']
                    actual_weight_kg = df.actual_barrel_weight.sum()
                    count_row_actual_weight_kg = len(df)
                    status = df.status.unique()
                    if "Processing" in status:
                        is_running = 'YES'
                    else:
                        is_running = 'NO'
                    # find factory_date and shift
                    hour = start_datetime.strftime('%Y-%m-%d %H:%M:%S')[11:13]
                    # factory_date
                    if int(hour) >= 6:
                        factory_date = start_datetime.strftime('%Y-%m-%d %H:%M:%S')[:10]
                    else:
                        factory_date = datetime.strftime(start_datetime - timedelta(days=1), '%Y-%m-%d %H:%M:%S')[:10]
                    # shift
                    if int(hour) >= 6 and int(hour) < 18:
                        shift = 'D'
                    elif int(hour) < 6 or int(hour) >= 18:
                        shift = 'N'
                    
                    # create row in database, insert plant_id, factory, line, factory_date, shift, sku, machine_type, machine_name, start_time, check_on
                    check_on = plant_id + factory + line + 'ViaTumbler' + sku + start_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    qs_insert = """
                                insert into line_monitor(
                                plant_id, factory, line, factory_date, shift, sku, machine_type, machine_name,
                                production_unit, actual_weight, start_datetime, check_on
                                )
                                values (
                                %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s,
                                %s, %s
                                )
                                on conflict (check_on) 
                                do update set
                                actual_weight = excluded.actual_weight
                    """
                    cursor.execute(qs_insert, (plant_id, factory, line, factory_date, shift, sku,
                        'Tumbler', 'Tumbler', 'kg', actual_weight_kg,
                        start_datetime.strftime('%Y-%m-%d %H:%M:%S'), check_on)
                    )
                    connection.commit()

                    # find production time
                    qs_production_time = """
                                    with shift_start as (
                                    select 
                                        (case 
                                            when extract(hour from (current_timestamp at time zone 'asia/bangkok')) >= 6 
                                            and extract(hour from (current_timestamp at time zone 'asia/bangkok')) < 18 
                                            then date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) + interval '6 hours'

                                            when extract(hour from (current_timestamp at time zone 'asia/bangkok')) >= 18 
                                            then date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) + interval '18 hours'

                                            else date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) - interval '6 hours'
                                        end) at time zone 'utc' as start_time
                                    )
                                    select
                                        sum(coalesce(ct.at_completed_timestamp, current_timestamp at time zone 'asia/bangkok') - ct.at_process_timestamp)
                                        as total_time_difference,
                                        sum(extract(epoch from (coalesce(ct.at_completed_timestamp, current_timestamp at time zone 'asia/bangkok') - ct.at_process_timestamp))) / 3600
                                        as total_hour,
                                        sum(extract(epoch from (coalesce(ct.at_completed_timestamp, current_timestamp at time zone 'asia/bangkok') - ct.at_process_timestamp))) / 60
                                        as total_minute
                                        from core_tumbler ct, shift_start ss
                                        where
                                        ct.plant_id = '{0}'
                                        and ct.factory = '{1}'
                                        and ct.line = '{2}'
                                        and ct.material_description = '{3}'
                                        and (lower(ct.status) = 'processing' or lower(ct.status) = 'completed')
                                        and ((ct.at_process_timestamp >= ss.start_time and ct.at_completed_timestamp >= ss.start_time) or (ct.at_process_timestamp >= ss.start_time and ct.at_completed_timestamp is null))
                                    ;
                                
                    """
                    cursor.execute(qs_production_time.format(plant_id, factory, line, sku))
                    result_prod_time= cursor.fetchall()
                    if len(result_prod_time) > 0:
                        prod_time_str = result_prod_time[0][0]
                        prod_time_min = result_prod_time[0][2]
                        # update prod_time
                        qs_update_std_feed_rate = """
                                                update line_monitor
                                                set production_time = %s
                                                where check_on = %s
                        """
                        cursor.execute(qs_update_std_feed_rate, (prod_time_min, check_on))
                        connection.commit()
                        print('updated production time')
                    else:
                        prod_time_str = "00:00:00"
                        prod_time_min = 0


                    # find non_production time
                    qs_non_production_time = """
                                    with shift_start as (
                                    select 
                                        (case 
                                            when extract(hour from (current_timestamp at time zone 'asia/bangkok')) >= 6 
                                            and extract(hour from (current_timestamp at time zone 'asia/bangkok')) < 18 
                                            then date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) + interval '6 hours'

                                            when extract(hour from (current_timestamp at time zone 'asia/bangkok')) >= 18 
                                            then date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) + interval '18 hours'

                                            else date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) - interval '6 hours'
                                        end) at time zone 'utc' as start_time
                                    ),
                                    production_times as (
                                        select 
                                            ct.id,
                                            ct.plant_id,
                                            ct.factory,
                                            ct.line,
                                            ct.material_description,
                                            ct.status,
                                            ct.at_completed_timestamp,
                                            coalesce(ct.at_process_timestamp, current_timestamp at time zone 'Asia/Bangkok') as at_process_timestamp,
                                            lead(coalesce(ct.at_process_timestamp, current_timestamp at time zone 'Asia/Bangkok')) 
                                                over (partition by ct.plant_id, ct.factory, ct.line 
                                                    order by coalesce(ct.at_process_timestamp, current_timestamp at time zone 'Asia/Bangkok')) 
                                                as next_process_timestamp,
                                            GREATEST(
                                            (
                                                lead(coalesce(ct.at_process_timestamp, current_timestamp at time zone 'Asia/Bangkok'))
                                                    over (
                                                        partition by ct.plant_id, ct.factory, ct.line
                                                        order by coalesce(ct.at_process_timestamp, current_timestamp at time zone 'Asia/Bangkok')
                                                    )
                                                - ct.at_completed_timestamp
                                            ),
                                            interval '0 second'
                                        ) as non_production_time
                                        from core_tumbler ct
                                    )
                                    select 
                                        sum(non_production_time) as total_non_production_time,
                                        sum(extract(epoch from non_production_time)) / 60 as total_non_production_minutes
                                    from production_times, shift_start
                                    where
                                        plant_id = '{0}'
                                        and factory = '{1}'
                                        and lower(line) = lower('{2}')
                                        and material_description = '{3}'
                                        and (lower(status) = 'processing' or lower(status) = 'completed')
                                        and ((at_process_timestamp >= shift_start.start_time or at_completed_timestamp >= shift_start.start_time) or (at_process_timestamp >= shift_start.start_time and at_completed_timestamp is null));
                    """
                    cursor.execute(qs_non_production_time.format(plant_id, factory, line, sku))
                    result_non_prod_time= cursor.fetchall()
                    if len(result_non_prod_time) > 0:
                        non_prod_time_min = result_non_prod_time[0][1]
                        non_prod_time_str = result_non_prod_time[0][0]
                        # update prod_time
                        qs_update_non_prod_time = """
                                                update line_monitor
                                                set non_production_time = %s
                                                where check_on = %s
                        """
                        cursor.execute(qs_update_non_prod_time, (non_prod_time_min, check_on))
                        connection.commit()
                        print('updated non-production time')
                    else:
                        non_prod_time_min = 0
                        non_prod_time_str = "00:00:00"


                    # find standard pack
                    qs_std = """
                                select multihead_auto_pack, belt_scale_feed_rate, multihead_target_pack
                                from core_master_std 
                                where plant_id = '{0}' and factory = '{1}' and line = '{2}' and
                                material_description = '{3}'
                                order by updated_at desc limit 1
                    """
                    cursor.execute(qs_std.format(plant_id, factory, line, sku))
                    result_master_std = cursor.fetchall()
                    if len(result_master_std) == 1:
                        performance_std = result_master_std[0][1]
                        # update belt_scale_feed_rate
                        qs_update_std_feed_rate = """
                                                update line_monitor
                                                set performance_std = %s
                                                where check_on = %s
                        """
                        cursor.execute(qs_update_std_feed_rate, (performance_std, check_on))
                        connection.commit()
                        print('updated std feed rate')
                    else:
                        performance_std = 'NA'
                    
                    # find actual feed rate
                    if len(result_prod_time) > 0:
                        actual_feed_rate = round(actual_weight_kg / float(result_prod_time[0][1]), 0)
                        # update actual_feed_rate
                        qs_update_std_feed_rate = """
                                                update line_monitor
                                                set actual_feed_rate = %s
                                                where check_on = %s
                        """
                        cursor.execute(qs_update_std_feed_rate, (actual_feed_rate, check_on))
                        connection.commit()
                        print('updated actual feed rate')
                    else:
                        actual_feed_rate = 0

                    # find actual feed rate (last bin)
                    qs_last_bin = '''
                                    with shift_start as (
                                    select 
                                        (case 
                                            when extract(hour from (current_timestamp at time zone 'asia/bangkok')) >= 6 
                                            and extract(hour from (current_timestamp at time zone 'asia/bangkok')) < 18 
                                            then date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) + interval '6 hours'

                                            when extract(hour from (current_timestamp at time zone 'asia/bangkok')) >= 18 
                                            then date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) + interval '18 hours'

                                            else date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) - interval '6 hours'
                                        end) at time zone 'utc' as start_time
                                    )
                                    select
                                        (extract(epoch from (coalesce(ct.at_completed_timestamp, current_timestamp at time zone 'asia/bangkok') - ct.at_process_timestamp))) / 3600
                                        as total_hour,
                                        (ct.actual_barrel_weight) as actual_weight
                                        from core_tumbler ct, shift_start ss
                                        where
                                        ct.plant_id = '{0}'
                                        and ct.factory = '{1}'
                                        and ct.line = '{2}'
                                        and ct.material_description = '{3}'
                                        and lower(ct.status) = 'completed'
                                        and ct.at_completed_timestamp >= ss.start_time
										order by (ct.at_completed_timestamp) desc limit 1
                                    ;
                    '''.format(plant_id, factory, line, sku)
                    cursor.execute(qs_last_bin)
                    last_bin_result = cursor.fetchall()
                    if len(last_bin_result) > 0:
                        last_bin_hour = last_bin_result[0][0]
                        last_bin_weight = last_bin_result[0][1]
                        last_bin_feed_rate = float(last_bin_weight) / float(last_bin_hour)
                    else:
                        last_bin_feed_rate = 0
                    

                    # find target po
                    qs_target = """
                                select 
                                    sum(production_quantity) as target_kg
                                from sap_outbound 
                                where 
                                    plant_id = '{0}'
                                    and factory = '{1}'
                                    and work_center = '{2}'
                                    and schedule_start_date = '{3}'
                                    and shift = '{4}'
                                    and material_description like '%{5}:%'
                                    and status_process_order not like '%TECO %'
                    """.format(plant_id, factory, line, factory_date, shift, sku)
                    cursor.execute(qs_target)
                    target_result = cursor.fetchall()
                    if len(target_result) > 0:
                        target_po = target_result[0][0]
                        # update target_po
                        qs_update_std_feed_rate = """
                                                update line_monitor
                                                set production_target = %s
                                                where check_on = %s
                        """
                        cursor.execute(qs_update_std_feed_rate, (target_po, check_on))
                        connection.commit()
                        print('updated target po')

                        # update current__sku_matched
                        current_sku_matched = 'YES'
                        sql_insert_query = '''INSERT INTO misc(
                                                        key, value
                                                    )
                                                    
                                                    VALUES (%s, %s)
                                                    ON CONFLICT (key) DO UPDATE
                                                    SET value = EXCLUDED.value
                                            '''
                        cursor.execute(sql_insert_query, (plant_id + factory + line + 'ViaTumbler' + 'current_sku_matched', current_sku_matched))
                        connection.commit()
                        print('update current_sku_matched')

                        if target_po is None:
                            target_po = 'NA'

                        # actual weight in percent
                        if target_result[0][0] is not None:
                            percent_actual = round(actual_weight_kg / float(target_result[0][0]) * 100)
                            if percent_actual > 100:
                                percent_actual = 100
                        else:
                            percent_actual = 0
                        
                        
                        # actual weight remaining in percent
                        percent_remaining = 100 - percent_actual
                    else:
                        current_sku_matched = 'NO'
                        sql_insert_query = '''INSERT INTO misc(
                                                        key, value
                                                    )
                                                    
                                                    VALUES (%s, %s)
                                                    ON CONFLICT (key) DO UPDATE
                                                    SET value = EXCLUDED.value
                                            '''
                        cursor.execute(sql_insert_query, (plant_id + factory + line + 'ViaTumbler' + 'current_sku_matched', current_sku_matched))
                        connection.commit()
                        print('update not current_sku_matched')
                        percent_actual = 0
                        percent_remaining = 0
                        target_po = 'NA'

                    # find forecast finish time
                    print('find forecast finish time')
                    if len(target_result) > 0 and len(result_master_std) == 1 and performance_std is not None:
                        if target_result[0][0] is not None:
                            finish_time = float(target_result[0][0]) / float(performance_std)
                            hour = int(finish_time)
                            minute = int((finish_time - hour) * 60)
                            forecast_finish_datetime = start_datetime + timedelta(hours=hour) + timedelta(minutes=minute)
                    else:
                        forecast_finish_datetime = 'NA'
                    
                    print('find target now')
                    # find target now
                    if len(result_master_std) == 1 and len(result_prod_time) > 0 and len(target_result) > 0:
                        if target_result[0][0] is not None:
                            current_bangkok_time = datetime.now(timezone.utc) + timedelta(hours=7)
                            time_diff_in_hr = (current_bangkok_time - start_datetime).total_seconds() / 3600
                            target_now_kg = float(performance_std) * time_diff_in_hr
                            percent_target_now = round(target_now_kg / float(target_result[0][0])  * 100, 0)
                        else:
                            target_now_kg = 'NA'
                            percent_target_now = 'NA'
                    else:
                        target_now_kg = 'NA'
                        percent_target_now = 'NA'

                    # forecast kg at the end of the shift
                    if shift == 'D':
                        planned_end_time = factory_date + ' 18:00:00'
                    else:
                        planned_end_time = (datetime.strptime(factory_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d') + ' 06:00:00'
                    
                    if forecast_finish_datetime != 'NA':
                        print('forecast finish datetime: ', forecast_finish_datetime)
                        print('planned end time: ', datetime.strptime(planned_end_time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc))
                        if forecast_finish_datetime > datetime.strptime(planned_end_time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc):
                            planned_end_time = planned_end_time
                        else:
                            planned_end_time = forecast_finish_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    remaining_time = (datetime.strptime(planned_end_time, '%Y-%m-%d %H:%M:%S') - (datetime.now() + timedelta(hours=7))).total_seconds()/3600
                    forecast_finished_kg = round(actual_feed_rate * remaining_time + actual_weight_kg, 0)
                    remaining_time_second = (datetime.strptime(planned_end_time, '%Y-%m-%d %H:%M:%S') - (datetime.now() + timedelta(hours=7))).total_seconds()
                    print('planned end time: ', planned_end_time)
                    print('now: ', datetime.now() + timedelta(hours=7))
                    print('remaining end time: ', remaining_time) 

                    # buffer unrealistic forecast kg
                    if count_row_actual_weight_kg == 1:
                        forecast_finished_kg = 'NA'

                    # update misc
                    # current_check_on = plant_id + factory_code + line + 'ViaTumbler' + 'check_on'
                    sql_insert_query = '''INSERT INTO misc(
                                                    key, value
                                                )
                                                
                                                VALUES (%s, %s)
                                                ON CONFLICT (key) DO UPDATE
                                                SET value = EXCLUDED.value
                                        '''
                    cursor.execute(sql_insert_query, (plant_id + factory + line + 'ViaTumbler' + 'check_on', check_on))
                    connection.commit()
                    print('misc: update current_check_on')

                    # current_sku = plant_id + factory_code + line + 'ViaTumbler' + 'sku'
                    sql_insert_query = '''INSERT INTO misc(
                                                    key, value
                                                )
                                                
                                                VALUES (%s, %s)
                                                ON CONFLICT (key) DO UPDATE
                                                SET value = EXCLUDED.value
                                        '''
                    cursor.execute(sql_insert_query, (plant_id + factory + line + 'ViaTumbler' + 'sku', sku))
                    connection.commit()
                    print('misc: update current_sku')

                    # current_factory_date = plant_id + factory_code + line + 'ViaTumbler' + 'factory_date'
                    sql_insert_query = '''INSERT INTO misc(
                                                    key, value
                                                )
                                                
                                                VALUES (%s, %s)
                                                ON CONFLICT (key) DO UPDATE
                                                SET value = EXCLUDED.value
                                        '''
                    cursor.execute(sql_insert_query, (plant_id + factory + line + 'ViaTumbler' + 'factory_date', factory_date))
                    connection.commit()
                    print('misc: update current_factory_date')

                    # current_shift = plant_id + factory_code + line + 'ViaTumbler' + 'shift'
                    sql_insert_query = '''INSERT INTO misc(
                                                    key, value
                                                )
                                                
                                                VALUES (%s, %s)
                                                ON CONFLICT (key) DO UPDATE
                                                SET value = EXCLUDED.value
                                        '''
                    cursor.execute(sql_insert_query, (plant_id + factory + line + 'ViaTumbler' + 'shift', shift))
                    connection.commit()
                    print('misc: update current_shift')
                    
                    # is_completed = plant_id + factory_code + line + 'ViaTumbler' + 'is_completed'
                    sql_insert_query = '''INSERT INTO misc(
                                                    key, value
                                                )
                                                
                                                VALUES (%s, %s)
                                                ON CONFLICT (key) DO UPDATE
                                                SET value = EXCLUDED.value
                                        '''
                    cursor.execute(sql_insert_query, (plant_id + factory + line + 'ViaTumbler' + 'is_completed', 'False'))
                    connection.commit()
                    print('misc: update is_completed')

                    # status = plant_id + factory_code + line + 'ViaTumbler' + 'status'
                    connection_w = psycopg2.connect(
                        user = RDS_USERNAME,
                        password =  RDS_PASSWORD,
                        host  = RDS_HOST,
                        port = RDS_PORT,
                        database = RDS_DATABASE_NAME_WEB
                    )
                    cursor_w = connection_w.cursor()
                    sql_insert_query = '''INSERT INTO core_line_status(
                                                    plant_id, factory, line,
                                                    status, timestamp, country, factory_date
                                                )
                                                
                                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                        '''
                    cursor_w.execute(sql_insert_query, (plant_id, factory, line, 'Run', start_datetime.strftime('%Y-%m-%d %H:%M:%S'), country, factory_date))
                    connection_w.commit()
                    if connection_w:
                        connection_w.close()

                    # update firestore (factory_date, shift, sku, production_unit, actual_weight, start_datetime)
                    doc_id = plant_id + factory + line + 'ViaTumbler' + 'Performance'
                    data = {
                        "PLANT_ID": plant_id,
                        "FACTORY_CODE": factory,
                        "LINE": line,
                        "FACTORY_DATE": factory_date,
                        "SHIFT": shift,
                        "SKU": sku,
                        "ACTUAL_WEIGHT": float(actual_weight_kg),
                        "REMAINING_WEIGHT": float(target_po) -  float(actual_weight_kg) if target_po != 'NA' else 'NA',
                        "TARGET_WEIGHT_KG": float(target_po) if target_po != 'NA' else 'NA',
                        "PERCENT_ACTUAL_WEIGHT": float(percent_actual) if percent_actual != 'NA' else 'NA',
                        "PERCENT_REMAINING_WEIGHT": float(percent_remaining) if percent_remaining != 'NA' else 'NA',
                        # "ACTUAL_FEED_RATE": float(actual_feed_rate) if actual_feed_rate != 'NA' else 'NA', version 1
                        # "ACTUAL_FEED_RATE_LAST_BIN": last_bin_feed_rate, version 1
                        "ACTUAL_FEED_RATE": 'NA', # version 2 (current)
                        "ACTUAL_FEED_RATE_LAST_BIN": 'NA', # version 2 (current)
                        "STD_FEED_RATE": float(performance_std) if performance_std != 'NA' else 'NA',
                        "START_DATETIME": start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        "FORECAST_FINISH_DATETIME": forecast_finish_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        "FINISH_DATETIME": 'NA',
                        "PRODUCTION_TIME": str(prod_time_str).zfill(8),
                        "NON_PRODUCTION_TIME": str(non_prod_time_str).zfill(8),
                        "TARGET_NOW_KG": float(target_now_kg) if target_now_kg != 'NA' else 'NA',
                        "PERCENT_TARGET_NOW": float(percent_target_now) if percent_target_now != 'NA' else 'NA',
                        "FORECAST_IN_KG": float(forecast_finished_kg) if forecast_finished_kg != 'NA' else 'NA',
                        "FORECAST": forecast_finished_kg - float(target_po) if (target_po != 'NA' and forecast_finished_kg != 'NA') else 'NA',
                        "IS_SKU_MATCHED": current_sku_matched,
                        "IS_RUNNING": is_running,
                        "LATEST_UPDATE": (datetime.now() + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S'),
                        "STATUS": "Run"
                    }
                    print(data)
                    # Get document object
                    doc = db.collection('COLLECTION_PERFORMANCE_P12_V2').document(doc_id).get()
                    if doc.exists:
                        db.collection('COLLECTION_PERFORMANCE_P12_V2').document(doc_id).update(data)
                    else:
                        db.collection('COLLECTION_PERFORMANCE_P12_V2').document(doc_id).set(data)

            else:
                print('lenght of df tumbler = 0')
        # case 2: is_completed = False
        else:
            print('case 2: is_completed = False')
            # get latest check_on
            qs_check_on = "select value from misc where key = '{0}'"
            cursor.execute(qs_check_on.format(plant_id + factory + line + 'ViaTumbler' + 'check_on'))
            check_on = cursor.fetchall()[0][0]
            print('check_on: ', check_on)

            # get sku
            qs_sku = "select value from misc where key = '{0}'"
            cursor.execute(qs_sku.format(plant_id + factory + line + 'ViaTumbler' + 'sku'))
            current_sku = cursor.fetchall()[0][0]
            print('current sku: ', current_sku)

            # get factory_date
            qs_factory_date = "select value from misc where key = '{0}'"
            cursor.execute(qs_factory_date.format(plant_id + factory + line + 'ViaTumbler' + 'factory_date'))
            current_factory_date = cursor.fetchall()[0][0]
            print('current factory_date: ', current_factory_date)

            # get shift
            qs_shift = "select value from misc where key = '{0}'"
            cursor.execute(qs_shift.format(plant_id + factory + line + 'ViaTumbler' + 'shift'))
            current_shift = cursor.fetchall()[0][0]
            print('current shift: ', current_shift)

            # find server factory date and shift
            server_now = (datetime.now() + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S')
            print('server_now: ', server_now)
            server_hour = server_now[11:13]
            if int(server_hour) >= 6:
                server_factory_date = server_now[:10]
            else:
                server_factory_date = datetime.strftime(datetime.strptime(server_now, '%Y-%m-%d %H:%M:%S') - timedelta(days=1), '%Y-%m-%d %H:%M:%S')[:10]
            # shift
            if int(server_hour) >= 6 and int(server_hour) < 18:
                server_shift = 'D'
            elif int(server_hour) < 6 or int(server_hour) >= 18:
                server_shift = 'N'
            
            print('server_factory_date: ', server_factory_date, ' server_shift: ', server_shift)
            print('misc_factory_date: ', current_factory_date, ' misc_shift: ', current_shift)

            if (server_factory_date == current_factory_date) and (server_shift == current_shift):
                print('server factory = current factory and server shift = current shift')
                # find sku, actual_barrel_weight, min(at_process_timestamp)
                qs = """
                with shift_start as (
                    select 
                        (case 
                            when extract(hour from (current_timestamp at time zone 'asia/bangkok')) >= 6 
                            and extract(hour from (current_timestamp at time zone 'asia/bangkok')) < 18 
                            then date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) + interval '6 hours'

                            when extract(hour from (current_timestamp at time zone 'asia/bangkok')) >= 18 
                            then date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) + interval '18 hours'

                            else date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) - interval '6 hours'
                        end) at time zone 'utc' as start_time
                )
                select
                    distinct(ct.material_description), sum(ct.actual_barrel_weight), min(ct.at_process_timestamp), ct.status
                    from core_tumbler ct, shift_start ss
                    where
                    ct.plant_id = '{0}'
                    and ct.factory = '{1}'
                    and ct.line = '{2}'
                    and material_description = '{3}'
                    and (lower(ct.status) = 'processing' or lower(ct.status) = 'completed')
                    and ((ct.at_process_timestamp >= ss.start_time and ct.at_completed_timestamp >= ss.start_time) or (ct.at_process_timestamp >= ss.start_time and ct.at_completed_timestamp is null))
                    group by ct.material_description, ct.status
                    ;
                """
                cursor.execute(qs.format(plant_id, factory, line, current_sku))
                result = cursor.fetchall()
                df = pd.DataFrame(result)
                if len(df) > 0:
                    print('df tumbler > 0')
                    df.columns = ['sku', 'actual_barrel_weight', 'min_at_processing_time', 'status']
                    if len(df.sku.unique()) == 1:
                        print('df.sku.unique(): ', df.sku.unique())
                        start_datetime = df.min_at_processing_time.min()
                        sku = df.sku.unique()[0]
                        if sku == current_sku:
                            actual_weight_kg = df.actual_barrel_weight.sum()
                            status = df.status.unique()
                            print('all status: ', status)
                            if "Processing" in status:
                                is_running = 'YES'
                            else:
                                is_running = 'NO'
                            # find factory_date and shift
                            hour = start_datetime.strftime('%Y-%m-%d %H:%M:%S')[11:13]
                            # factory_date
                            if int(hour) >= 6:
                                factory_date = start_datetime.strftime('%Y-%m-%d %H:%M:%S')[:10]
                            else:
                                factory_date = datetime.strftime(start_datetime - timedelta(days=1), '%Y-%m-%d %H:%M:%S')[:10]
                            # shift
                            if int(hour) >= 6 and int(hour) < 18:
                                shift = 'D'
                            elif int(hour) < 6 or int(hour) >= 18:
                                shift = 'N'
                            
                            # create row in database, insert plant_id, factory, line, factory_date, shift, sku, machine_type, machine_name, start_time, check_on
                            qs_insert = """
                                        insert into line_monitor(
                                        plant_id, factory, line, factory_date, shift, sku, machine_type, machine_name,
                                        production_unit, actual_weight, start_datetime, check_on
                                        )
                                        values (
                                        %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s,
                                        %s, %s
                                        )
                                        on conflict (check_on) 
                                        do update set
                                            actual_weight = excluded.actual_weight
                            """
                            cursor.execute(qs_insert, (plant_id, factory, line, factory_date, shift, sku,
                                'Tumbler', 'Tumbler', 'kg', actual_weight_kg,
                                start_datetime.strftime('%Y-%m-%d %H:%M:%S'), check_on)
                            )
                            connection.commit()

                            # find production time
                            qs_production_time = """
                                            with shift_start as (
                                            select 
                                                (case 
                                                    when extract(hour from (current_timestamp at time zone 'asia/bangkok')) >= 6 
                                                    and extract(hour from (current_timestamp at time zone 'asia/bangkok')) < 18 
                                                    then date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) + interval '6 hours'

                                                    when extract(hour from (current_timestamp at time zone 'asia/bangkok')) >= 18 
                                                    then date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) + interval '18 hours'

                                                    else date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) - interval '6 hours'
                                                end) at time zone 'utc' as start_time
                                            )
                                            select
                                                sum(coalesce(ct.at_completed_timestamp, current_timestamp at time zone 'asia/bangkok') - ct.at_process_timestamp)
                                                as total_time_difference,
                                                sum(extract(epoch from (coalesce(ct.at_completed_timestamp, current_timestamp at time zone 'asia/bangkok') - ct.at_process_timestamp))) / 3600
                                                as total_hour,
                                                sum(extract(epoch from (coalesce(ct.at_completed_timestamp, current_timestamp at time zone 'asia/bangkok') - ct.at_process_timestamp))) / 60
                                                as total_minute
                                                from core_tumbler ct, shift_start ss
                                                where
                                                ct.plant_id = '{0}'
                                                and ct.factory = '{1}'
                                                and ct.line = '{2}'
                                                and ct.material_description = '{3}'
                                                and (lower(ct.status) = 'processing' or lower(ct.status) = 'completed')
                                                and ((ct.at_process_timestamp >= ss.start_time and ct.at_completed_timestamp >= ss.start_time) or (ct.at_process_timestamp >= ss.start_time and ct.at_completed_timestamp is null))
                                            ;
                                        
                            """
                            cursor.execute(qs_production_time.format(plant_id, factory, line, sku))
                            result_prod_time= cursor.fetchall()
                            if len(result_prod_time) > 0:
                                prod_time_str = result_prod_time[0][0]
                                prod_time_min = result_prod_time[0][2]
                                # update prod_time
                                qs_update_std_feed_rate = """
                                                        update line_monitor
                                                        set production_time = %s
                                                        where check_on = %s
                                """
                                cursor.execute(qs_update_std_feed_rate, (prod_time_min, check_on))
                                connection.commit()
                                print('updated production time')
                            else:
                                prod_time_str = "00:00:00"
                                prod_time_min = 0


                            # find non_production time
                            qs_non_production_time = """
                                            with shift_start as (
                                            select 
                                                (case 
                                                    when extract(hour from (current_timestamp at time zone 'asia/bangkok')) >= 6 
                                                    and extract(hour from (current_timestamp at time zone 'asia/bangkok')) < 18 
                                                    then date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) + interval '6 hours'

                                                    when extract(hour from (current_timestamp at time zone 'asia/bangkok')) >= 18 
                                                    then date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) + interval '18 hours'

                                                    else date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) - interval '6 hours'
                                                end) at time zone 'utc' as start_time
                                            ),
                                            production_times as (
                                                select 
                                                    ct.id,
                                                    ct.plant_id,
                                                    ct.factory,
                                                    ct.line,
                                                    ct.material_description,
                                                    ct.status,
                                                    ct.at_completed_timestamp,
                                                    coalesce(ct.at_process_timestamp, current_timestamp at time zone 'Asia/Bangkok') as at_process_timestamp,
                                                    lead(coalesce(ct.at_process_timestamp, current_timestamp at time zone 'Asia/Bangkok')) 
                                                        over (partition by ct.plant_id, ct.factory, ct.line 
                                                            order by coalesce(ct.at_process_timestamp, current_timestamp at time zone 'Asia/Bangkok')) 
                                                        as next_process_timestamp,
                                                    GREATEST(
                                                        (
                                                            lead(coalesce(ct.at_process_timestamp, current_timestamp at time zone 'Asia/Bangkok'))
                                                                over (
                                                                    partition by ct.plant_id, ct.factory, ct.line
                                                                    order by coalesce(ct.at_process_timestamp, current_timestamp at time zone 'Asia/Bangkok')
                                                                )
                                                            - ct.at_completed_timestamp
                                                        ),
                                                        interval '0 second'
                                                    ) as non_production_time
                                                from core_tumbler ct
                                            )
                                            select 
                                                sum(non_production_time) as total_non_production_time,
                                                sum(extract(epoch from non_production_time)) / 60 as total_non_production_minutes
                                            from production_times, shift_start
                                            where
                                                plant_id = '{0}'
                                                and factory = '{1}'
                                                and lower(line) = lower('{2}')
                                                and material_description = '{3}'
                                                and (lower(status) = 'processing' or lower(status) = 'completed')
                                                and ((at_process_timestamp >= shift_start.start_time or at_completed_timestamp >= shift_start.start_time) or (at_process_timestamp >= shift_start.start_time and at_completed_timestamp is null));
                            """
                            cursor.execute(qs_non_production_time.format(plant_id, factory, line, sku))
                            result_non_prod_time= cursor.fetchall()
                            if len(result_non_prod_time) > 0:
                                non_prod_time_min = result_non_prod_time[0][1]
                                non_prod_time_str = result_non_prod_time[0][0]
                                # update prod_time
                                qs_update_non_prod_time = """
                                                        update line_monitor
                                                        set non_production_time = %s
                                                        where check_on = %s
                                """
                                cursor.execute(qs_update_non_prod_time, (non_prod_time_min, check_on))
                                connection.commit()
                                print('updated non-production time')
                            else:
                                non_prod_time_min = 0
                                non_prod_time_str = "00:00:00"


                            # find standard pack
                            qs_std = """
                                        select multihead_auto_pack, belt_scale_feed_rate, multihead_target_pack
                                        from core_master_std 
                                        where plant_id = '{0}' and factory = '{1}' and line = '{2}' and
                                        material_description = '{3}'
                                        order by updated_at desc limit 1
                            """
                            cursor.execute(qs_std.format(plant_id, factory, line, sku))
                            result_master_std = cursor.fetchall()
                            if len(result_master_std) == 1:
                                performance_std = result_master_std[0][1]
                                # update belt_scale_feed_rate
                                qs_update_std_feed_rate = """
                                                        update line_monitor
                                                        set performance_std = %s
                                                        where check_on = %s
                                """
                                cursor.execute(qs_update_std_feed_rate, (performance_std, check_on))
                                connection.commit()
                                print('updated std feed rate')
                            else:
                                performance_std = 'NA'
                            
                            # find actual feed rate
                            if len(result_prod_time) > 0:
                                # find actual kg that has at_completed_timestamp
                                qs = """
                                        with shift_start as (
                                            select 
                                                (case 
                                                    when extract(hour from (current_timestamp at time zone 'asia/bangkok')) >= 6 
                                                    and extract(hour from (current_timestamp at time zone 'asia/bangkok')) < 18 
                                                    then date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) + interval '6 hours'

                                                    when extract(hour from (current_timestamp at time zone 'asia/bangkok')) >= 18 
                                                    then date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) + interval '18 hours'

                                                    else date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) - interval '6 hours'
                                                end) at time zone 'utc' as start_time
                                        )
                                        select
                                            distinct(ct.material_description), 
                                            sum(ct.actual_barrel_weight), 
                                            min(ct.at_process_timestamp), 
                                            ct.status
                                        from core_tumbler ct, shift_start ss
                                        where
                                            ct.plant_id = '{0}'
                                            and ct.factory = '{1}'
                                            and ct.line = '{2}'
                                            and material_description = '{3}'
                                            and (lower(ct.status) = 'processing' or lower(ct.status) = 'completed')
                                            and (ct.at_process_timestamp >= ss.start_time and ct.at_completed_timestamp >= ss.start_time)
                                            and ct.at_completed_timestamp is not null
                                        group by ct.material_description, ct.status;
                                """
                                cursor.execute(qs.format(plant_id, factory, line, current_sku))
                                result_feedrate = cursor.fetchall()
                                df_feedrate = pd.DataFrame(result_feedrate)
                                df_feedrate.columns = ['sku', 'actual_barrel_weight', 'min_at_processing_time', 'status']
                                actual_weight_kg_feedrate = df_feedrate.actual_barrel_weight.sum()

                                actual_feed_rate = round(actual_weight_kg_feedrate / float(result_prod_time[0][1]), 0)
                                # update actual_feed_rate
                                qs_update_std_feed_rate = """
                                                        update line_monitor
                                                        set actual_feed_rate = %s
                                                        where check_on = %s
                                """
                                cursor.execute(qs_update_std_feed_rate, (actual_feed_rate, check_on))
                                connection.commit()
                                print('updated actual feed rate')
                            else:
                                actual_feed_rate = 0

                            # find actual feed rate (last bin)
                            qs_last_bin = '''
                                            with shift_start as (
                                            select 
                                                (case 
                                                    when extract(hour from (current_timestamp at time zone 'asia/bangkok')) >= 6 
                                                    and extract(hour from (current_timestamp at time zone 'asia/bangkok')) < 18 
                                                    then date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) + interval '6 hours'

                                                    when extract(hour from (current_timestamp at time zone 'asia/bangkok')) >= 18 
                                                    then date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) + interval '18 hours'

                                                    else date_trunc('day', (current_timestamp at time zone 'asia/bangkok')) - interval '6 hours'
                                                end) at time zone 'utc' as start_time
                                            )
                                            select
                                                (extract(epoch from (coalesce(ct.at_completed_timestamp, current_timestamp at time zone 'asia/bangkok') - ct.at_process_timestamp))) / 3600
                                                as total_hour,
                                                (ct.actual_barrel_weight) as actual_weight
                                                from core_tumbler ct, shift_start ss
                                                where
                                                ct.plant_id = '{0}'
                                                and ct.factory = '{1}'
                                                and ct.line = '{2}'
                                                and ct.material_description = '{3}'
                                                and lower(ct.status) = 'completed'
                                                and ct.at_completed_timestamp >= ss.start_time
                                                order by (ct.at_completed_timestamp) desc limit 1
                                            ;
                            '''.format(plant_id, factory, line, sku)
                            cursor.execute(qs_last_bin)
                            last_bin_result = cursor.fetchall()
                            if len(last_bin_result) > 0:
                                last_bin_hour = last_bin_result[0][0]
                                last_bin_weight = last_bin_result[0][1]
                                print('hour: ', last_bin_hour, ' weight: ', last_bin_weight)
                                last_bin_feed_rate = float(last_bin_weight) / float(last_bin_hour)
                            else:
                                last_bin_feed_rate = 0

                            # find target po
                            qs_target = """
                                        select 
                                            sum(production_quantity) as target_kg
                                        from sap_outbound 
                                        where 
                                            plant_id = '{0}'
                                            and factory = '{1}'
                                            and work_center = '{2}'
                                            and schedule_start_date = '{3}'
                                            and shift = '{4}'
                                            and material_description like '%{5}:%'
                                            and status_process_order not like '%TECO %'
                            """.format(plant_id, factory, line, factory_date, shift, sku)
                            cursor.execute(qs_target)
                            target_result = cursor.fetchall()
                            if len(target_result) > 0:
                                target_po = target_result[0][0]
                                # update target_po
                                qs_update_std_feed_rate = """
                                                        update line_monitor
                                                        set production_target = %s
                                                        where check_on = %s
                                """
                                cursor.execute(qs_update_std_feed_rate, (target_po, check_on))
                                connection.commit()
                                print('updated target po')

                                # update current__sku_matched
                                current_sku_matched = 'YES'
                                sql_insert_query = '''INSERT INTO misc(
                                                                key, value
                                                            )
                                                            
                                                            VALUES (%s, %s)
                                                            ON CONFLICT (key) DO UPDATE
                                                            SET value = EXCLUDED.value
                                                    '''
                                cursor.execute(sql_insert_query, (plant_id + factory + line + 'ViaTumbler' + 'current_sku_matched', current_sku_matched))
                                connection.commit()
                                print('updated current_sku_matched')
                                
                                if target_po is None:
                                    target_po = 'NA'

                                # actual weight in percent
                                if target_result[0][0] is not None:
                                    print('po: ', target_result[0][0])
                                    percent_actual = round(actual_weight_kg / float(target_result[0][0]) * 100)
                                    if percent_actual > 100:
                                        percent_actual = 100
                                else:
                                    percent_actual = 0
                                
                                
                                # actual weight remaining in percent
                                percent_remaining = 100 - percent_actual

                                # find forecast finish time
                                if len(target_result) > 0 and len(result_master_std) == 1 and performance_std is not None:
                                    print('target result: ', target_result[0][0], ' performance std: ', performance_std)
                                    if target_result[0][0] is not None:
                                        print('target result: ', target_result[0][0], ' performance std: ', performance_std)
                                        finish_time = float(target_result[0][0]) / float(performance_std)
                                        hour = int(finish_time)
                                        minute = int((finish_time - hour) * 60)
                                        forecast_finish_datetime = start_datetime + timedelta(hours=hour) + timedelta(minutes=minute)
                                    else:
                                        forecast_finish_datetime = 'NA'
                                else:
                                    forecast_finish_datetime = 'NA'
                                
                                # find target now
                                if len(result_master_std) == 1 and len(result_prod_time) > 0 and len(target_result) > 0:
                                    if target_result[0][0] is not None:
                                        current_bangkok_time = datetime.now(timezone.utc) + timedelta(hours=7)
                                        time_diff_in_hr = (current_bangkok_time - start_datetime).total_seconds() / 3600
                                        target_now_kg = float(performance_std) * time_diff_in_hr
                                        percent_target_now = round(target_now_kg / float(target_result[0][0])  * 100, 0)
                                    else:
                                        target_now_kg = 'NA'
                                        percent_target_now = 'NA'
                                else:
                                    target_now_kg = 'NA'
                                    percent_target_now = 'NA'

                                # forecast kg at the end of the shift
                                if shift == 'D':
                                    planned_end_time = factory_date + ' 18:00:00'
                                else:
                                    planned_end_time = (datetime.strptime(factory_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d') + ' 06:00:00'
                                
                                if forecast_finish_datetime != 'NA':
                                    print('forecast finish datetime: ', forecast_finish_datetime)
                                    print('planned end time: ', datetime.strptime(planned_end_time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc))
                                    if forecast_finish_datetime > datetime.strptime(planned_end_time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc):
                                        planned_end_time = planned_end_time
                                    else:
                                        planned_end_time = forecast_finish_datetime.strftime('%Y-%m-%d %H:%M:%S')
                                
                                remaining_time = (datetime.strptime(planned_end_time, '%Y-%m-%d %H:%M:%S') - (datetime.now() + timedelta(hours=7))).total_seconds()/3600
                                remaining_time_second = (datetime.strptime(planned_end_time, '%Y-%m-%d %H:%M:%S') - (datetime.now() + timedelta(hours=7))).total_seconds()
                                print('planned end time: ', planned_end_time)
                                print('now: ', datetime.now() + timedelta(hours=7))
                                print('remaining end time: ', remaining_time) 
                                forecast_finished_kg = round(actual_feed_rate * remaining_time + actual_weight_kg, 0)

                            else:
                                current_sku_matched = 'NO'
                                sql_insert_query = '''INSERT INTO misc(
                                                                key, value
                                                            )
                                                            
                                                            VALUES (%s, %s)
                                                            ON CONFLICT (key) DO UPDATE
                                                            SET value = EXCLUDED.value
                                                    '''
                                cursor.execute(sql_insert_query, (plant_id + factory + line + 'ViaTumbler' + 'current_sku_matched', current_sku_matched))
                                connection.commit()
                                print('update current_sku_matched')
                                percent_actual = 'NA'
                                percent_remaining = 'NA'
                                target_po = 'NA'

                                # find forecast finish time
                                if len(target_result) > 0 and len(result_master_std) == 1 and performance_std is not None:
                                    print('target result: ', target_result[0][0], ' performance std: ', performance_std)
                                    if target_result[0][0] is not None:
                                        print('target result: ', target_result[0][0], ' performance std: ', performance_std)
                                        finish_time = float(target_result[0][0]) / float(performance_std)
                                        hour = int(finish_time)
                                        minute = int((finish_time - hour) * 60)
                                        forecast_finish_datetime = start_datetime + timedelta(hours=hour) + timedelta(minutes=minute)
                                    else:
                                        forecast_finish_datetime = 'NA'
                                else:
                                    forecast_finish_datetime = 'NA'
                                
                                # find target now
                                if len(result_master_std) == 1 and len(result_prod_time) > 0 and len(target_result) > 0:
                                    if target_result[0][0] is not None:
                                        current_bangkok_time = datetime.now(timezone.utc) + timedelta(hours=7)
                                        time_diff_in_hr = (current_bangkok_time - start_datetime).total_seconds() / 3600
                                        target_now_kg = float(performance_std) * time_diff_in_hr
                                        percent_target_now = round(target_now_kg / float(target_result[0][0])  * 100, 0)
                                    else:
                                        target_now_kg = 'NA'
                                        percent_target_now = 'NA'
                                else:
                                    target_now_kg = 'NA'
                                    percent_target_now = 'NA'

                                # forecast kg at the end of the shift
                                if shift == 'D':
                                    planned_end_time = factory_date + ' 18:00:00'
                                else:
                                    planned_end_time = (datetime.strptime(factory_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d') + ' 06:00:00'
                                
                                if forecast_finish_datetime != 'NA':
                                    print('forecast finish datetime: ', forecast_finish_datetime)
                                    print('planned end time: ', datetime.strptime(planned_end_time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc))
                                    if forecast_finish_datetime > datetime.strptime(planned_end_time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc):
                                        planned_end_time = planned_end_time
                                    else:
                                        planned_end_time = forecast_finish_datetime.strftime('%Y-%m-%d %H:%M:%S')
                                remaining_time = (datetime.strptime(planned_end_time, '%Y-%m-%d %H:%M:%S') - (datetime.now() + timedelta(hours=7))).total_seconds()/3600
                                forecast_finished_kg = round(actual_feed_rate * remaining_time + actual_weight_kg, 0)
                                remaining_time_second = (datetime.strptime(planned_end_time, '%Y-%m-%d %H:%M:%S') - (datetime.now() + timedelta(hours=7))).total_seconds()
                                print('planned end time: ', planned_end_time)
                                print('now: ', datetime.now() + timedelta(hours=7))
                                print('remaining end time: ', remaining_time) 
                            
                            # update firestore (factory_date, shift, sku, production_unit, actual_weight, start_datetime)
                            doc_id = plant_id + factory + line + 'ViaTumbler' + 'Performance'
                            if target_po != 'NA':
                                print('Target is not NA')
                                data = {
                                    "REMAINING_WEIGHT": float(target_po) -  float(actual_weight_kg) if target_po != 'NA' else 'NA',
                                    "TARGET_WEIGHT_KG": float(target_po) if target_po != 'NA' else 'NA',
                                    "PERCENT_ACTUAL_WEIGHT": float(percent_actual) if percent_actual != 'NA' else 'NA',
                                    "PERCENT_REMAINING_WEIGHT": float(percent_remaining) if percent_remaining != 'NA' else 'NA',
                                    "FORECAST_FINISH_DATETIME": forecast_finish_datetime.strftime('%Y-%m-%d %H:%M:%S') if forecast_finish_datetime != 'NA' else 'NA' ,
                                    "TARGET_NOW_KG": float(target_now_kg) if target_now_kg != 'NA' else 'NA',
                                    "PERCENT_TARGET_NOW": float(percent_target_now) if percent_target_now != 'NA' else 'NA',
                                    "FORECAST_IN_KG": float(forecast_finished_kg) if forecast_finished_kg != 'NA' else 'NA',
                                    "FORECAST": forecast_finished_kg - target_po if target_po != 'NA' else 'NA',
                                    "LATEST_UPDATE": (datetime.now() + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S')
                                }
                                print(data)
                                # Get document object
                                doc = db.collection('COLLECTION_PERFORMANCE_P12_V2').document(doc_id).get()
                                if doc.exists:
                                    db.collection('COLLECTION_PERFORMANCE_P12_V2').document(doc_id).update(data)
                                else:
                                    db.collection('COLLECTION_PERFORMANCE_P12_V2').document(doc_id).set(data)
                            else:
                                print('Target is NA')
                                target_po = db.collection('COLLECTION_PERFORMANCE_P12_V2').document(doc_id).get().to_dict()['TARGET_WEIGHT_KG']
                                if target_po != 'NA':
                                    percent_actual = round(actual_weight_kg / float(target_po) * 100)
                                    if percent_actual > 100:
                                        percent_actual = 100
                                else:
                                    percent_actual = 0
                                
                                
                                # actual weight remaining in percent
                                percent_remaining = 100 - percent_actual

                                qs_std = """
                                        select multihead_auto_pack, belt_scale_feed_rate, multihead_target_pack
                                        from core_master_std 
                                        where plant_id = '{0}' and factory = '{1}' and line = '{2}' and
                                        material_description = '{3}'
                                        order by updated_at desc limit 1
                                """
                                cursor.execute(qs_std.format(plant_id, factory, line, sku))
                                result_master_std = cursor.fetchall()

                                # find forecast finish time
                                if target_po != 'NA' and len(result_master_std) == 1:
                                    print('target: ', target_po, ' performance std: ', performance_std)
                                    if target_po is not None:
                                        print('target: ', target_po, ' performance std: ', performance_std)
                                        finish_time = float(target_po) / float(performance_std)
                                        hour = int(finish_time)
                                        minute = int((finish_time - hour) * 60)
                                        forecast_finish_datetime = start_datetime + timedelta(hours=hour) + timedelta(minutes=minute)
                                    else:
                                        forecast_finish_datetime = 'NA'
                                else:
                                    forecast_finish_datetime = 'NA'
                                
                                # find target now
                                if len(result_master_std) == 1 and len(result_prod_time) > 0 and len(target_result) > 0:
                                    if target_po is not None:
                                        current_bangkok_time = datetime.now(timezone.utc) + timedelta(hours=7)
                                        time_diff_in_hr = (current_bangkok_time - start_datetime).total_seconds() / 3600
                                        target_now_kg = float(performance_std) * time_diff_in_hr
                                        percent_target_now = round(target_now_kg / float(target_po)  * 100, 0)
                                    else:
                                        target_now_kg = 'NA'
                                        percent_target_now = 'NA'
                                else:
                                    target_now_kg = 'NA'
                                    percent_target_now = 'NA'

                                # forecast kg at the end of the shift
                                if shift == 'D':
                                    planned_end_time = factory_date + ' 18:00:00'
                                else:
                                    planned_end_time = (datetime.strptime(factory_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d') + ' 06:00:00'
                                
                                if forecast_finish_datetime != 'NA':
                                    print('forecast finish datetime: ', forecast_finish_datetime)
                                    print('planned end time: ', datetime.strptime(planned_end_time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc))
                                    if forecast_finish_datetime > datetime.strptime(planned_end_time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc):
                                        planned_end_time = planned_end_time
                                    else:
                                        planned_end_time = forecast_finish_datetime.strftime('%Y-%m-%d %H:%M:%S')
                                        
                                remaining_time = (datetime.strptime(planned_end_time, '%Y-%m-%d %H:%M:%S') - (datetime.now() + timedelta(hours=7))).total_seconds()/3600
                                forecast_finished_kg = round(actual_feed_rate * remaining_time + actual_weight_kg, 0)
                                remaining_time_second = (datetime.strptime(planned_end_time, '%Y-%m-%d %H:%M:%S') - (datetime.now() + timedelta(hours=7))).total_seconds()
                                print('planned end time: ', planned_end_time)
                                print('now: ', datetime.now() + timedelta(hours=7))
                                print('remaining end time: ', remaining_time) 
                                
                                data = {
                                    "REMAINING_WEIGHT": float(target_po) -  float(actual_weight_kg) if target_po != 'NA' else 'NA',
                                    "TARGET_WEIGHT_KG": float(target_po) if target_po != 'NA' else 'NA',
                                    "PERCENT_ACTUAL_WEIGHT": float(percent_actual) if percent_actual != 'NA' else 'NA',
                                    "PERCENT_REMAINING_WEIGHT": float(percent_remaining) if percent_remaining != 'NA' else 'NA',
                                    "FORECAST_FINISH_DATETIME": forecast_finish_datetime.strftime('%Y-%m-%d %H:%M:%S') if forecast_finish_datetime != 'NA' else 'NA' ,
                                    "TARGET_NOW_KG": float(target_now_kg) if target_now_kg != 'NA' else 'NA',
                                    "PERCENT_TARGET_NOW": float(percent_target_now) if percent_target_now != 'NA' else 'NA',
                                    "FORECAST_IN_KG": float(forecast_finished_kg) if forecast_finished_kg != 'NA' else 'NA',
                                    "FORECAST": forecast_finished_kg - target_po if target_po != 'NA' else 'NA',
                                    "LATEST_UPDATE": (datetime.now() + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S')
                                }
                                print(data)
                                # Get document object
                                doc = db.collection('COLLECTION_PERFORMANCE_P12_V2').document(doc_id).get()
                                if doc.exists:
                                    db.collection('COLLECTION_PERFORMANCE_P12_V2').document(doc_id).update(data)
                                else:
                                    db.collection('COLLECTION_PERFORMANCE_P12_V2').document(doc_id).set(data)

                            data = {
                                "PLANT_ID": plant_id,
                                "FACTORY_CODE": factory,
                                "FACTORY_DATE": current_factory_date,
                                "LINE": line,
                                "SHIFT": shift,
                                "SKU": sku,
                                "ACTUAL_WEIGHT": float(actual_weight_kg),
                                "ACTUAL_FEED_RATE": float(actual_feed_rate) if actual_feed_rate != 'NA' else 'NA',
                                "ACTUAL_FEED_RATE_LAST_BIN": last_bin_feed_rate,
                                "STD_FEED_RATE": float(performance_std) if performance_std != 'NA' else 'NA',
                                "START_DATETIME": start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                                "FINISH_DATETIME": 'NA',
                                "PRODUCTION_TIME": str(prod_time_str).zfill(8),
                                "NON_PRODUCTION_TIME": str(non_prod_time_str).zfill(8),
                                "FORECAST_IN_KG": float(forecast_finished_kg) if forecast_finished_kg != 'NA' else 'NA',
                                "IS_SKU_MATCHED": current_sku_matched,
                                "IS_RUNNING": is_running,
                                "LATEST_UPDATE": (datetime.now() + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S')
                            }
                            print(data)
                            # Get document object
                            doc = db.collection('COLLECTION_PERFORMANCE_P12_V2').document(doc_id).get()
                            if doc.exists:
                                db.collection('COLLECTION_PERFORMANCE_P12_V2').document(doc_id).update(data)
                            else:
                                db.collection('COLLECTION_PERFORMANCE_P12_V2').document(doc_id).set(data)
    
                else:
                    doc_id = plant_id + factory + line + 'ViaTumbler' + 'Performance'
                    data = {
                        "PLANT_ID": plant_id,
                        "FACTORY_CODE": factory,
                        "LINE": line,
                        "FACTORY_DATE": '-',
                        "SHIFT": '-',
                        "SKU": '-',
                        "ACTUAL_WEIGHT": '-',
                        "REMAINING_WEIGHT": '-',
                        "TARGET_WEIGHT_KG": '-',
                        "PERCENT_ACTUAL_WEIGHT": '-',
                        "PERCENT_REMAINING_WEIGHT": '-',
                        "ACTUAL_FEED_RATE": '-',
                        "ACTUAL_FEED_RATE_LAST_BIN": '-',
                        "STD_FEED_RATE": '-',
                        "START_DATETIME": '-',
                        "FORECAST_FINISH_DATETIME": '-',
                        "FINISH_DATETIME": 'NA',
                        "PRODUCTION_TIME": '-',
                        "NON_PRODUCTION_TIME": '-',
                        "TARGET_NOW_KG": '-',
                        "PERCENT_TARGET_NOW": '-',
                        "FORECAST_IN_KG": '-',
                        "FORECAST": '-',
                        "IS_SKU_MATCHED": '-',
                        "IS_RUNNING": 'No',
                        "LATEST_UPDATE": '-',
                        "STATUS": "Not Start"
                    }

                    # Get document object
                    doc = db.collection('COLLECTION_PERFORMANCE_P12_V2').document(doc_id).get()
                    if doc.exists:
                        db.collection('COLLECTION_PERFORMANCE_P12_V2').document(doc_id).update(data)
                    else:
                        db.collection('COLLECTION_PERFORMANCE_P12_V2').document(doc_id).set(data)

    except Exception as ex:
        print(ex)
    finally:
        if connection:
            connection.close()
            print('Postgresql connection is closed.')    
