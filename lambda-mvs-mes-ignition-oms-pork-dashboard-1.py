def clean_string(value: str):
    if not isinstance(value, str):
        return value
    value = value.replace("\xa0", " ").replace("\u200b", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def clean_dict(data):
    if isinstance(data, dict):
        return {k: clean_dict(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [clean_dict(v) for v in data]
    elif isinstance(data, str):
        return clean_string(data)
    return data

def get_factory_date_dynamic(shift_start_str: str, now=None, tz="Asia/Bangkok"):
    tzinfo = pytz.timezone(tz)
    now = now or datetime.now(tzinfo)

    hh, mm = shift_start_str.split(":")
    shift_start = time(int(hh), int(mm))

    if now.time() < shift_start:
        factory_date = (now - timedelta(days=1)).date()
    else:
        factory_date = now.date()

    return factory_date


def lambda_handler(event, context):
    org_code = event['pathParameters']['org_code']
    program_code = event['pathParameters']['program_code']
    auth_body = {
        "grant_type": GRANT_TYPE,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }

    # set default
    daily_qty = 700
    actual_percent = 0
    factory_date = ''
    shift = ''
    auth_response = requests.post(f'{HOSTNAME}{TOKEN_PATH}', data=auth_body, timeout=5)
    stock_lot = {}
    daily_plan = {}
    if auth_response.status_code == 200:
        token_type = auth_response.json()['token_type']
        access_token = auth_response.json()['access_token']
        config_response = requests.get(f'{HOSTNAME}{CONFIG_PATH}OrgCode={org_code}&ProgramCode={program_code}', 
            headers={
            'Authorization': f'{token_type} {access_token}'
            },
            timeout=5
        )
        if config_response.status_code == 200:
            value = config_response.json()["programCode"]
            if value is not None:
                # stock lot
                keys_to_include = ['docTypeI', 'locationCode', 'productCodes', 'unitTransactionData', 'unitLotDisplay']
                filtered = {
                    key[0].upper() + key[1:]: config_response.json()[key]
                    for key in keys_to_include
                    if key in config_response.json()
                }

                stock_lot_query = urlencode(filtered, doseq=True)
                stock_lot_params = f'OrgCode={org_code}&{stock_lot_query}'
                # print(stock_lot_params)
                stock_lot_response = requests.get(f'{HOSTNAME}{STOCK_LOT_PATH}{stock_lot_params}', 
                    headers={
                    'Authorization': f'{token_type} {access_token}'
                    },
                    timeout=5
                )
                # print(stock_lot_response.status_code)
                if stock_lot_response.status_code == 200:
                    stock_lot = stock_lot_response.json()
                
                # daily plan
                keys_to_include = ['docTypeI', 'docTypeR', 'locationCode', 'productCodes', 'unitTransactionData', 'unitStockDisplay', 'docTypeIProductGroup2', 'docTypeRProductgroup2', 'resetDataTime']
                filtered = {
                    key[0].upper() + key[1:]: config_response.json()[key]
                    for key in keys_to_include
                    if key in config_response.json()
                }

                daily_plan_query = urlencode(filtered, doseq=True)
                daily_plan_params = f'OrgCode={org_code}&{daily_plan_query}'
                daily_plan_response = requests.get(f'{HOSTNAME}{DAILY_PLAN_PATH}{daily_plan_params}', 
                    headers={
                    'Authorization': f'{token_type} {access_token}'
                    },
                    timeout=5
                )
                # print(daily_plan_response.status_code)
                if daily_plan_response.status_code == 200:
                    daily_plan = daily_plan_response.json()
                    for part_name, part_data in daily_plan["productGroupYield"]["productionLine"].items():
                        for item in part_data["items"]:
                            std_yield = item["stdYield"]
                            act_yield = item["actYield"]

                            # target = TARGET * stdYield * 120 / 100 / 1000
                            target = daily_qty * std_yield * 120 / 100 / 1000

                            # actual_weight = dailyProductionPlan['Qty'] * actYield * 120 / 100 / 1000
                            actual_weight = daily_qty * act_yield * 120 / 100 / 1000

                            # actual_percent = target / actual_weight
                            actual_percent = target / actual_weight if actual_weight else None

                            # add snake_case keys
                            item["target"] = target
                            item["actualWeight"] = actual_weight
                            item["actualPercent"] = actual_percent
                            actual_percent = daily_plan['dailyProductionPlan']['actualQty']/daily_qty * 100
                    factory_date = get_factory_date_dynamic(daily_plan['dailyProductionPlan']['shiftStartTime'].strip())
                    shift = 'D' if daily_plan['dailyProductionPlan']['shiftStartTime'].strip() == '06:00' else 'N'      
                
                data = {
                    'stockLot': stock_lot,
                    'dailyPlan': daily_plan,
                    'actualPercent': actual_percent,
                    'shift': shift,
                    'factoryDate': factory_date.strftime('%Y-%m-%d')
                }
                
                doc_id = f'{factory_date}.{shift}.{org_code}.{program_code}'
                doc = db.collection('COLLECTION_OMS_PORK_DASHBOARD_1').document(doc_id).get()
                if doc.exists:
                    db.collection('COLLECTION_OMS_PORK_DASHBOARD_1').document(doc_id).update(data)
                else:
                    db.collection('COLLECTION_OMS_PORK_DASHBOARD_1').document(doc_id).set(data)
                
                return {
                    "headers": {
                    "X-Requested-With": '*',
                    "Access-Control-Allow-Headers": '*',
                    "Access-Control-Allow-Origin": '*',
                    "Access-Control-Allow-Methods": 'POST,GET,OPTIONS'
                    },
                    'statusCode': 200,
                    'body': json.dumps(data),
                    "isBase64Encoded": False
                }
            return {
                    "headers": {
                    "X-Requested-With": '*',
                    "Access-Control-Allow-Headers": '*',
                    "Access-Control-Allow-Origin": '*',
                    "Access-Control-Allow-Methods": 'POST,GET,OPTIONS'
                    },
                    'statusCode': 404,
                    'body': 'Not Found',
                    "isBase64Encoded": False
                }
