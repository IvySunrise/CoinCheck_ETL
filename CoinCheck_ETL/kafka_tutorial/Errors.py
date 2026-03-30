from kafka_tutorial.kafka_functions import public_get

def producer_get_error(url, params=None):
    try:
        data_fetched = public_get(url, params)
    except Exception as e:
        print("Error: ", e, "fetching data")
        data_fetched = None
    return data_fetched

def producer_send_error(producer, topic, data_fetched):
    try:
        if data_fetched and "data" in data_fetched:
            for trade in data_fetched["data"]:
                producer.send(topic, trade)  # send each trade individually
            producer.flush()
            print(f"{len(data_fetched['data'])} trades sent to {topic}")
        else:
            print("No trades to send")
    except Exception as e:
        print("Error:", e, "sending data")

def import_error(method, import_file= None):
    try:
        if import_file == None:
            import method
        else:
            from import_file import method
    except Exception as e:
        print("Error: ", e, "importing files")
