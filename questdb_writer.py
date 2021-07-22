from datetime import datetime
import socket

def get_epoch(datetimestring):
    """
    (Badly) get Nanoseconds from the timestamp. We dont need that kind of accuracy (yet anyway)
    so its fine for now but QuestDB requires this in nano seconds (1e9)
    for Python 3.7+ time_ns should be used if you require Nanosecond accuracy
    :param datetimestring:
    :return:
    """
    dt = datetimestring.strftime('%Y-%m-%d %H:%M:%S.%f')
    dt_obj = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S.%f')
    # replace this with time_ns in Python 3.7
    nanoseconds = int(dt_obj.timestamp() * 1e9)

    return nanoseconds


def _row_to_line_protocol(row, measurement, table, timestamp_field):
    """
    Transform a DF row into line protocol
    :param row: The Data Row
    :param measurement: The QuestDB measurement to write
    :param table: The QuestDB Table to write to
    :param timestamp_field: The field used as timestamp (in Nanoseconds)
    :return:
    """

    # convert timestamp to nanos and remove from main array
    timestamp = get_epoch(row[timestamp_field])

    # double quote all date fields
    for x, y in row.items():
        if isinstance(y, datetime):
            new_dt = y.strftime('%Y-%m-%dT%H:%M:%S.%f')
            row.update({x: "\"" + new_dt + "\""})

    measurement_value = measurement + '=' + row[measurement]

    # delete measurements from rowset
    row = {k: v for k, v in row.items() if k not in measurement}

    rows_kv = ','.join(f'{k}={v}' for k, v in row.items())

    metrics = f"{table},{measurement_value} {rows_kv} {timestamp}"

    return metrics


def write_to_quest(df, measurement, table, timestamp_field, args):
    """
    Open a socket and write the row directly into Quest
    :param df: The Pyspark / AWS Glue DF
    :param measurement: The measurement for QuestDB
    :param table: The QuestDB Table to write to
    :param timestamp_field: he field used as timestamp (in Nanoseconds)
    :param args: AWS Glue Job Args
    :return:
    """
    HOST = args['questdb_host']
    PORT = int(args['questdb_port'])
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        sock.connect((HOST, PORT))
        rows = df.rdd.map(lambda row: row.asDict(True))
        new_rdd = rows.map(lambda row:
                            _row_to_line_protocol(row, measurement, table, timestamp_field))
        result = new_rdd.map(lambda r: "".join(r) + "\n") \
            .aggregate("", lambda a, b: a + b, lambda a, b: a + b)
        sock.sendall((result.encode()))
        
    except socket.error as e:
        print("Got error: %s" % (e))

    sock.close()

