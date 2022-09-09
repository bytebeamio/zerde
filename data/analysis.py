import csv
import matplotlib.pyplot as plt

plt.rcdefaults()
fig, ax = plt.subplots()

def get_details(file_name):
    with open(file_name) as f:
        csvreader = csv.reader(f)
        idx = [1, 3, 6, 9, 12, 16, 18, 21, 24, 27, 31, 33, 36, 39, 42, 46, 48, 51, 54, 57, 61, 63, 66, 69, 72, 76, 78, 81, 84, 87]
        next(csvreader)
        first = next(csvreader)
        count = 1
        byte_sizes = list(map(lambda x : int(first[x]), idx))
        for row in csvreader:
            count += 1
            row = list(map(lambda x : int(row[x]), idx))
            for i in range(len(row)):
                byte_sizes[i] += row[i]
    
        return (count, byte_sizes)

headers = ["json", "json_lz4", "json_snappy", "json_zlib", "json_zstd", "protobuf", "protobuf_lz4", "protobuf_snappy", "protobuf_zlib", "protobuf_zstd", "msgpack", "msgpack_lz4", "msgpack_snappy", "msgpack_zlib", "msgpack_zstd", "bson", "bson_lz4", "bson_snappy", "bson_zlib", "bson_zstd", "cbor", "cbor_lz4", "cbor_snappy", "cbor_zlib", "cbor_zstd", "pickle", "pickle_lz4", "pickle_snappy", "pickle_zlib", "pickle_zstd"]
print("batch_size, data_type, row_count,", ", ".join(headers))
for batch_size in [1, 10, 100, 1000]:
    for data_type in ["bms", "gps", "imu", "motor", "peripherals", "shadow"]:
        file_name = "{}_{}".format(batch_size, data_type) 
        (count, row) = get_details(file_name + ".csv")
        print(batch_size, ", ", data_type,", ", count, ", ", ", ".join(list(map(str, row))), sep="")
        ax.barh(range(30), row, align='center')
        ax.set_yticks(range(30), labels=headers)
        ax.invert_yaxis()
        ax.set_xlabel('Bytes')
        ax.set_title('Byte size from serialization and compression')
        plt.savefig(file_name + ".png")


