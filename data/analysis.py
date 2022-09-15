import csv
import matplotlib.pyplot as plt

plt.rcdefaults()
fig, ax = plt.subplots()

def get_details(file_name, idx):
    with open(file_name) as f:
        csvreader = csv.reader(f)
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

# Collate byte-size data
with open("analysis_sizes.csv", "w") as f:
    idx = [1, 3, 6, 9, 12, 16, 18, 21, 24, 27, 31, 33, 36, 39, 42, 46, 48, 51, 54, 57, 61, 63, 66, 69, 72, 76, 78, 81, 84, 87, 91, 93, 96, 99, 102]
    headers = ["json", "json_lz4", "json_snappy", "json_zlib", "json_zstd", "protobuf", "protobuf_lz4", "protobuf_snappy", "protobuf_zlib", "protobuf_zstd", "protoref", "protoref_lz4", "protoref_snappy", "protoref_zlib", "protoref_zstd", "msgpack", "msgpack_lz4", "msgpack_snappy", "msgpack_zlib", "msgpack_zstd", "bson", "bson_lz4", "bson_snappy", "bson_zlib", "bson_zstd", "cbor", "cbor_lz4", "cbor_snappy", "cbor_zlib", "cbor_zstd", "pickle", "pickle_lz4", "pickle_snappy", "pickle_zlib", "pickle_zstd"]
    line = "batch_size, data_type, row_count, " + ", ".join(headers) + "\n"
    f.write(line)
    for batch_size in [1, 10, 100, 1000]:
        for data_type in ["bms", "gps", "imu", "motor"]:
            file_name = "{}_{}".format(batch_size, data_type) 
            (count, row) = get_details(file_name + ".csv", idx)
            line = f"{batch_size}, {data_type}, {count}, " + ", ".join(list(map(str, row))) + "\n"
            f.write(line)
            ax.barh(range(35), row, align='center')
            ax.set_yticks(range(35), labels=headers)
            ax.invert_yaxis()
            ax.set_xlabel('Bytes')
            ax.set_title('Byte size from serialization and compression')
            plt.savefig(file_name + "_sizes.png")

# Collate microsecond-time data
with open("analysis_times.csv", "w") as f:
    idx = [0, 2, 4, 5, 7, 8, 10, 11, 13, 14, 15, 17, 19, 20, 22, 23, 25, 26, 28, 29, 30, 32, 34, 35, 37, 38, 40, 41, 43, 44, 45, 47, 49, 50, 52, 53, 55, 56, 58, 59, 60, 62, 64, 65, 67, 68, 70, 71, 73, 74, 75, 77, 79, 80, 82, 83, 85, 86, 88, 89, 90, 92, 94, 95, 97, 98, 99, 101, 103, 104]
    headers = ["json_ser", "json_lz4#", "json_lz4!", "json_snappy#", "json_snappy!", "json_zlib#", "json_zlib!", "json_zstd#", "json_zstd!", "json_de", "protobuf_ser", "protobuf_lz4#", "protobuf_lz4!", "protobuf_snappy#", "protobuf_snappy!", "protobuf_zlib#", "protobuf_zlib!", "protobuf_zstd#", "protobuf_zstd!", "protobuf_de", "protoref_ser", "protoref_lz4#", "protoref_lz4!", "protoref_snappy#", "protoref_snappy!", "protoref_zlib#", "protoref_zlib!", "protoref_zstd#", "protoref_zstd!", "protoref_de", "msgpack_ser", "msgpack_lz4#", "msgpack_lz4!", "msgpack_snappy#", "msgpack_snappy!", "msgpack_zlib#", "msgpack_zlib!", "msgpack_zstd#", "msgpack_zstd!", "msgpack_de", "bson_ser", "bson_lz4#", "bson_lz4!", "bson_snappy#", "bson_snappy!", "bson_zlib#", "bson_zlib!", "bson_zstd#", "bson_zstd!", "bson_de", "cbor_ser", "cbor_lz4#", "cbor_lz4!", "cbor_snappy#", "cbor_snappy!", "cbor_zlib#", "cbor_zlib!", "cbor_zstd#", "cbor_zstd!", "cbor_de", "pickle_ser", "pickle_lz4#", "pickle_lz4!", "pickle_snappy#", "pickle_snappy!", "pickle_zlib#", "pickle_zlib!", "pickle_zstd#", "pickle_zstd!", "pickle_de"]
    line = "batch_size, data_type, row_count, " + ", ".join(headers) + "\n"
    f.write(line)
    for batch_size in [1, 10, 100, 1000]:
        for data_type in ["bms", "gps", "imu", "motor"]:
            file_name = "{}_{}".format(batch_size, data_type) 
            (count, row) = get_details(file_name + ".csv", idx)
            line = f"{batch_size}, {data_type}, {count}, " + ", ".join(list(map(str, row))) + "\n"
            f.write(line)
            ax.barh(range(70), row, align='center')
            ax.set_yticks(range(70), labels=headers)
            ax.invert_yaxis()
            ax.set_xlabel('Micros')
            ax.set_title('Time from serialization and compression')
            plt.savefig(file_name + "_times.png")
