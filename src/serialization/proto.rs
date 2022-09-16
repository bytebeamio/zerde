use prost::Message;

use crate::base::Payload;

use self::test::{Bms, BmsList, Gps, GpsList, Imu, ImuList, Motor, MotorList};

use super::Error;

mod test {
    include!(concat!(env!("OUT_DIR"), "/test.rs"));
}

pub fn serialize(payload: Vec<Payload>, stream: &str) -> Result<Vec<u8>, Error> {
    let mut buf = vec![];

    match stream {
        "test.gpsList" => {
            let messages: Vec<Gps> = payload
                .iter()
                .map(|p| Gps {
                    longitude: p.payload.get("longitude").unwrap().as_f64().unwrap(),
                    latitude: p.payload.get("longitude").unwrap().as_f64().unwrap(),
                    timestamp: p.timestamp,
                    sequence: p.sequence as i32,
                })
                .collect();
            let list = GpsList { messages };
            list.encode(&mut buf)?;
        }
        "test.imuList" => {
            let messages: Vec<Imu> = payload
                .iter()
                .map(|p| Imu {
                    timestamp: p.timestamp,
                    sequence: p.sequence,
                    ax: p.payload.get("ax").unwrap().as_f64().unwrap(),
                    ay: p.payload.get("ay").unwrap().as_f64().unwrap(),
                    az: p.payload.get("az").unwrap().as_f64().unwrap(),
                    pitch: p.payload.get("pitch").unwrap().as_f64().unwrap(),
                    roll: p.payload.get("roll").unwrap().as_f64().unwrap(),
                    yaw: p.payload.get("yaw").unwrap().as_f64().unwrap(),
                    magx: p.payload.get("magx").unwrap().as_f64().unwrap(),
                    magy: p.payload.get("magy").unwrap().as_f64().unwrap(),
                    magz: p.payload.get("magz").unwrap().as_f64().unwrap(),
                })
                .collect();
            let list = ImuList { messages };
            list.encode(&mut buf)?;
        }
        "test.motorList" => {
            let messages: Vec<Motor> = payload
                .iter()
                .map(|p| Motor {
                    timestamp: p.timestamp,
                    sequence: p.sequence,
                    temperature1: p.payload.get("temperature1").unwrap().as_f64().unwrap(),
                    temperature2: p.payload.get("temperature2").unwrap().as_f64().unwrap(),
                    temperature3: p.payload.get("temperature3").unwrap().as_f64().unwrap(),
                    voltage: p.payload.get("voltage").unwrap().as_f64().unwrap(),
                    current: p.payload.get("current").unwrap().as_f64().unwrap(),
                    rpm: p.payload.get("rpm").unwrap().as_u64().unwrap() as u32,
                })
                .collect();
            let list = MotorList { messages };
            list.encode(&mut buf)?;
        }
        "test.bmsList" => {
            let messages: Vec<Bms> = payload
                .iter()
                .map(|p| Bms {
                    timestamp: p.timestamp,
                    sequence: p.sequence as i32,
                    periodicity_ms: p.payload.get("periodicity_ms").unwrap().as_i64().unwrap()
                        as i32,
                    mosfet_temperature: p
                        .payload
                        .get("mosfet_temperature")
                        .unwrap()
                        .as_f64()
                        .unwrap(),
                    ambient_temperature: p
                        .payload
                        .get("ambient_temperature")
                        .unwrap()
                        .as_f64()
                        .unwrap(),
                    mosfet_status: p.payload.get("mosfet_status").unwrap().as_i64().unwrap() as i32,
                    cell_voltage_count: p
                        .payload
                        .get("cell_voltage_count")
                        .unwrap()
                        .as_i64()
                        .unwrap() as i32,
                    cell_voltage_1: p.payload.get("cell_voltage_1").unwrap().as_f64().unwrap(),
                    cell_voltage_2: p.payload.get("cell_voltage_2").unwrap().as_f64().unwrap(),
                    cell_voltage_3: p.payload.get("cell_voltage_3").unwrap().as_f64().unwrap(),
                    cell_voltage_4: p.payload.get("cell_voltage_4").unwrap().as_f64().unwrap(),
                    cell_voltage_5: p.payload.get("cell_voltage_5").unwrap().as_f64().unwrap(),
                    cell_voltage_6: p.payload.get("cell_voltage_6").unwrap().as_f64().unwrap(),
                    cell_voltage_7: p.payload.get("cell_voltage_7").unwrap().as_f64().unwrap(),
                    cell_voltage_8: p.payload.get("cell_voltage_8").unwrap().as_f64().unwrap(),
                    cell_voltage_9: p.payload.get("cell_voltage_9").unwrap().as_f64().unwrap(),
                    cell_voltage_10: p.payload.get("cell_voltage_10").unwrap().as_f64().unwrap(),
                    cell_voltage_11: p.payload.get("cell_voltage_11").unwrap().as_f64().unwrap(),
                    cell_voltage_12: p.payload.get("cell_voltage_12").unwrap().as_f64().unwrap(),
                    cell_voltage_13: p.payload.get("cell_voltage_13").unwrap().as_f64().unwrap(),
                    cell_voltage_14: p.payload.get("cell_voltage_14").unwrap().as_f64().unwrap(),
                    cell_voltage_15: p.payload.get("cell_voltage_15").unwrap().as_f64().unwrap(),
                    cell_voltage_16: p.payload.get("cell_voltage_16").unwrap().as_f64().unwrap(),
                    cell_thermistor_count: p
                        .payload
                        .get("cell_thermistor_count")
                        .unwrap()
                        .as_i64()
                        .unwrap() as i32,
                    cell_temp_1: p.payload.get("cell_temp_1").unwrap().as_f64().unwrap(),
                    cell_temp_2: p.payload.get("cell_temp_2").unwrap().as_f64().unwrap(),
                    cell_temp_3: p.payload.get("cell_temp_3").unwrap().as_f64().unwrap(),
                    cell_temp_4: p.payload.get("cell_temp_4").unwrap().as_f64().unwrap(),
                    cell_temp_5: p.payload.get("cell_temp_5").unwrap().as_f64().unwrap(),
                    cell_temp_6: p.payload.get("cell_temp_6").unwrap().as_f64().unwrap(),
                    cell_temp_7: p.payload.get("cell_temp_7").unwrap().as_f64().unwrap(),
                    cell_temp_8: p.payload.get("cell_temp_8").unwrap().as_f64().unwrap(),
                    cell_balancing_status: p
                        .payload
                        .get("cell_balancing_status")
                        .unwrap()
                        .as_i64()
                        .unwrap() as i32,
                    pack_voltage: p.payload.get("pack_voltage").unwrap().as_f64().unwrap(),
                    pack_current: p.payload.get("pack_current").unwrap().as_f64().unwrap(),
                    pack_soc: p.payload.get("pack_soc").unwrap().as_f64().unwrap(),
                    pack_soh: p.payload.get("pack_soh").unwrap().as_f64().unwrap(),
                    pack_sop: p.payload.get("pack_sop").unwrap().as_f64().unwrap(),
                    pack_cycle_count: p.payload.get("pack_cycle_count").unwrap().as_i64().unwrap(),
                    pack_available_energy: p
                        .payload
                        .get("pack_available_energy")
                        .unwrap()
                        .as_i64()
                        .unwrap(),
                    pack_consumed_energy: p
                        .payload
                        .get("pack_consumed_energy")
                        .unwrap()
                        .as_i64()
                        .unwrap(),
                    pack_fault: p.payload.get("pack_fault").unwrap().as_i64().unwrap() as i32,
                    pack_status: p.payload.get("pack_status").unwrap().as_i64().unwrap() as i32,
                })
                .collect();
            let list = BmsList { messages };
            list.encode(&mut buf)?;
        }
        _ => {
            panic!("Couldn't serialize for stream: {}!", stream)
        }
    }

    Ok(buf)
}

pub fn deserialize(payload: &[u8], stream: &str) -> Result<Vec<Payload>, Error> {
    match stream {
        "test.gpsList" => {
            let list: GpsList = Message::decode(payload)?;
            let payload = list
            .messages
            .iter()
            .map(|g| Payload {
                sequence: g.sequence as u32,
                timestamp: g.timestamp,
                payload: serde_json::json! ({ "longitude": g.longitude, "latitude": g.latitude }),
                ..Default::default()
            })
            .collect();
            Ok(payload)
        }
        "test.imuList" => {
            let list: ImuList = Message::decode(payload)?;
            let payload = list
            .messages
            .iter()
            .map(|i| Payload {
                sequence: i.sequence as u32,
                timestamp: i.timestamp,
                payload: serde_json::json! ({ "ax": i.ax, "ay": i.ay, "az": i.az, "roll": i.roll, "pitch": i.pitch, "yaw": i.yaw, "magx": i.magx, "magy": i.magy, "magz": i.magz }),
                ..Default::default()
            })
            .collect();
            Ok(payload)
        }
        "test.motorList" => {
            let list: MotorList = Message::decode(payload)?;
            let payload = list
            .messages
            .iter()
            .map(|m| Payload {
                sequence: m.sequence as u32,
                timestamp: m.timestamp,
                payload: serde_json::json! ({ "temperature1": m.temperature1, "temperature2": m.temperature2, "temperature3": m.temperature3, "rpm": m.rpm, "voltage": m.voltage, "current": m.current }),
                ..Default::default()
            })
            .collect();
            Ok(payload)
        }
        "test.bmsList" => {
            let list: BmsList = Message::decode(payload)?;
            let payload = list
                .messages
                .iter()
                .map(|b| Payload {
                    sequence: b.sequence as u32,
                    timestamp: b.timestamp,
                    payload: serde_json::json! ({ "periodicity_ms": b.periodicity_ms, "mosfet_temperature": b.mosfet_temperature, "ambient_temperature": b.ambient_temperature, "mosfet_status": b.mosfet_status, "cell_voltage_count": b.cell_voltage_count }),
                    ..Default::default()
                })
                .collect();
            Ok(payload)
        }
        _ => {
            panic!("Couldn't deserialize for stream: {}!", stream)
        }
    }
}
