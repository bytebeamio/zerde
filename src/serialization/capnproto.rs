use capnp::{
    message::{ReaderOptions, TypedBuilder},
    serialize::{read_message, write_message},
};
use serde_json::json;

use crate::{
    base::Payload,
    test_capnp::{bms_list, gps_list, imu_list, motor_list},
};

use super::Error;

pub fn serialize(payload: Vec<Payload>, stream: &str) -> Result<Vec<u8>, Error> {
    let mut buf = vec![];
    match stream {
        "test.gpsList" => {
            let mut message = TypedBuilder::<gps_list::Owned>::new_default();
            let mut gps_list = message.init_root().init_messages(payload.len() as u32);
            for g in 0..payload.len() {
                let mut gps = gps_list.reborrow().get(g as u32);
                gps.set_sequence(payload[g].sequence as i32);
                gps.set_timestamp(payload[g].timestamp);
                gps.set_latitude(payload[g].payload["latitude"].as_f64().unwrap());
                gps.set_longitude(payload[g].payload["longitude"].as_f64().unwrap());
            }

            write_message(&mut buf, message.borrow_inner())?;
        }
        "test.imuList" => {
            let mut message = TypedBuilder::<imu_list::Owned>::new_default();
            let mut imu_list = message.init_root().init_messages(payload.len() as u32);
            for i in 0..payload.len() {
                let mut imu = imu_list.reborrow().get(i as u32);
                imu.set_sequence(payload[i].sequence);
                imu.set_timestamp(payload[i].timestamp);
                imu.set_ax(payload[i].payload["ax"].as_f64().unwrap());
                imu.set_ay(payload[i].payload["ay"].as_f64().unwrap());
                imu.set_az(payload[i].payload["az"].as_f64().unwrap());
                imu.set_pitch(payload[i].payload["pitch"].as_f64().unwrap());
                imu.set_roll(payload[i].payload["roll"].as_f64().unwrap());
                imu.set_yaw(payload[i].payload["yaw"].as_f64().unwrap());
                imu.set_magx(payload[i].payload["magx"].as_f64().unwrap());
                imu.set_magy(payload[i].payload["magy"].as_f64().unwrap());
                imu.set_magz(payload[i].payload["magz"].as_f64().unwrap());
            }

            write_message(&mut buf, message.borrow_inner())?;
        }
        "test.motorList" => {
            let mut message = TypedBuilder::<motor_list::Owned>::new_default();
            let mut motor_list = message.init_root().init_messages(payload.len() as u32);
            for m in 0..payload.len() {
                let mut motor = motor_list.reborrow().get(m as u32);
                motor.set_sequence(payload[m].sequence);
                motor.set_timestamp(payload[m].timestamp);
                motor.set_temperature1(payload[m].payload["temperature1"].as_f64().unwrap());
                motor.set_temperature2(payload[m].payload["temperature2"].as_f64().unwrap());
                motor.set_temperature3(payload[m].payload["temperature3"].as_f64().unwrap());
                motor.set_voltage(payload[m].payload["voltage"].as_f64().unwrap());
                motor.set_current(payload[m].payload["current"].as_f64().unwrap());
                motor.set_rpm(payload[m].payload["rpm"].as_u64().unwrap() as u32);
            }

            write_message(&mut buf, message.borrow_inner())?;
        }
        "test.bmsList" => {
            let mut message = TypedBuilder::<bms_list::Owned>::new_default();
            let mut bms_list = message.init_root().init_messages(payload.len() as u32);
            for b in 0..payload.len() {
                let mut bms = bms_list.reborrow().get(b as u32);
                bms.set_sequence(payload[b].sequence as i32);
                bms.set_timestamp(payload[b].timestamp);
                bms.set_periodicity_ms(
                    payload[b].payload["periodicity_ms"].as_i64().unwrap() as i32
                );
                bms.set_mosfet_temperature(
                    payload[b].payload["mosfet_temperature"].as_f64().unwrap(),
                );
                bms.set_ambient_temperature(
                    payload[b].payload["ambient_temperature"].as_f64().unwrap(),
                );
                bms.set_mosfet_status(payload[b].payload["mosfet_status"].as_i64().unwrap() as i32);
                bms.set_cell_voltage_count(
                    payload[b].payload["cell_voltage_count"].as_i64().unwrap() as i32,
                );
                bms.set_cell_voltage1(payload[b].payload["cell_voltage_1"].as_f64().unwrap());
                bms.set_cell_voltage2(payload[b].payload["cell_voltage_2"].as_f64().unwrap());
                bms.set_cell_voltage2(payload[b].payload["cell_voltage_2"].as_f64().unwrap());
                bms.set_cell_voltage3(payload[b].payload["cell_voltage_3"].as_f64().unwrap());
                bms.set_cell_voltage4(payload[b].payload["cell_voltage_4"].as_f64().unwrap());
                bms.set_cell_voltage5(payload[b].payload["cell_voltage_5"].as_f64().unwrap());
                bms.set_cell_voltage6(payload[b].payload["cell_voltage_6"].as_f64().unwrap());
                bms.set_cell_voltage7(payload[b].payload["cell_voltage_7"].as_f64().unwrap());
                bms.set_cell_voltage8(payload[b].payload["cell_voltage_8"].as_f64().unwrap());
                bms.set_cell_voltage9(payload[b].payload["cell_voltage_9"].as_f64().unwrap());
                bms.set_cell_voltage10(payload[b].payload["cell_voltage_10"].as_f64().unwrap());
                bms.set_cell_voltage11(payload[b].payload["cell_voltage_11"].as_f64().unwrap());
                bms.set_cell_voltage12(payload[b].payload["cell_voltage_12"].as_f64().unwrap());
                bms.set_cell_voltage13(payload[b].payload["cell_voltage_13"].as_f64().unwrap());
                bms.set_cell_voltage14(payload[b].payload["cell_voltage_14"].as_f64().unwrap());
                bms.set_cell_voltage15(payload[b].payload["cell_voltage_15"].as_f64().unwrap());
                bms.set_cell_voltage16(payload[b].payload["cell_voltage_16"].as_f64().unwrap());
                bms.set_cell_thermistor_count(
                    payload[b].payload["cell_thermistor_count"]
                        .as_i64()
                        .unwrap() as i32,
                );
                bms.set_cell_temp1(payload[b].payload["cell_temp_1"].as_f64().unwrap());
                bms.set_cell_temp2(payload[b].payload["cell_temp_2"].as_f64().unwrap());
                bms.set_cell_temp3(payload[b].payload["cell_temp_3"].as_f64().unwrap());
                bms.set_cell_temp4(payload[b].payload["cell_temp_4"].as_f64().unwrap());
                bms.set_cell_temp5(payload[b].payload["cell_temp_5"].as_f64().unwrap());
                bms.set_cell_temp6(payload[b].payload["cell_temp_6"].as_f64().unwrap());
                bms.set_cell_temp7(payload[b].payload["cell_temp_7"].as_f64().unwrap());
                bms.set_cell_temp8(payload[b].payload["cell_temp_8"].as_f64().unwrap());
                bms.set_cell_balancing_status(
                    payload[b].payload["cell_balancing_status"]
                        .as_i64()
                        .unwrap() as i32,
                );
                bms.set_pack_voltage(payload[b].payload["pack_voltage"].as_f64().unwrap());
                bms.set_pack_current(payload[b].payload["pack_current"].as_f64().unwrap());
                bms.set_pack_soc(payload[b].payload["pack_soc"].as_f64().unwrap());
                bms.set_pack_soh(payload[b].payload["pack_soh"].as_f64().unwrap());
                bms.set_pack_sop(payload[b].payload["pack_sop"].as_f64().unwrap());
                bms.set_pack_cycle_count(payload[b].payload["pack_cycle_count"].as_i64().unwrap());
                bms.set_pack_available_energy(
                    payload[b].payload["pack_available_energy"]
                        .as_i64()
                        .unwrap(),
                );
                bms.set_pack_consumed_energy(
                    payload[b].payload["pack_consumed_energy"].as_i64().unwrap(),
                );
                bms.set_pack_fault(payload[b].payload["pack_fault"].as_i64().unwrap() as i32);
                bms.set_pack_status(payload[b].payload["pack_status"].as_i64().unwrap() as i32);
            }

            write_message(&mut buf, message.borrow_inner())?;
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
            let message = read_message(payload, ReaderOptions::new())?;
            let gps_list = message.get_root::<gps_list::Reader>()?.get_messages()?;
            let mut payload = vec![];
            for gps in gps_list {
                payload.push(Payload {
                    sequence: gps.get_sequence() as u32,
                    timestamp: gps.get_timestamp(),
                    payload: json!({"latitude": gps.get_latitude(), "longitude":gps.get_longitude(),}),
                    ..Default::default()
                });
            }

            Ok(payload)
        }
        "test.imuList" => {
            let message = read_message(payload, ReaderOptions::new())?;
            let imu_list = message.get_root::<imu_list::Reader>()?.get_messages()?;
            let mut payload = vec![];
            for imu in imu_list {
                payload.push(Payload {
                    sequence: imu.get_sequence(),
                    timestamp: imu.get_timestamp(),
                    payload: json!({"ax": imu.get_ax(), "ay": imu.get_ay(), "az": imu.get_az(), "pitch": imu.get_pitch(), "roll": imu.get_roll(), "yaw": imu.get_yaw(), "magx": imu.get_magx(), "magy": imu.get_magy(), "magz": imu.get_magz() }),
                    ..Default::default()
                });
            }

            Ok(payload)
        }
        "test.motorList" => {
            let message = read_message(payload, ReaderOptions::new())?;
            let motor_list = message.get_root::<motor_list::Reader>()?.get_messages()?;
            let mut payload = vec![];
            for motor in motor_list {
                payload.push(Payload {
                    sequence: motor.get_sequence(),
                    timestamp: motor.get_timestamp(),
                    payload: json!({ "temperature1": motor.get_temperature1(), "temperature2":motor.get_temperature2(), "temperature3": motor.get_temperature3(), "voltage": motor.get_voltage(), "current": motor.get_current(), "rpm": motor.get_rpm() }),
                    ..Default::default()
                });
            }

            Ok(payload)
        }
        "test.bmsList" => {
            let message = read_message(payload, ReaderOptions::new())?;
            let bms_list = message.get_root::<bms_list::Reader>()?.get_messages()?;
            let mut payload = vec![];
            for bms in bms_list {
                payload.push(Payload {
                    sequence: bms.get_sequence() as u32,
                    timestamp: bms.get_timestamp(),
                    payload: json!({ "periodicity_ms": bms.get_periodicity_ms(), "mosfet_temperature": bms.get_mosfet_temperature(), "ambient_temperature": bms.get_ambient_temperature(), "mosfet_status": bms.get_mosfet_status(), "cell_voltage_count": bms.get_cell_voltage_count(), "cell_voltage_1": bms.get_cell_voltage1(), "cell_voltage_2": bms.get_cell_voltage2(), "cell_voltage_3":bms.get_cell_voltage3(), "cell_voltage_4": bms.get_cell_voltage4(), "cell_voltage_5": bms.get_cell_voltage5(), "cell_voltage_6": bms.get_cell_voltage6(), "cell_voltage_7": bms.get_cell_voltage7(), "cell_voltage_8": bms.get_cell_voltage8(), "cell_voltage_9": bms.get_cell_voltage9(), "cell_voltage_10": bms.get_cell_voltage10(), "cell_voltage_11": bms.get_cell_voltage11(), "cell_voltage_12": bms.get_cell_voltage12(), "cell_voltage_13": bms.get_cell_voltage13(), "cell_voltage_14": bms.get_cell_voltage14(), "cell_voltage_15": bms.get_cell_voltage15(), "cell_voltage_16": bms.get_cell_voltage16(), "cell_thermistor_count": bms.get_cell_thermistor_count(), "cell_temp_1": bms.get_cell_temp1(), "cell_temp_2": bms.get_cell_temp2(), "cell_temp_3": bms.get_cell_temp3(), "cell_temp_4": bms.get_cell_temp4(), "cell_temp_5": bms.get_cell_temp5(), "cell_temp_6": bms.get_cell_temp6(), "cell_temp_7": bms.get_cell_temp7(), "cell_temp_8": bms.get_cell_temp8(), "cell_balancing_status": bms.get_cell_balancing_status(), "pack_voltage": bms.get_pack_voltage(), "pack_current": bms.get_pack_current(), "pack_soc": bms.get_pack_soc(), "pack_soh": bms.get_pack_soh(), "pack_sop": bms.get_pack_sop(), "pack_cycle_count": bms.get_pack_cycle_count(), "pack_available_energy": bms.get_pack_available_energy(), "pack_consumed_energy": bms.get_pack_consumed_energy(), "pack_fault": bms.get_pack_fault(), "pack_status": bms.get_pack_status(),}),
                    ..Default::default()
                });
            }

            Ok(payload)
        }
        _ => {
            panic!("Couldn't deserialize for stream: {}!", stream)
        }
    }
}
