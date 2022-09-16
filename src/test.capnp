
@0x9eb32e19f86ee174;

struct GpsList {
  messages @0 :List(Gps);

  struct Gps {
    longitude @0 : Float64;
    latitude @1 : Float64;
    timestamp @2 : UInt64;
    sequence @3 : Int32;
  }
}

struct ImuList {
  messages @0 :List(Imu);

  struct Imu {
    timestamp @0 : UInt64;
    sequence @1 : UInt32;
    ax @2 : Float64;
    ay @3 : Float64;
    az @4 : Float64;
    pitch @5 : Float64;
    roll @6 : Float64;
    yaw @7 : Float64;
    magx @8 : Float64;
    magy @9 : Float64;
    magz @10 : Float64;
  }
}

struct MotorList {
  messages @0 :List(Motor);

  struct Motor {
    timestamp @0 : UInt64;
    sequence @1 : UInt32;
    temperature1 @2 : Float64;
    temperature2 @3 : Float64;
    temperature3 @4 : Float64;
    voltage @5 : Float64;
    current @6 : Float64;
    rpm @7 : UInt32;
  }
}

struct BmsList {
  messages @0 :List(Bms);

  struct Bms {
    sequence @0 : Int32;
    timestamp @1 : UInt64;
    periodicityMs @2 : Int32;
    mosfetTemperature @3 : Float64;
    ambientTemperature @4 : Float64;
    mosfetStatus @5 : Int32;
    cellVoltageCount @6 : Int32;
    cellVoltage1 @7 : Float64;
    cellVoltage2 @8 : Float64;
    cellVoltage3 @9 : Float64;
    cellVoltage4 @10 : Float64;
    cellVoltage5 @11 : Float64;
    cellVoltage6 @12 : Float64;
    cellVoltage7 @13 : Float64;
    cellVoltage8 @14 : Float64;
    cellVoltage9 @15 : Float64;
    cellVoltage10 @16 : Float64;
    cellVoltage11 @17 : Float64;
    cellVoltage12 @18 : Float64;
    cellVoltage13 @19 : Float64;
    cellVoltage14 @20 : Float64;
    cellVoltage15 @21 : Float64;
    cellVoltage16 @22 : Float64;
    cellThermistorCount @23 : Int32;
    cellTemp1 @24 : Float64;
    cellTemp2 @25 : Float64;
    cellTemp3 @26 : Float64;
    cellTemp4 @27 : Float64;
    cellTemp5 @28 : Float64;
    cellTemp6 @29 : Float64;
    cellTemp7 @30 : Float64;
    cellTemp8 @31 : Float64;
    cellBalancingStatus @32 : Int32;
    packVoltage @33 : Float64;
    packCurrent @34 : Float64;
    packSoc @35 : Float64;
    packSoh @36 : Float64;
    packSop @37 : Float64;
    packCycleCount @38 : Int64;
    packAvailableEnergy @39 : Int64;
    packConsumedEnergy @40 : Int64;
    packFault @41 : Int32;
    packStatus @42 : Int32;
  }
}
