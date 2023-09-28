use shoju::log::partition::Partition;

fn main() -> std::io::Result<()> {
    let mut partition = Partition::init()?;
    // for _i in 0..1055 {
    //     partition
    //         .append_record(&[0, 0, 1, 0])
    //         .expect("Error writing to disk");
    // }
    let record = partition.find_record(0).expect("Failed look up");
    println!("Record: {}", record);
    let record = partition.find_record(14).expect("Failed look up 14");
    println!("Record: {}", record);
    let record = partition.find_record(163).expect("Failed look up 163");
    println!("Record: {}", record);
    let record = partition.find_record(563).expect("Failed look up 563");
    println!("Record: {}", record);
    let record = partition.find_record(957).expect("Failed look up 957");
    println!("Record: {}", record);
    let record = partition.find_record(980).expect("Failed look up 980");
    println!("Record: {}", record);
    let record = partition.find_record(1010).expect("Failed look up 1010");
    println!("Record: {}", record);
    let record = partition.find_record(1400).expect("Failed look up 1400");
    println!("Record: {}", record);

    Ok(())
}
