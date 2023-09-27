use shoju::log::partition::Partition;

fn main() -> std::io::Result<()> {
    let mut partition = Partition::init()?;
    // for _i in 0..55 {
    //     partition
    //         .append_record(&[0, 0, 1, 0])
    //         .expect("Error writing to disk");
    // }
    let record = partition.find_record(0).expect("Failed look up");
    println!("Record: {}", record);
    let record = partition.find_record(14).expect("Failed look up");
    println!("Record: {}", record);
    let record = partition.find_record(163).expect("Failed look up");
    println!("Record: {}", record);

    Ok(())
}
