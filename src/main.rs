use shoju::log::partition::Partition;

fn main() -> std::io::Result<()> {
    let mut partition = Partition::init()?;
    let record = partition.find_record(14).expect("Failed look up");
    println!("Record: {}", record);
    // let mut segment =
    //     Segment::new("0000000000000000".into(), true, 0).expect("Segment creation failed");
    // for i in 0..55 {
    //     let r = Record::new(i, vec![0, 0, 1]);
    //     segment.append_record(r).expect("Error writing to disk");
    // }
    //
    // let record = segment.read_at(17).unwrap();
    // println!("Record: {}", record);
    // let record = segment.read_at(20).unwrap();
    // println!("Record: {}", record);
    // let record = segment.read_at(11).unwrap();
    // println!("Record: {}", record);
    Ok(())
}
