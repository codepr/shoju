mod record;
mod segment;

fn main() {
    let mut segment =
        segment::Segment::new("0000000000000000".into()).expect("Segment creation failed");
    for i in 0..55 {
        let r = record::Record::new(i, vec![0, 0, 1]);
        segment.append_record(r).expect("Error writing to disk");
    }

    let record = segment.read_at(17).unwrap();
    println!("Record: {}", record.offset);
}
